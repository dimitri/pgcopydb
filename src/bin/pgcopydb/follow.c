/*
 * src/bin/pgcopydb/follow.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include "catalog.h"
#include "cli_common.h"
#include "cli_root.h"
#include "ld_stream.h"
#include "log.h"
#include "progress.h"
#include "signals.h"


/*
 * follow_assert_pipe_fd validates that a pipe file descriptor is positive and
 * ready for use.  A non-positive fd means the pipe was never created, or was
 * already closed and set to -1 by stream_signal_upstream_done — either way
 * the follow pipeline cannot make progress.  We exit immediately with a FATAL
 * so the operator gets a clear diagnostic rather than a confusing EBADF later.
 */
static void
follow_assert_pipe_fd(int fd, const char *label)
{
	if (fd <= 0)
	{
		log_fatal("BUG: pipe fd %s = %d is invalid; "
				  "--follow pipeline is broken and cannot proceed",
				  label, fd);
		exit(EXIT_CODE_INTERNAL_ERROR);
	}
}


/*
 * follow_close_pipe_fd closes a pipe fd when it is still open.
 *
 * stream_signal_upstream_done sets specs->pipe_XX[n] = -1 after it has closed
 * the underlying file descriptor.  Callers that close the fd as a safety net
 * after their main loop use this helper so that the already-handled case is
 * silently skipped rather than calling close(-1) and dying with EBADF.
 */
static void
follow_close_pipe_fd(int fd, const char *label)
{
	if (fd <= 0)
	{
		log_debug("follow_close_pipe_fd: %s already closed (fd=%d), skipping",
				  label, fd);
		return;
	}

	if (close(fd) != 0)
	{
		log_fatal("Failed to close pipe fd %s (%d): %m", label, fd);
		exit(EXIT_CODE_INTERNAL_ERROR);
	}
}


/*
 * follow_export_snapshot opens a snapshot that we're going to re-use in all
 * our connections to the source database. When the --snapshot option has been
 * used, instead of exporting a new snapshot, we can just re-use it.
 */
bool
follow_export_snapshot(CopyDataSpec *copySpecs, StreamSpecs *streamSpecs)
{
	/*
	 * When using logical decoding, we need to create our replication slot and
	 * fetch the snapshot from that logical replication command.
	 */
	char *logrep_pguri = streamSpecs->connStrings->logrep_pguri;

	if (!copydb_create_logical_replication_slot(copySpecs,
												logrep_pguri,
												&(streamSpecs->slot),
												streamSpecs->paths.dir))
	{
		/* errors have already been logged */
		return false;
	}

	if (!catalog_write_replication_slot(streamSpecs->sourceDB,
										&(streamSpecs->slot)))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Also update the setup table's snapshot/plugin/slot_name so that
	 * catalog_register_setup_from_specs() can validate consistency when
	 * later commands (e.g. pgcopydb clone) open the same catalog.
	 */
	if (!catalog_setup_replication(streamSpecs->sourceDB,
								   streamSpecs->slot.snapshot))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * follow_setup_databases ensures that both source and target databases are
 * setup for logical decoding operations (replication slot, replication origin
 * tracking, pgcopydb.sentinel table).
 */
bool
follow_setup_databases(CopyDataSpec *copySpecs, StreamSpecs *streamSpecs)
{
	/*
	 * We want to make sure to use a private PGSQL client connection
	 * instance when connecting to the source database now, as the main
	 * connection is currently active holding a snapshot for the whole
	 * process.
	 */
	CopyDataSpec setupSpecs = { 0 };
	TransactionSnapshot snapshot = { 0 };

	/* copy our structure wholesale */
	setupSpecs = *copySpecs;

	/* ensure we use a new snapshot and connection in setupSpecs */
	if (!copydb_copy_snapshot(copySpecs, &snapshot))
	{
		/* errors have already been logged */
		return false;
	}

	setupSpecs.sourceSnapshot = snapshot;

	/*
	 * Now create the replication slot and the pgcopydb sentinel table on
	 * the source database, and the origin (replication progress tracking)
	 * on the target database.
	 */
	if (!stream_setup_databases(&setupSpecs, streamSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * follow_reset_sequences resets the sequences on the target database to match
 * the source database at this very moment (not in any pre-established
 * snapshot). Postgres logical decoding lacks support for syncing sequences.
 *
 * This step is implement as if running the following command:
 *
 *   $ pgcopydb copy sequences --resume --not-consistent
 *
 * The whole idea is to fetch the "new" current values of the sequences, not
 * the ones that were current when the main snapshot was exported.
 */
bool
follow_reset_sequences(CopyDataSpec *copySpecs, StreamSpecs *streamSpecs)
{
	CopyDataSpec seqSpecs = { 0 };

	/* copy our structure wholesale */
	seqSpecs = *copySpecs;

	/* then force some options such as --resume --not-consistent */
	seqSpecs.restart = false;
	seqSpecs.resume = true;
	seqSpecs.consistent = false;
	seqSpecs.section = DATA_SECTION_SET_SEQUENCES;

	/* we don't want to re-use any snapshot */
	TransactionSnapshot snapshot = { 0 };

	seqSpecs.sourceSnapshot = snapshot;

	/* fetch schema information from source catalogs, including filtering */
	if (!copydb_fetch_schema_and_prepare_specs(&seqSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	bool reset = true;

	if (!copydb_copy_all_sequences(&seqSpecs, reset))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * follow_init_sentinel sets the sentinel endpos to the command line --endpos
 * option, when given.
 */
bool
follow_init_sentinel(StreamSpecs *specs, CopyDBSentinel *sentinel)
{
	DatabaseCatalog *sourceDB = specs->sourceDB;

	if (!catalog_open(sourceDB))
	{
		/* errors have already been logged */
		return false;
	}

	if (specs->endpos != InvalidXLogRecPtr)
	{
		if (!sentinel_update_endpos(sourceDB, specs->endpos))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if (!sentinel_get(sourceDB, sentinel))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * follow_get_sentinel refreshes the given CopyDBSentinel with the current
 * values from the pgcopydb.sentinel table.
 */
bool
follow_get_sentinel(StreamSpecs *specs, CopyDBSentinel *sentinel, bool verbose)
{
	DatabaseCatalog *sourceDB = specs->sourceDB;

	if (!sentinel_get(sourceDB, sentinel))
	{
		/* errors have already been logged */
		return false;
	}

	/* always accept the startpos and apply values from the sentinel */
	specs->startpos = sentinel->startpos;

	/* the endpos might have changed on the sentinel table */
	if (sentinel->endpos != InvalidXLogRecPtr &&
		sentinel->endpos != specs->endpos)
	{
		specs->endpos = sentinel->endpos;

		log_info("Current sentinel replay_lsn is %X/%X, "
				 "endpos has now been set to %X/%X",
				 LSN_FORMAT_ARGS(sentinel->replay_lsn),
				 LSN_FORMAT_ARGS(sentinel->endpos));
	}
	else if (verbose)
	{
		if (sentinel->endpos != InvalidXLogRecPtr)
		{
			log_info("Current sentinel replay_lsn is %X/%X, endpos is %X/%X",
					 LSN_FORMAT_ARGS(sentinel->replay_lsn),
					 LSN_FORMAT_ARGS(sentinel->endpos));
		}
		else if (sentinel->replay_lsn != InvalidXLogRecPtr)
		{
			log_info("Current sentinel replay_lsn is %X/%X",
					 LSN_FORMAT_ARGS(sentinel->replay_lsn));
		}
	}

	return true;
}


/*
 * follow_main_loop implements the main loop for the follow sub-process
 * management. It loops between two modes of operations:
 *
 *  1. prefetch + catchup
 *  2. live replay using Unix pipes between sub-processes
 *
 * When the catchup process needs to read a file on-disk that does not exist
 * yet, it quits with EXIT_CODE_QUIT (success) and the loop terminate the other
 * subprocesses and switch to the live replay mode of operations.
 *
 * When a sub-process ends abnormally then the main process terminates the
 * sibling worker processes and restart in the other mode.
 *
 * Each time we switch from a mode of operations to another, a catchup from
 * disk is done to ensure we don't miss applying what has already been
 * received.
 */
bool
follow_main_loop(CopyDataSpec *copySpecs, StreamSpecs *streamSpecs)
{
	DatabaseCatalog *sourceDB = &(copySpecs->catalogs.source);

	if (!catalog_open(sourceDB))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * The SQLite-based CDC pipeline uses a single CATCHUP mode for all
	 * operations: prefetch writes WAL records to the output table, transform
	 * converts them to parameterised SQL in the replay table, and catchup
	 * applies them to the target.  The old CATCHUP↔REPLAY alternation (which
	 * switched to Unix-pipe live streaming once the on-disk files were caught
	 * up) is no longer needed — SQLite provides the inter-process
	 * communication and the three workers can restart in the same mode every
	 * time.
	 */
	while (true)
	{
		if (!followDB(copySpecs, streamSpecs))
		{
			log_error("Failed to follow changes from source, "
					  "see above for details");
			return false;
		}

		if (asked_to_quit)
		{
			log_error("Main follow process received SIGQUIT, exiting");
			return false;
		}

		bool done = false;

		if (!follow_reached_endpos(streamSpecs, &done))
		{
			/* errors have already been logged */
			return false;
		}

		if (done)
		{
			log_info("Follow mode is now done, "
					 "reached endpos %X/%X with replay_lsn %X/%X",
					 LSN_FORMAT_ARGS(streamSpecs->sentinel.endpos),
					 LSN_FORMAT_ARGS(streamSpecs->sentinel.replay_lsn));

			return true;
		}

		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			log_warn("Main follow process was asked to terminate, exiting");
			return true;
		}

		/*
		 * With endpos set, subprocesses have exited but follow_reached_endpos
		 * returned done=false — meaning neither the replay_lsn check nor the
		 * pipeline_state 'done' check fired.  This is a genuine pipeline
		 * failure: endpos is set but the data was not fully applied.
		 *
		 * The replay_lsn >= endpos check here is a fallback safety net (the
		 * primary check is inside follow_reached_endpos); if it somehow fires
		 * here, accept it as success rather than returning a spurious error.
		 */
		if (streamSpecs->sentinel.endpos != InvalidXLogRecPtr)
		{
			if (streamSpecs->sentinel.endpos <= streamSpecs->sentinel.replay_lsn)
			{
				log_info("Subprocesses exited with endpos reached at %X/%X "
						 "(replay_lsn: %X/%X). Pipeline complete.",
						 LSN_FORMAT_ARGS(streamSpecs->sentinel.endpos),
						 LSN_FORMAT_ARGS(streamSpecs->sentinel.replay_lsn));
				return true;
			}
			else
			{
				log_error("Subprocesses exited with endpos set at %X/%X "
						  "but replay_lsn only reached %X/%X and pipeline_state "
						  "did not confirm clean completion. "
						  "Data not fully applied.",
						  LSN_FORMAT_ARGS(streamSpecs->sentinel.endpos),
						  LSN_FORMAT_ARGS(streamSpecs->sentinel.replay_lsn));
				return false;
			}
		}

		log_info("Restarting logical decoding follower in %s mode "
				 "(endpos unset, waiting for more data)",
				 LogicalStreamModeToString(streamSpecs->mode));
	}

	/* keep compiler happy */
	log_warn("BUG: follow_main_loop reached out of loop");
	return true;
}


/*
 * follow_reached_endpos sets done to true when endpos has been reached.
 *
 * Two checks are performed:
 *
 * Check (a) — primary: sentinel.replay_lsn >= sentinel.endpos.
 *   Fires when the last applied commit naturally covered endpos (normal case,
 *   Guard 3 mid-txn where the full transaction was committed and its
 *   commitLSN >= endpos).
 *
 * Check (b) — secondary: both transform and apply exited with run_state='done'.
 *   Fires when endpos fell between two transactions (Guard 2: endpos < beginLSN)
 *   or inside an uncommitted transaction (mid-txn endpos).  In those cases
 *   apply exits cleanly without advancing replay_lsn past endpos, so check (a)
 *   does not fire.  Both processes mark run_state='done' only on a successful,
 *   intentional exit; signal-driven exits leave transform as 'error', preventing
 *   false positives.
 */
bool
follow_reached_endpos(StreamSpecs *streamSpecs, bool *done)
{
	bool verbose = true;
	CopyDBSentinel *sentinel = &(streamSpecs->sentinel);

	if (!follow_get_sentinel(streamSpecs, sentinel, verbose))
	{
		log_error("Failed to get sentinel values");
		return false;
	}

	/* Check (a): normal commit-boundary endpos */
	if (sentinel->endpos != InvalidXLogRecPtr &&
		sentinel->endpos <= sentinel->replay_lsn)
	{
		/* follow_get_sentinel logs replay_lsn and endpos already */
		*done = true;

		log_info("Current endpos %X/%X has been reached at replay_lsn %X/%X",
				 LSN_FORMAT_ARGS(sentinel->endpos),
				 LSN_FORMAT_ARGS(sentinel->replay_lsn));

		return true;
	}

	/*
	 * Check (b): endpos fell between or inside transactions.
	 *
	 * replay_lsn is still at the last real commit (< endpos).  The pipeline
	 * signals completion by having both transform and apply exit with
	 * run_state='done'.
	 */
	if (sentinel->endpos != InvalidXLogRecPtr)
	{
		PipelineStateEntry ts = { 0 };
		PipelineStateEntry as = { 0 };
		DatabaseCatalog *sourceDB = streamSpecs->sourceDB;

		bool transform_done =
			pipeline_state_get(sourceDB, "transform", &ts) &&
			ts.process_name[0] != '\0' &&
			strcmp(ts.run_state, "done") == 0;

		bool apply_done =
			pipeline_state_get(sourceDB, "apply", &as) &&
			as.process_name[0] != '\0' &&
			strcmp(as.run_state, "done") == 0;

		if (transform_done && apply_done)
		{
			*done = true;

			log_info("Current endpos %X/%X reached: transform done at %X/%X, "
					 "apply done at %X/%X (replay_lsn %X/%X; "
					 "endpos was at a transaction boundary)",
					 LSN_FORMAT_ARGS(sentinel->endpos),
					 LSN_FORMAT_ARGS(ts.run_end_lsn),
					 LSN_FORMAT_ARGS(as.run_end_lsn),
					 LSN_FORMAT_ARGS(sentinel->replay_lsn));
		}
	}

	return true;
}


/*
 * follow_prepare_mode_switch prepares for the next mode of operation. We need
 * to make sure that all that was streamed in our JSON file has been
 * transformed and replayed from file before changing our mode of operations.
 */
bool
follow_prepare_mode_switch(StreamSpecs *streamSpecs,
						   LogicalStreamMode previousMode,
						   LogicalStreamMode currentMode)
{
	log_info("Catching-up from existing on-disk files");

	/*
	 * Catch-up with what has been streamed and transformed into the SQLite
	 * replayDB, applying those changes to the target database.
	 */
	LogicalStreamMode mode = streamSpecs->mode;
	streamSpecs->mode = STREAM_MODE_CATCHUP;

	FollowSubProcess *catchup = &(streamSpecs->catchup);

	if (!follow_start_subprocess(streamSpecs, catchup))
	{
		streamSpecs->mode = mode;
		log_error("Failed to start the %s process", catchup->name);
		return false;
	}

	if (!follow_wait_subprocesses(streamSpecs))
	{
		streamSpecs->mode = mode;
		log_error("Failed to catchup with on-disk files, "
				  "see above for details");
		return false;
	}

	/* re-install the streamSpecs->mode as it was before getting there */
	streamSpecs->mode = mode;

	return true;
}


/*
 * followDB implements a logical decoding client for streaming changes from the
 * source database into the target database.
 *
 * The source database is expected to have been setup already so that the
 * replication slot using wal2json is ready, the pgcopydb.sentinel table
 * exists, and the target database replication origin has been created too.
 */
bool
followDB(CopyDataSpec *copySpecs, StreamSpecs *streamSpecs)
{
	int errors = 0;

	if (streamSpecs->mode < STREAM_MODE_PREFETCH)
	{
		log_error("BUG: followDB with stream mode %d", streamSpecs->mode);
		return false;
	}

	/*
	 * Before starting sub-processes, make sure to close our SQLite catalogs.
	 * We open the SQLite catalogs again before returning from this function
	 * (if only when reaching the end of it and returning true).
	 */
	if (!catalog_close(streamSpecs->sourceDB))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Create the lifecycle signal pipes.
	 *
	 * pipe_rt carries an 8-byte big-endian done-LSN from receive → transform.
	 * pipe_ta carries an 8-byte big-endian done-LSN from transform → apply.
	 *
	 * These are always created when both endpoints will run together so that
	 * the downstream process can select() on the read end and wake up
	 * immediately when upstream finishes, without polling SQLite.
	 *
	 * In text-streaming mode (stdOut / stdIn), the same pipe fds are also
	 * used for the JSON data stream; in replayDB mode they carry only the
	 * single lifecycle signal message.
	 */
	if (pipe(streamSpecs->pipe_rt) != 0)
	{
		log_fatal("Failed to create receive→transform pipe: %m");
		return false;
	}

	if (pipe(streamSpecs->pipe_ta) != 0)
	{
		log_fatal("Failed to create transform→apply pipe: %m");
		close(streamSpecs->pipe_rt[0]);
		close(streamSpecs->pipe_rt[1]);
		return false;
	}

	FollowSubProcess *prefetch = &(streamSpecs->prefetch);
	FollowSubProcess *transform = &(streamSpecs->transform);
	FollowSubProcess *catchup = &(streamSpecs->catchup);

	/*
	 * When set to prefetch changes, we always also run the transform process
	 * to prepare the SQL files from the JSON files. Also upper modes (catchup,
	 * replay) does imply prefetching (and transform).
	 */
	if (streamSpecs->mode >= STREAM_MODE_PREFETCH)
	{
		if (!follow_start_subprocess(streamSpecs, prefetch))
		{
			log_error("Failed to start the %s process", prefetch->name);
			return false;
		}

		if (!follow_start_subprocess(streamSpecs, transform))
		{
			log_error("Failed to start the transform process");

			(void) follow_exit_early(streamSpecs);
			return false;
		}
	}

	/*
	 * When set to catchup or replay mode, we also start the catchup process.
	 */
	if (streamSpecs->mode >= STREAM_MODE_CATCHUP)
	{
		if (!follow_start_subprocess(streamSpecs, catchup))
		{
			log_error("Failed to start the %s process", catchup->name);

			(void) follow_exit_early(streamSpecs);
			return false;
		}
	}

	/*
	 * Close pipe ends which follow is not using. Otherwise the processes
	 * like transform and apply which reads from the pipe during replay
	 * will never see EOF.
	 */
	if (streamSpecs->stdOut)
	{
		close_fd_or_exit(streamSpecs->pipe_rt[1]);
		close_fd_or_exit(streamSpecs->pipe_rt[0]);
	}

	if (streamSpecs->stdIn)
	{
		close_fd_or_exit(streamSpecs->pipe_ta[0]);
		close_fd_or_exit(streamSpecs->pipe_ta[1]);
	}

	/*
	 * Finally wait until the process are finished.
	 *
	 * This happens when the sentinel endpos is set, typically using the
	 * command: pgcopydb stream sentinel set endpos --current.
	 *
	 * When waipid() catches a subprocess termination, we need to update our
	 * sentinel values, and for that we need to catalogs open again. The caller
	 * to this function had the catalogs open, so we let them opened when
	 * returning here.
	 */
	if (!catalog_open(streamSpecs->sourceDB))
	{
		/* errors have already been logged */
		return false;
	}

	if (follow_wait_subprocesses(streamSpecs))
	{
		log_info("Subprocesses for %s, %s, and %s have now all exited",
				 prefetch->name,
				 transform->name,
				 catchup->name);
	}
	else
	{
		++errors;
		log_error("Some sub-process exited with errors, "
				  "see above for details");
	}

	return errors == 0;
}


/*
 * follow_start_prefetch starts a sub-process that prefetches changes from the
 * source database into local files.
 */
bool
follow_start_prefetch(StreamSpecs *specs)
{
	if (specs->mode == STREAM_MODE_REPLAY)
	{
		/* arrange to write to the receive-transform pipe */
		specs->stdIn = false;
		specs->stdOut = true;

		/* validate before use: a bad fd here means follow_start_subprocesses
		 * failed to create the pipe, which is a programming error */
		follow_assert_pipe_fd(specs->pipe_rt[1], "pipe_rt[1]");

		specs->out = fdopen(specs->pipe_rt[1], "a");

		/* close pipe ends we're not using */
		close_fd_or_exit(specs->pipe_rt[0]);
		close_fd_or_exit(specs->pipe_ta[0]);
		close_fd_or_exit(specs->pipe_ta[1]);

		/* switch out stream from block buffered to line buffered mode */
		if (setvbuf(specs->out, NULL, _IOLBF, 0) != 0)
		{
			log_error("Failed to set out stream to line buffered mode: %m");
			return false;
		}

		bool success = startLogicalStreaming(specs);

		/*
		 * Safety net: close pipe_rt[1] if it was not already closed by
		 * stream_signal_upstream_done inside streamCloseFile.  That function
		 * closes the raw fd and sets specs->pipe_rt[1] = -1 to prevent
		 * double-close.  follow_close_pipe_fd skips the close when fd <= 0.
		 */
		follow_close_pipe_fd(specs->pipe_rt[1], "pipe_rt[1]");

		log_info("Prefetch process has terminated");

		return success;
	}
	else
	{
		specs->stdIn = false;
		specs->stdOut = false;

		bool success = startLogicalStreaming(specs);

		log_info("Prefetch process has terminated");

		return success;
	}

	return true;
}


/*
 * follow_start_transform runs in the transform sub-process.
 *
 * In PREFETCH/CATCHUP mode: reads from the SQLite output table and writes
 * transformed statements to the replay+stmt tables.
 *
 * In REPLAY mode: reads JSON lines from the receive-transform Unix pipe and
 * writes SQL to the transform-apply Unix pipe (streaming, no SQLite).
 */
bool
follow_start_transform(StreamSpecs *specs)
{
	if (specs->mode == STREAM_MODE_REPLAY)
	{
		/*
		 * In replay mode the transform process reads JSON lines from the
		 * receive → transform pipe and writes SQL to the transform → apply
		 * pipe.  Set up the pipe file descriptors before calling into the
		 * streaming transform.
		 */
		specs->stdIn = true;
		specs->stdOut = true;

		follow_assert_pipe_fd(specs->pipe_rt[0], "pipe_rt[0]");
		follow_assert_pipe_fd(specs->pipe_ta[1], "pipe_ta[1]");

		specs->in = fdopen(specs->pipe_rt[0], "r");
		specs->out = fdopen(specs->pipe_ta[1], "a");

		/* close the ends of each pipe that this process does not use */
		close_fd_or_exit(specs->pipe_rt[1]);
		close_fd_or_exit(specs->pipe_ta[0]);

		/* line-buffer the output so the apply process sees complete lines */
		if (setvbuf(specs->out, NULL, _IOLBF, 0) != 0)
		{
			log_error("Failed to set out stream to line buffered mode: %m");
			return false;
		}

		bool success = stream_transform_stream(specs);

		log_info("Transform process has terminated");

		/*
		 * pipe_rt[0]: the receive side of the receive→transform pipe.  We
		 * only read from it; receive closes the write end when done.  This
		 * fd is never set to -1 by our code, so a plain close is correct.
		 *
		 * pipe_ta[1]: the write end of the transform→apply pipe.
		 * stream_signal_upstream_done (called from the midTxnEndpos or normal
		 * exit paths inside stream_transform_stream) may have already closed
		 * this fd and set specs->pipe_ta[1] = -1.  Use follow_close_pipe_fd
		 * to skip the close in that case.
		 */
		close_fd_or_exit(specs->pipe_rt[0]);
		follow_close_pipe_fd(specs->pipe_ta[1], "pipe_ta[1]");

		return success;
	}

	/*
	 * In PREFETCH and CATCHUP modes the transform reads from the SQLite
	 * output table and populates the replay+stmt tables.
	 */
	if (!stream_transform_messages(specs))
	{
		log_error("Transform process failed, see above for details");
		return false;
	}

	return true;
}


/*
 * follow_start_catchup starts a sub-process that catches-up using the SQL
 * files that have been prepared by the prefetch process.
 */
bool
follow_start_catchup(StreamSpecs *specs)
{
	/*
	 * In replay mode, the SQL command are read from stdin.
	 */
	if (specs->mode == STREAM_MODE_REPLAY)
	{
		/* arrange to read from the transform-apply pipe */
		specs->stdIn = true;
		specs->stdOut = false;

		follow_assert_pipe_fd(specs->pipe_ta[0], "pipe_ta[0]");

		specs->in = fdopen(specs->pipe_ta[0], "r");

		/* close pipe ends we're not using */
		close_fd_or_exit(specs->pipe_rt[0]);
		close_fd_or_exit(specs->pipe_rt[1]);
		close_fd_or_exit(specs->pipe_ta[1]);

		bool success = stream_apply_replay(specs);

		log_info("Apply process has terminated");

		/*
		 * pipe_ta[0] is the read end of the transform→apply pipe.  The apply
		 * process only reads from it; transform holds and closes the write end.
		 * This fd is never set to -1 by our code paths, so a plain close is
		 * correct.  Use follow_close_pipe_fd as a defensive measure nonetheless.
		 */
		follow_close_pipe_fd(specs->pipe_ta[0], "pipe_ta[0]");

		return success;
	}
	else
	{
		/*
		 * In other modes of operations (CATCHUP, really, here), we
		 * start the file based catchup mechanism, which follows the
		 * current LSN on the target database origin tracking system to
		 * open the right SQL file and apply statements from there.
		 */
		specs->stdIn = false;
		specs->stdOut = false;

		bool success = stream_apply_catchup(specs);

		log_info("Apply process has terminated");

		return success;
	}

	return true;
}


/*
 * follow_start_subprocess forks a subprocess and calls the given function.
 */
bool
follow_start_subprocess(StreamSpecs *specs, FollowSubProcess *subprocess)
{
	/* make sure to re-init the structure dynamic fields */
	subprocess->pid = -1;
	subprocess->exited = false;

	/*
	 * Flush stdio channels just before fork, to avoid double-output
	 * problems.
	 */
	fflush(stdout);
	fflush(stderr);

	/* now we can fork a sub-process to transform the current file */
	pid_t fpid = fork();

	switch (fpid)
	{
		case -1:
		{
			log_error("Failed to fork %s subprocess: %m", subprocess->name);
			return false;
		}

		case 0:
		{
			/* child process runs the command */
			pid_t pid = getpid();
			char psTitle[BUFSIZE] = { 0 };

			sformat(psTitle, sizeof(psTitle), "pgcopydb: follow %s",
					subprocess->name);

			(void) set_ps_title(psTitle);

			/* also track the process information in our catalogs */
			ProcessInfo ps = {
				.pid = pid,
				.psTitle = ps_buffer
			};

			strlcpy(ps.psType, subprocess->name, sizeof(ps.psType));

			DatabaseCatalog *sourceDB = specs->sourceDB;

			if (!catalog_open(sourceDB))
			{
				/* errors have already been logged */
				return false;
			}

			if (!catalog_upsert_process_info(sourceDB, &ps))
			{
				log_error("Failed to track progress in our catalogs, "
						  "see above for details");
				return false;
			}

			log_notice("Starting the %s sub-process", subprocess->name);

			if (!(subprocess->command)(specs))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			exit(EXIT_CODE_QUIT);
		}

		default:
		{
			subprocess->pid = fpid;
			return true;
		}
	}

	return true;
}


/*
 * follow_exit_early exits early, typically used when a process fails to start
 * and other processes where started already.
 */
void
follow_exit_early(StreamSpecs *specs)
{
	log_debug("follow_exit_early");

	if (!follow_terminate_subprocesses(specs))
	{
		log_error("Failed to terminate other subprocesses, "
				  "see above for details");
	}

	if (!follow_wait_subprocesses(specs))
	{
		log_error("Some sub-process exited in error, "
				  "see above for details");
	}
}


/*
 * follow_wait_subprocesses waits until both sub-processes are finished.
 */
bool
follow_wait_subprocesses(StreamSpecs *specs)
{
	FollowSubProcess *processArray[] = {
		&(specs->prefetch),
		&(specs->transform),
		&(specs->catchup)
	};

	int count = sizeof(processArray) / sizeof(processArray[0]);

	bool success = true;
	int stillRunning = count;

	/* now the main loop, that waits until all given processes have exited */
	while (stillRunning > 0)
	{
		if (asked_to_quit)
		{
			if (!follow_terminate_subprocesses(specs))
			{
				log_error("Failed to terminate other subprocesses, "
						  "see above for details");
				return false;
			}
		}

		/* re-init stillRunning at each iteration */
		stillRunning = count;

		for (int i = 0; i < count; i++)
		{
			/* skip already exited sub-processes, and not started ones too */
			if (processArray[i]->pid <= 0 || processArray[i]->exited)
			{
				--stillRunning;
				continue;
			}

			/* follow_wait_pid is non-blocking: uses WNOHANG */
			if (!follow_wait_pid(processArray[i]->pid,
								 &(processArray[i]->exited),
								 &(processArray[i]->returnCode),
								 &(processArray[i]->sig)))
			{
				/* errors have already been logged */
				return false;
			}

			if (processArray[i]->exited)
			{
				--stillRunning;

				/*
				 * First, delete the process from our tracking in the catalogs.
				 */
				if (!catalog_delete_process(specs->sourceDB,
											processArray[i]->pid))
				{
					log_notice("Failed to delete process entry for pid %d",
							   processArray[i]->pid);
				}

				int logLevel = LOG_NOTICE;
				char details[BUFSIZE] = { 0 };

				/*
				 * A sub-process exit is considered a "successful" exit when
				 * the return code is zero and the signal for termination is a
				 * signal that pgcopydb knows to handle and expects.
				 */
				bool exitedSuccessfully =
					processArray[i]->returnCode == 0 &&
					signal_is_handled(processArray[i]->sig);

				if (exitedSuccessfully)
				{
					if (processArray[i]->sig == 0)
					{
						sformat(details, sizeof(details), "successfully");
					}
					else
					{
						sformat(details, sizeof(details),
								"successfully after signal %s",
								signal_to_string(processArray[i]->sig));
					}
				}
				else
				{
					logLevel = LOG_ERROR;

					if (processArray[i]->returnCode == 0)
					{
						sformat(details, sizeof(details),
								"with return code %d and signal %s",
								processArray[i]->returnCode,
								signal_to_string(processArray[i]->sig));
					}
					else if (processArray[i]->sig == 0)
					{
						sformat(details, sizeof(details),
								"with error code %d",
								processArray[i]->returnCode);
					}
					else
					{
						sformat(details, sizeof(details),
								"with error code %d and signal %s",
								processArray[i]->returnCode,
								signal_to_string(processArray[i]->sig));
					}
				}

				log_level(logLevel,
						  "Subprocess %s with pid %d has exited %s",
						  processArray[i]->name,
						  processArray[i]->pid,
						  details);

				/*
				 * When one sub-process has exited abnormally, we terminate all
				 * the other sub-processes to handle the problem at the caller.
				 *
				 * When a sub-process exits successfully but endpos is still
				 * unset, that's normal: the process has caught up with available
				 * data and is waiting for more (e.g., the inject service hasn't
				 * set endpos yet). We should NOT terminate the other processes.
				 *
				 * Only if the process exited with an error do we need to
				 * terminate everything.  In the old CATCHUP/REPLAY mode,
				 * processes would continue running; now they exit when caught up,
				 * which is normal behavior.
				 */
				if (!exitedSuccessfully)
				{
					if (!follow_get_sentinel(specs, &(specs->sentinel), false))
					{
						/* continue without updated endpos */
						log_warn("Failed to get sentinel values");
					}

					log_error("Process %s has exited with error code %d, "
							  "terminating other processes",
							  processArray[i]->name,
							  processArray[i]->returnCode);

					if (!follow_terminate_subprocesses(specs))
					{
						log_error("Failed to terminate other subprocesses, "
								  "see above for details");
						return false;
					}
				}

				success = success && exitedSuccessfully;
			}
		}

		/* avoid busy looping, wait for 150ms before checking again */
		pg_usleep(150 * 1000);
	}

	return success;
}


/*
 * follow_terminate_subprocesses is used in case of errors in one sub-process
 * to signal the other ones to quit early.
 */
bool
follow_terminate_subprocesses(StreamSpecs *specs)
{
	FollowSubProcess *processArray[] = {
		&(specs->prefetch),
		&(specs->transform),
		&(specs->catchup)
	};
	int count = sizeof(processArray) / sizeof(processArray[0]);

	/* signal the processes to exit */
	for (int i = 0; i < count; i++)
	{
		if (processArray[i]->pid <= 0 || processArray[i]->exited)
		{
			continue;
		}

		log_notice("kill -TERM %d (%s)",
				   processArray[i]->pid,
				   processArray[i]->name);

		if (kill(processArray[i]->pid, SIGTERM) != 0)
		{
			/* process might have exited on its own already */
			if (errno != ESRCH)
			{
				log_error("Failed to signal %s process %d: %m",
						  processArray[i]->name,
						  processArray[i]->pid);

				return false;
			}
		}
	}

	return true;
}


/*
 * follow_wait_pid waits for a given known sub-process.
 */
bool
follow_wait_pid(pid_t subprocess, bool *exited, int *returnCode, int *sig)
{
	int status = 0;

	if (subprocess <= 0)
	{
		log_error("BUG: follow_wait_pid called with subprocess %d", subprocess);
		return false;
	}

	int pid = waitpid(subprocess, &status, WNOHANG);

	switch (pid)
	{
		case -1:
		{
			if (errno == ECHILD)
			{
				/* no more childrens */
				*sig = 0;
				*exited = true;
				*returnCode = -1;
				return true;
			}
			else
			{
				log_warn("Failed to call waitpid(): %m");
				return false;
			}

			break;
		}

		case 0:
		{
			/*
			 * We're using WNOHANG, 0 means there are no stopped or
			 * exited children, it's all good.
			 */
			*sig = 0;
			*exited = false;
			*returnCode = -1;
			break;
		}

		default:
		{
			/* sub-process has finished now */
			if (pid != subprocess)
			{
				log_error("BUG: waitpid on %d returned %d", subprocess, pid);
				return false;
			}

			*sig = 0;
			*exited = true;
			*returnCode = WEXITSTATUS(status);

			if (WIFSIGNALED(status))
			{
				*sig = WTERMSIG(status);
			}

			break;
		}
	}

	return true;
}
