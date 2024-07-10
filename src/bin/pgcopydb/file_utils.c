/*
 * src/bin/pgcopydb/file_utils.c
 *   Implementations of utility functions for reading and writing files
 */

#include <limits.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <unistd.h>

#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif

#include "postgres_fe.h"
#include "pqexpbuffer.h"
#include "snprintf.h"

#include "cli_root.h"
#include "defaults.h"
#include "env_utils.h"
#include "file_utils.h"
#include "log.h"
#include "signals.h"
#include "string_utils.h"

static bool read_file_internal(FILE *fileStream,
							   const char *filePath,
							   char **contents,
							   long *fileSize);

/*
 * file_exists returns true if the given filename is known to exist
 * on the file system or false if it does not exist or in case of
 * error.
 */
bool
file_exists(const char *filename)
{
	bool exists = access(filename, F_OK) != -1;
	if (!exists && errno != 0)
	{
		/*
		 * Only log "interesting" errors here.
		 *
		 * The fact that the file does not exist is not interesting: we're
		 * retuning false and the caller figures it out, maybe then creating
		 * the file.
		 */
		if (errno != ENOENT && errno != ENOTDIR)
		{
			log_error("Failed to check if file \"%s\" exists: %m", filename);
		}
		return false;
	}

	return exists;
}


/*
 * file_is_empty returns true if the given filename is known to exist on the
 * file system and is empty: its content is "".
 */
bool
file_is_empty(const char *filename)
{
	if (file_exists(filename))
	{
		char *fileContents;
		long fileSize;

		if (!read_file(filename, &fileContents, &fileSize))
		{
			/* errors are logged */
			return false;
		}

		if (fileSize == 0)
		{
			return true;
		}
	}

	return false;
}


/*
 * directory_exists returns whether the given path is the name of a directory that
 * exists on the file system or not.
 */
bool
directory_exists(const char *path)
{
	struct stat info;

	if (!file_exists(path))
	{
		return false;
	}

	if (stat(path, &info) != 0)
	{
		log_error("Failed to stat \"%s\": %m\n", path);
		return false;
	}

	bool result = (info.st_mode & S_IFMT) == S_IFDIR;
	return result;
}


/*
 * ensure_empty_dir ensures that the given path points to an empty directory with
 * the given mode. If it fails to do so, it returns false.
 */
bool
ensure_empty_dir(const char *dirname, int mode)
{
	/* pg_mkdir_p might modify its input, so create a copy of dirname. */
	char dirname_copy[MAXPGPATH];
	strlcpy(dirname_copy, dirname, MAXPGPATH);

	if (directory_exists(dirname))
	{
		if (!rmtree(dirname, true))
		{
			log_error("Failed to remove directory \"%s\": %m", dirname);
			return false;
		}
	}
	else
	{
		/*
		 * reset errno, we don't care anymore that it failed because dirname
		 * doesn't exists.
		 */
		errno = 0;
	}

	if (pg_mkdir_p(dirname_copy, mode) == -1)
	{
		log_error("Failed to ensure empty directory \"%s\": %m", dirname);
		return false;
	}

	return true;
}


/*
 * fopen_with_umask is a version of fopen that gives more control. The main
 * advantage of it is that it allows specifying a umask of the file. This makes
 * sure files are not accidentally created with umask 777 if the user has it
 * configured in a weird way.
 *
 * This function returns NULL when opening the file fails. So this should be
 * handled. It will log an error in this case though, so that's not necessary
 * at the callsite.
 */
FILE *
fopen_with_umask(const char *filePath, const char *modes, int flags, mode_t umask)
{
	int fileDescriptor = open(filePath, flags, umask);
	if (fileDescriptor == -1)
	{
		log_error("Failed to open file \"%s\": %m", filePath);
		return NULL;
	}

	FILE *fileStream = fdopen(fileDescriptor, modes);
	if (fileStream == NULL)
	{
		log_error("Failed to open file \"%s\": %m", filePath);
		close(fileDescriptor);
	}
	return fileStream;
}


/*
 * fopen_read_only opens the file as a read only stream.
 */
FILE *
fopen_read_only(const char *filePath)
{
	/*
	 * Explanation of IGNORE-BANNED
	 * fopen is safe here because we open the file in read only mode. So no
	 * exclusive access is needed.
	 */
	return fopen(filePath, "rb"); /* IGNORE-BANNED */
}


/*
 * write_file writes the given data to the file given by filePath using
 * our logging library to report errors. If succesful, the function returns
 * true.
 */
bool
write_file(char *data, long fileSize, const char *filePath)
{
	FILE *fileStream = fopen_with_umask(filePath, "wb", FOPEN_FLAGS_W, 0644);

	if (fileStream == NULL)
	{
		/* errors have already been logged */
		return false;
	}

	if (fwrite(data, sizeof(char), fileSize, fileStream) < fileSize)
	{
		log_error("Failed to write file \"%s\": %m", filePath);
		fclose(fileStream);
		return false;
	}

	if (fclose(fileStream) == EOF)
	{
		log_error("Failed to write file \"%s\"", filePath);
		return false;
	}

	return true;
}


/*
 * append_to_file writes the given data to the end of the file given by
 * filePath using our logging library to report errors. If succesful, the
 * function returns true.
 */
bool
append_to_file(char *data, long fileSize, const char *filePath)
{
	FILE *fileStream = fopen_with_umask(filePath, "ab", FOPEN_FLAGS_A, 0644);

	if (fileStream == NULL)
	{
		/* errors have already been logged */
		return false;
	}

	if (fwrite(data, sizeof(char), fileSize, fileStream) < fileSize)
	{
		log_error("Failed to write file \"%s\": %m", filePath);
		fclose(fileStream);
		return false;
	}

	if (fclose(fileStream) == EOF)
	{
		log_error("Failed to write file \"%s\"", filePath);
		return false;
	}

	return true;
}


/*
 * read_file_if_exists is a utility function that reads the contents of a file
 * using our logging library to report errors. ENOENT is not considered worth
 * of a log message in this function, and we still return false in that case.
 *
 * If successful, the function returns true and fileSize points to the number
 * of bytes that were read and contents points to a buffer containing the entire
 * contents of the file. This buffer should be freed by the caller.
 */
bool
read_file_if_exists(const char *filePath, char **contents, long *fileSize)
{
	/* open a file */
	FILE *fileStream = fopen_read_only(filePath);

	if (fileStream == NULL)
	{
		if (errno != ENOENT)
		{
			log_error("Failed to open file \"%s\": %m", filePath);
		}
		return false;
	}

	return read_file_internal(fileStream, filePath, contents, fileSize);
}


/*
 * read_file is a utility function that reads the contents of a file using our
 * logging library to report errors.
 *
 * If successful, the function returns true and fileSize points to the number
 * of bytes that were read and contents points to a buffer containing the entire
 * contents of the file. This buffer should be freed by the caller.
 */
bool
read_file(const char *filePath, char **contents, long *fileSize)
{
	/* open a file */
	FILE *fileStream = fopen_read_only(filePath);
	if (fileStream == NULL)
	{
		log_error("Failed to open file \"%s\": %m", filePath);
		return false;
	}

	return read_file_internal(fileStream, filePath, contents, fileSize);
}


/*
 * read_file_internal is shared by both read_file and read_file_if_exists
 * functions.
 */
static bool
read_file_internal(FILE *fileStream,
				   const char *filePath, char **contents, long *fileSize)
{
	/* get the file size */
	if (fseek(fileStream, 0, SEEK_END) != 0)
	{
		log_error("Failed to read file \"%s\": %m", filePath);
		fclose(fileStream);
		return false;
	}

	*fileSize = ftell(fileStream);
	if (*fileSize < 0)
	{
		log_error("Failed to read file \"%s\": %m", filePath);
		fclose(fileStream);
		return false;
	}

	if (fseek(fileStream, 0, SEEK_SET) != 0)
	{
		log_error("Failed to read file \"%s\": %m", filePath);
		fclose(fileStream);
		return false;
	}

	/* read the contents */
	char *data = malloc(*fileSize + 1);
	if (data == NULL)
	{
		log_error("Failed to allocate %ld bytes", *fileSize);
		log_error(ALLOCATION_FAILED_ERROR);
		fclose(fileStream);
		return false;
	}

	if (fread(data, sizeof(char), *fileSize, fileStream) < *fileSize)
	{
		log_error("Failed to read file \"%s\": %m", filePath);
		fclose(fileStream);
		return false;
	}

	if (fclose(fileStream) == EOF)
	{
		log_error("Failed to read file \"%s\"", filePath);
		return false;
	}

	data[*fileSize] = '\0';
	*contents = data;

	return true;
}


/*
 * file_iter_lines read a file's content line-by-line, and for each line calls
 * the user-provided callback function.
 */
bool
file_iter_lines(const char *filename,
				size_t bufsize,
				void *context, FileIterLinesFun *callback)
{
	FileLinesIterator *iter =
		(FileLinesIterator *) calloc(1, sizeof(FileLinesIterator));

	if (iter == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	iter->filename = filename;
	iter->bufsize = bufsize;

	if (!file_iter_lines_init(iter))
	{
		/* errors have already been logged */
		return false;
	}

	for (;;)
	{
		if (!file_iter_lines_next(iter))
		{
			/* errors have already been logged */
			return false;
		}

		if (iter->line == NULL)
		{
			if (!file_iter_lines_finish(iter))
			{
				/* errors have already been logged */
				return false;
			}

			break;
		}

		if (!(*callback)(context, iter->line))
		{
			log_error("Failed to iterate over lines of file \"%s\", "
					  "see above for details",
					  iter->filename);
			return false;
		}
	}

	return true;
}


/*
 * file_iter_lines_init initializes an Iterator over a file to read it
 * line-by-line and allocate only one line at a time.
 */
bool
file_iter_lines_init(FileLinesIterator *iter)
{
	iter->stream = fopen_read_only(iter->filename);

	if (iter->stream == NULL)
	{
		log_error("Failed to open file \"%s\": %m", iter->filename);
		return false;
	}

	/*
	 * Allocate a buffer to hold the line contents, and re-use the same buffer
	 * over and over when reading the next line.
	 */
	iter->line = calloc(1, iter->bufsize * sizeof(char));

	if (iter->line == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	return true;
}


/*
 * file_iter_lines_next fetches the next line in the opened file.
 */
bool
file_iter_lines_next(FileLinesIterator *iter)
{
	size_t len = iter->bufsize - 1;

	char *lineptr = fgets(iter->line, len, iter->stream);

	/* Upon successful completion, fgets() shall return s. */
	if (lineptr == iter->line)
	{
		return true;
	}

	if (lineptr == NULL)
	{
		/*
		 * If the stream is at end-of-file, the end-of-file indicator for the
		 * stream shall be set and fgets() shall return a null pointer.
		 */
		if (feof(iter->stream) != 0)
		{
			/* signal end-of-file by setting line to NULL */
			iter->line = NULL;
			return true;
		}

		/*
		 * If a read error occurs, the error indicator for the stream shall be
		 * set, fgets() shall return a null pointer, and shall set errno to
		 * indicate the error.
		 */
		log_error("Failed to iterate over file \"%s\": %m", iter->filename);
		return false;
	}

	return true;
}


/*
 * file_iter_lines_finish closes the file that was iterated over.
 */
bool
file_iter_lines_finish(FileLinesIterator *iter)
{
	if (fclose(iter->stream) == EOF)
	{
		log_error("Failed to read file \"%s\"", iter->filename);
		return false;
	}

	return true;
}


/*
 * write_to_stream writes given buffer of given size to the given stream. It
 * loops around calling write(2) if necessary: not all the bytes of the buffer
 * might be sent in a single call.
 */
bool
write_to_stream(FILE *stream, const char *buffer, size_t size)
{
	long bytes_left = size;
	long bytes_written = 0;

	while (bytes_left > 0)
	{
		int ret;

		ret = fwrite(buffer + bytes_written,
					 sizeof(char),
					 bytes_left,
					 stream);

		if (ret < 0)
		{
			log_error("Failed to write %ld bytes: %m", bytes_left);
			return false;
		}

		/* Write was successful, advance our position */
		bytes_written += ret;
		bytes_left -= ret;
	}

	return true;
}


/*
 * read_from_stream reads lines from an input stream, such as a Unix Pipe, and
 * for each line read calls the provided context->callback function with its
 * own private context as an argument.
 */
bool
read_from_stream(FILE *stream, ReadFromStreamContext *context)
{
	int countFdsReadyToRead, nfds; /* see man select(2) */
	fd_set readFileDescriptorSet;
	fd_set exceptFileDescriptorSet;

	context->fd = fileno(stream);
	nfds = context->fd + 1;

	bool doneReading = false;

	uint64_t multiPartCount = 0;
	PQExpBuffer multiPartBuffer = NULL;

	while (!doneReading)
	{
		/* feof returns non-zero when the end-of-file indicator is set */
		if (feof(stream) != 0)
		{
			log_debug("read_from_stream: stream closed");
			break;
		}

		struct timeval timeout = { 0, 100 * 1000 }; /* 100 ms */

		FD_ZERO(&readFileDescriptorSet);
		FD_SET(context->fd, &readFileDescriptorSet);

		FD_ZERO(&exceptFileDescriptorSet);
		FD_SET(context->fd, &exceptFileDescriptorSet);

		countFdsReadyToRead =
			select(nfds,
				   &readFileDescriptorSet, NULL, &exceptFileDescriptorSet,
				   &timeout);

		if (countFdsReadyToRead == -1)
		{
			log_debug("countFdsReadyToRead == -1");

			if (errno == EINTR || errno == EAGAIN)
			{
				log_debug("received EINTR or EAGAIN");

				if (asked_to_quit)
				{
					/*
					 * When asked_to_stop || asked_to_stop_fast still continue
					 * reading through EOF on the input stream, then quit
					 * normally.
					 */
					doneReading = true;
				}

				continue;
			}
			else
			{
				log_error("Failed to select on file descriptor %d: %m",
						  context->fd);
				return false;
			}
		}

		if (FD_ISSET(context->fd, &exceptFileDescriptorSet))
		{
			log_error("Failed to select on file descriptor %d: "
					  "an exceptional condition happened",
					  context->fd);
			return false;
		}

		/*
		 * When asked_to_stop || asked_to_stop_fast still continue reading
		 * through EOF on the input stream, then quit normally. Here when
		 * select(2) reports that there is no data to read, it's a good time to
		 * quit.
		 */
		if (countFdsReadyToRead == 0)
		{
			if (asked_to_quit)
			{
				doneReading = true;
				log_notice("read_from_stream was asked to quit");
			}

			continue;
		}

		/*
		 * data is expected to be written one line at a time, if any data is
		 * available per select(2) call, then we should be able to read an
		 * entire line now.
		 */
		if (FD_ISSET(context->fd, &readFileDescriptorSet))
		{
			/*
			 * Typical Unix PIPE buffer size is 64kB. Make sure it fits in our
			 * buffer.
			 */
			size_t availableBytes = 0;

			if (ioctl(context->fd, FIONREAD, &availableBytes) == -1)
			{
				log_debug("Failed to request current PIPE buffer size: %m");
				availableBytes = 128 * 1024;
			}

			/* add 1 byte for the terminating \0 */
			char *buf = calloc(availableBytes + 1, sizeof(char));
			ssize_t bytes = read(context->fd, buf, availableBytes);

			if (bytes == -1)
			{
				log_error("Failed to read from input stream: %m");
				return false;
			}
			else if (bytes == 0)
			{
				doneReading = true;
				continue;
			}

			/* ensure properly terminated C-string now */
			buf[bytes] = '\0';

			/* if the buffer doesn't terminate with \n it's a partial read */
			bool partialRead = buf[bytes - 1] != '\n';
			LinesBuffer lbuf = { 0 };

			if (!splitLines(&lbuf, buf))
			{
				/* errors have already been logged */
				return false;
			}

			log_trace("read_from_stream read %6zu bytes in %lld lines %s[%lld]",
					  bytes,
					  (long long) lbuf.count,
					  partialRead ? "partial" : "",
					  (long long) multiPartCount);

			for (uint64_t i = 0; i < lbuf.count; i++)
			{
				/*
				 * Now might look like a good time to check for interrupts...
				 * That said we want to finish processing the current buffer.
				 */
				char *line = lbuf.lines[i];

				/*
				 * Take care of partial reads:
				 *
				 * - when we're reading the first partial buffer of a series
				 *   (partialRead is true, multiPartCount is still zero) append
				 *   only the last line received to the multiPartBuffer.
				 *
				 * - when we're reading a middle part partial buffer then
				 *   multiPartCount is non-zero and lineCount is 1 and i == 0.
				 *
				 * - when we're reading the last partial buffer of a series
				 *   (partialRead is false or lineCount > 1, multiPartCount is
				 *   non-zero) append only the first line received to the
				 *   multiPartBuffer.
				 *
				 * - we could also receive the last part of a multiPartBuffer
				 *   and the first part of the next multiPartBuffer in the
				 *   same read() call, hence the previous para condition:
				 *
				 *   multiPartCount > 0 && (!partialRead || lineCount > 1)
				 */
				bool firstLine = i == 0;
				bool lastLine = (i == (lbuf.count - 1));
				bool callUserCallback = true;
				bool appendToCurrentBuffer = false;

				/* first part of a multi-part buffer (last line read) */
				if (partialRead && multiPartCount == 0 && lastLine)
				{
					multiPartBuffer = createPQExpBuffer();
					callUserCallback = false;
					appendToCurrentBuffer = true;
				}

				/* middle part of a multi-part buffer */
				else if (partialRead && multiPartCount > 0 && lbuf.count == 1)
				{
					if (multiPartBuffer == NULL)
					{
						log_error("BUG: multiPartBuffer is NULL, "
								  "multiPartCount == %lld, "
								  "line == %lld, lineCount == 1",
								  (long long) multiPartCount,
								  (long long) i);
						return false;
					}
					callUserCallback = false;
					appendToCurrentBuffer = true;
				}

				/* last part of a multi-part buffer */
				else if (multiPartCount > 0 && firstLine)
				{
					callUserCallback = true;
					appendToCurrentBuffer = true;
				}

				/*
				 * If needed append to the current buffer, which has
				 * already been created even when multiPartCount is zero.
				 */
				if (appendToCurrentBuffer)
				{
					if (multiPartBuffer == NULL)
					{
						log_error("BUG: appendToCurrentBuffer is true, "
								  "multiPartBuffer is NULL");
						return false;
					}

					++multiPartCount;
					appendPQExpBufferStr(multiPartBuffer, line);

					if (PQExpBufferBroken(multiPartBuffer))
					{
						log_error("Failed to read multi-part message: "
								  "out of memory");
						destroyPQExpBuffer(multiPartBuffer);
						return false;
					}
				}

				/*
				 * Unless still reading a multi-part message, call user-defined
				 * callback function.
				 */
				if (callUserCallback)
				{
					/* replace the line pointer for multi-parts messages */
					if (multiPartCount > 0)
					{
						line = multiPartBuffer->data;
					}

					/* we count stream input lines as if reading from a file */
					++context->lineno;

					/* call the user-provided function */
					bool stop = false;

					if (!(*context->callback)(context->ctx, line, &stop))
					{
						destroyPQExpBuffer(multiPartBuffer);
						return false;
					}

					/* reset multiPartBuffer and count after callback */
					if (multiPartCount > 0)
					{
						destroyPQExpBuffer(multiPartBuffer);
						multiPartCount = 0;
						multiPartBuffer = NULL;
					}

					if (stop)
					{
						doneReading = true;
						break;
					}
				}
			}
		}

		/* doneReading might have been set from the user callback already */
		doneReading = doneReading || feof(stream) != 0;
	}

	return true;
}


/*
 * move_file is a utility function to move a file from sourcePath to
 * destinationPath. It behaves like mv system command. First attempts to move
 * a file using rename. if it fails with EXDEV error, the function duplicates
 * the source file with owner and permission information and removes it.
 */
bool
move_file(char *sourcePath, char *destinationPath)
{
	if (strncmp(sourcePath, destinationPath, MAXPGPATH) == 0)
	{
		/* nothing to do */
		log_warn("Source and destination are the same \"%s\", nothing to move.",
				 sourcePath);
		return true;
	}

	if (!file_exists(sourcePath))
	{
		log_error("Failed to move file, source file \"%s\" does not exist.",
				  sourcePath);
		return false;
	}

	if (file_exists(destinationPath))
	{
		log_error("Failed to move file, destination file \"%s\" already exists.",
				  destinationPath);
		return false;
	}

	/* first try atomic move operation */
	if (rename(sourcePath, destinationPath) == 0)
	{
		return true;
	}

	/*
	 * rename fails with errno = EXDEV when moving file to a different file
	 * system.
	 */
	if (errno != EXDEV)
	{
		log_error("Failed to move file \"%s\" to \"%s\": %m",
				  sourcePath, destinationPath);
		return false;
	}

	if (!duplicate_file(sourcePath, destinationPath))
	{
		/* specific error is already logged */
		log_error("Canceling file move due to errors.");
		return false;
	}

	/* everything is successful we can remove the file */
	unlink_file(sourcePath);

	return true;
}


/*
 * duplicate_file is a utility function to duplicate a file from sourcePath to
 * destinationPath. It reads the contents of the source file and writes to the
 * destination file. It expects non-existing destination file and does not
 * copy over if it exists. The function returns true on successful execution.
 *
 * Note: the function reads the whole file into memory before copying out.
 */
bool
duplicate_file(char *sourcePath, char *destinationPath)
{
	char *fileContents;
	long fileSize;
	struct stat sourceFileStat;

	if (!read_file(sourcePath, &fileContents, &fileSize))
	{
		/* errors are logged */
		return false;
	}

	if (file_exists(destinationPath))
	{
		log_error("Failed to duplicate, destination file already exists : %s",
				  destinationPath);
		return false;
	}

	bool foundError = !write_file(fileContents, fileSize, destinationPath);


	if (foundError)
	{
		/* errors are logged in write_file */
		return false;
	}

	/* set uid gid and mode */
	if (stat(sourcePath, &sourceFileStat) != 0)
	{
		log_error("Failed to get ownership and file permissions on \"%s\"",
				  sourcePath);
		foundError = true;
	}
	else
	{
		if (chown(destinationPath, sourceFileStat.st_uid, sourceFileStat.st_gid) != 0)
		{
			log_error("Failed to set user and group id on \"%s\"",
					  destinationPath);
			foundError = true;
		}
		if (chmod(destinationPath, sourceFileStat.st_mode) != 0)
		{
			log_error("Failed to set file permissions on \"%s\"",
					  destinationPath);
			foundError = true;
		}
	}

	if (foundError)
	{
		/* errors are already logged */
		unlink_file(destinationPath);
		return false;
	}

	return true;
}


/*
 * create_symbolic_link creates a symbolic link to source path.
 */
bool
create_symbolic_link(char *sourcePath, char *targetPath)
{
	if (symlink(sourcePath, targetPath) != 0)
	{
		log_error("Failed to create symbolic link \"%s\" -> \"%s\": %m",
				  targetPath,
				  sourcePath);
		return false;
	}
	return true;
}


/*
 * path_in_same_directory constructs the path for a file with name fileName
 * that is in the same directory as basePath, which should be an absolute
 * path. The result is written to destinationPath, which should be at least
 * MAXPATH in size.
 */
void
path_in_same_directory(const char *basePath, const char *fileName,
					   char *destinationPath)
{
	strlcpy(destinationPath, basePath, MAXPGPATH);
	get_parent_directory(destinationPath);
	join_path_components(destinationPath, destinationPath, fileName);
}


/* From PostgreSQL sources at src/port/path.c */
#ifndef WIN32
#define IS_PATH_VAR_SEP(ch) ((ch) == ':')
#else
#define IS_PATH_VAR_SEP(ch) ((ch) == ';')
#endif


/*
 * search_path_first copies the first entry found in PATH to result. result
 * should be a buffer of (at least) MAXPGPATH size.
 * The function returns false and logs an error when it cannot find the command
 * in PATH.
 */
bool
search_path_first(const char *filename, char *result, int logLevel)
{
	SearchPath paths = { 0 };

	if (!search_path(filename, &paths) || paths.found == 0)
	{
		log_level(logLevel, "Failed to find %s command in your PATH", filename);
		return false;
	}

	strlcpy(result, paths.matches[0], MAXPGPATH);

	return true;
}


/*
 * Searches all the directories in the PATH environment variable for the given
 * filename. Returns number of occurrences and each match found with its
 * fullname, including the given filename, in the given pre-allocated
 * SearchPath result.
 */
bool
search_path(const char *filename, SearchPath *result)
{
	char pathlist[MAXPATHSIZE] = { 0 };

	/* we didn't count nor find anything yet */
	result->found = 0;

	/* Create a copy of pathlist, because we modify it here. */
	if (!get_env_copy("PATH", pathlist, sizeof(pathlist)))
	{
		/* errors have already been logged */
		return false;
	}

	char *path = pathlist;

	while (path != NULL)
	{
		char candidate[MAXPGPATH] = { 0 };
		char *sep = first_path_var_separator(path);

		/* split path on current token, null-terminating string at separator */
		if (sep != NULL)
		{
			*sep = '\0';
		}

		(void) join_path_components(candidate, path, filename);
		(void) canonicalize_path(candidate);

		if (file_exists(candidate))
		{
			strlcpy(result->matches[result->found++], candidate, MAXPGPATH);
		}

		path = (sep == NULL ? NULL : sep + 1);
	}

	return true;
}


/*
 * search_path_deduplicate_symlinks traverse the SearchPath result obtained by
 * calling the search_path() function and removes entries that are pointing to
 * the same binary on-disk.
 *
 * In modern debian installations, for instance, we have /bin -> /usr/bin; and
 * then we might find pg_config both in /bin/pg_config and /usr/bin/pg_config
 * although it's only been installed once, and both are the same file.
 *
 * We use realpath() to deduplicate entries, and keep the entry that is not a
 * symbolic link.
 */
bool
search_path_deduplicate_symlinks(SearchPath *results, SearchPath *dedup)
{
	/* now re-initialize the target structure dedup */
	dedup->found = 0;

	for (int rIndex = 0; rIndex < results->found; rIndex++)
	{
		bool alreadyThere = false;

		char *currentPath = results->matches[rIndex];
		char currentRealPath[PATH_MAX] = { 0 };

		if (realpath(currentPath, currentRealPath) == NULL)
		{
			log_error("Failed to normalize file name \"%s\": %m", currentPath);
			return false;
		}

		/* add-in the realpath to dedup, unless it's already in there */
		for (int dIndex = 0; dIndex < dedup->found; dIndex++)
		{
			if (strcmp(dedup->matches[dIndex], currentRealPath) == 0)
			{
				alreadyThere = true;

				log_debug("dedup: skipping \"%s\"", currentPath);
				break;
			}
		}

		if (!alreadyThere)
		{
			int bytesWritten =
				strlcpy(dedup->matches[dedup->found++],
						currentRealPath,
						MAXPGPATH);

			if (bytesWritten >= MAXPGPATH)
			{
				log_error(
					"Real path \"%s\" is %d bytes long, and pgcopydb "
					"is limited to handling paths of %d bytes long, maximum",
					currentRealPath,
					(int) strlen(currentRealPath),
					MAXPGPATH);

				return false;
			}
		}
	}

	return true;
}


/*
 * unlink_state_file calls unlink(2) on the state file to make sure we don't
 * leave a lingering state on-disk.
 */
bool
unlink_file(const char *filename)
{
	if (unlink(filename) == -1)
	{
		/* if it didn't exist yet, good news! */
		if (errno != ENOENT && errno != ENOTDIR)
		{
			log_error("Failed to remove file \"%s\": %m", filename);
			return false;
		}
	}

	return true;
}


/*
 * close_fd_or_exit calls close(2) on given file descriptor, and exits if that
 * failed.
 */
void
close_fd_or_exit(int fd)
{
	if (close(fd) != 0)
	{
		log_fatal("Failed to close fd %d: %m", fd);
		exit(EXIT_CODE_INTERNAL_ERROR);
	}
}


/*
 * get_program_absolute_path returns the absolute path of the current program
 * being executed. Note: the shell is responsible to set that in interactive
 * environments, and when the pgcopydb binary is in the PATH of the user,
 * then argv[0] (here pgcopydb_argv0) is just "pgcopydb".
 */
bool
set_program_absolute_path(char *program, int size)
{
#if defined(__APPLE__)
	int actualSize = _NSGetExecutablePath(program, (uint32_t *) &size);

	if (actualSize != 0)
	{
		log_error("Failed to get absolute path for the pgcopydb program, "
				  "absolute path requires %d bytes and we support paths up "
				  "to %d bytes only", actualSize, size);
		return false;
	}

	log_debug("Found absolute program: \"%s\"", program);

#else

	/*
	 * On Linux and FreeBSD and Solaris, we can find a symbolic link to our
	 * program and get the information with readlink. Of course the /proc entry
	 * to read is not the same on both systems, so we try several things here.
	 */
	bool found = false;
	char *procEntryCandidates[] = {
		"/proc/self/exe",       /* Linux */
		"/proc/curproc/file",   /* FreeBSD */
		"/proc/self/path/a.out" /* Solaris */
	};
	int procEntrySize = sizeof(procEntryCandidates) / sizeof(char *);
	int procEntryIndex = 0;

	for (procEntryIndex = 0; procEntryIndex < procEntrySize; procEntryIndex++)
	{
		if (readlink(procEntryCandidates[procEntryIndex], program, size) != -1)
		{
			found = true;
			log_debug("Found absolute program \"%s\" in \"%s\"",
					  program,
					  procEntryCandidates[procEntryIndex]);
		}
		else
		{
			/* when the file does not exist, we try our next guess */
			if (errno != ENOENT && errno != ENOTDIR)
			{
				log_error("Failed to get absolute path for the "
						  "pgcopydb program: %m");
				return false;
			}
		}
	}

	if (found)
	{
		return true;
	}
	else
	{
		/*
		 * Now either return pgcopydb_argv0 when that's an absolute filename,
		 * or search for it in the PATH otherwise.
		 */
		SearchPath paths = { 0 };

		if (pgcopydb_argv0[0] == '/')
		{
			strlcpy(program, pgcopydb_argv0, size);
			return true;
		}

		if (!search_path(pgcopydb_argv0, &paths) || paths.found == 0)
		{
			log_error("Failed to find \"%s\" in PATH environment",
					  pgcopydb_argv0);
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
		else
		{
			log_debug("Found \"%s\" in PATH at \"%s\"",
					  pgcopydb_argv0, paths.matches[0]);
			strlcpy(program, paths.matches[0], size);

			return true;
		}
	}
#endif

	return true;
}


/*
 * normalize_filename returns the real path of a given filename that belongs to
 * an existing file on-disk, resolving symlinks and pruning double-slashes and
 * other weird constructs. filename and dst are allowed to point to the same
 * adress.
 */
bool
normalize_filename(const char *filename, char *dst, int size)
{
	/* normalize the path to the configuration file, if it exists */
	if (file_exists(filename))
	{
		char realPath[PATH_MAX] = { 0 };

		if (realpath(filename, realPath) == NULL)
		{
			log_fatal("Failed to normalize file name \"%s\": %m", filename);
			return false;
		}

		if (strlcpy(dst, realPath, size) >= size)
		{
			log_fatal("Real path \"%s\" is %d bytes long, and pgcopydb "
					  "is limited to handling paths of %d bytes long, maximum",
					  realPath, (int) strlen(realPath), size);
			return false;
		}
	}
	else
	{
		char realPath[PATH_MAX] = { 0 };

		/* protect against undefined behavior if dst overlaps with filename */
		strlcpy(realPath, filename, MAXPGPATH);
		strlcpy(dst, realPath, MAXPGPATH);
	}

	return true;
}


/*
 * fformat is a secured down version of pg_fprintf:
 *
 * Additional security checks are:
 *  - make sure stream is not null
 *  - make sure fmt is not null
 *  - rely on pg_fprintf Assert() that %s arguments are not null
 */
int
fformat(FILE *stream, const char *fmt, ...)
{
	va_list args;

	if (stream == NULL || fmt == NULL)
	{
		log_error("BUG: fformat is called with a NULL target or format string");
		return -1;
	}

	va_start(args, fmt);
	int len = pg_vfprintf(stream, fmt, args);
	va_end(args);
	return len;
}


/*
 * sformat is a secured down version of pg_snprintf
 */
int
sformat(char *str, size_t count, const char *fmt, ...)
{
	va_list args;

	if (str == NULL || fmt == NULL)
	{
		log_error("BUG: sformat is called with a NULL target or format string");
		return -1;
	}

	va_start(args, fmt);
	int len = pg_vsnprintf(str, count, fmt, args);
	va_end(args);

	if (len >= count)
	{
		log_error("BUG: sformat needs %d bytes to expend format string \"%s\", "
				  "and a target string of %zu bytes only has been given.",
				  len, fmt, count);
	}

	return len;
}


/*
 * set_ps_title sets the process title seen in ps/top and friends, truncating
 * if there is not enough space, rather than causing memory corruption.
 *
 * Inspired / stolen from Postgres code src/backend/utils/misc/ps_status.c with
 * most of the portability bits removed. At the moment we prefer simple code
 * that works on few targets to highly portable code.
 */
void
init_ps_buffer(int argc, char **argv)
{
#if defined(__linux__) || defined(__darwin__)
	char *end_of_area = NULL;
	int i;

	/*
	 * check for contiguous argv strings
	 */
	for (i = 0; i < argc; i++)
	{
		if (i == 0 || end_of_area + 1 == argv[i])
		{
			end_of_area = argv[i] + strlen(argv[i]); /* lgtm[cpp/tainted-arithmetic] */
		}
	}

	if (end_of_area == NULL)    /* probably can't happen? */
	{
		ps_buffer = NULL;
		ps_buffer_size = 0;
		return;
	}

	ps_buffer = argv[0];
	last_status_len = ps_buffer_size = end_of_area - argv[0]; /* lgtm[cpp/tainted-arithmetic] */

#else
	ps_buffer = NULL;
	ps_buffer_size = 0;

	return;
#endif
}


/*
 * set_ps_title sets our process name visible in ps/top/pstree etc.
 */
void
set_ps_title(const char *title)
{
	if (ps_buffer == NULL)
	{
		/* noop */
		return;
	}

	/* pad our process title string */
	int n = strlen(title);

	for (size_t i = 0; i < ps_buffer_size; i++)
	{
		if (i < n)
		{
			*(ps_buffer + i) = title[i];
		}
		else
		{
			*(ps_buffer + i) = '\0';
		}
	}

	/* make sure we have an \0 at the end of the ps_buffer */
	*(ps_buffer + ps_buffer_size - 1) = '\0';
}
