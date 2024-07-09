/*
 * runprogram.h
 *
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the PostgreSQL License.
 *
 */

#include <fcntl.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/wait.h>

#include "pqexpbuffer.h"

/*
 * Now setup internal constants
 */
#define BUFSIZE 1024
#define ARGS_INCREMENT 12

#if defined(WIN32) && !defined(__CYGWIN__)
#define DEV_NULL "NUL"
#else
#define DEV_NULL "/dev/null"
#endif

#define MAX(a, b) (((a) > (b)) ? (a) : (b))

typedef struct
{
	char *program;
	char **args;
	bool setsid;                /* shall we call setsid() ? */

	int error;                  /* save errno when something's gone wrong */
	int returnCode;

	bool capture;               /* do we capture output, or redirect it? */
	bool tty;					/* do we share our tty? */

	/* register a function to process output as it appears */
	void (*processBuffer)(const char *buffer, bool error);

	int stdOutFd;               /* redirect stdout to file descriptor */
	int stdErrFd;               /* redirect stderr to file descriptor */

	char *stdOut;
	char *stdErr;
} Program;

Program *run_program(const char *program, ...);
void initialize_program(Program *prog, char **args, bool setsid);
void execute_subprogram(Program *prog);
void execute_program(Program *prog);
void free_program(Program *prog);
int snprintf_program_command_line(Program *prog, char *buffer, int size);

#ifdef RUN_PROGRAM_IMPLEMENTATION
#undef RUN_PROGRAM_IMPLEMENTATION

static void exit_internal_error(void);
static void dup2_or_exit(int fildes, int fildes2);
static void close_or_exit(int fildes);
static void read_from_pipes(Program *prog,
							pid_t childPid, int *outpipe, int *errpipe);
static size_t read_into_buf(Program *prog,
							int filedes,
							PQExpBuffer buffer,
							bool error);
static void waitprogram(Program *prog, pid_t childPid);

/*
 * Run a program using fork() and exec(), get the stdOut and stdErr output from
 * the run and then return a Program struct instance with the result of running
 * the program.
 */
Program *
run_program(const char *program, ...)
{
	int nb_args = 0;
	va_list args;
	const char *param;
	Program *prog = (Program *) malloc(sizeof(Program));

	if (prog == NULL)
	{
		return NULL;
	}

	prog->program = strdup(program);
	prog->returnCode = -1;
	prog->error = 0;
	prog->setsid = false;
	prog->capture = true;
	prog->tty = false;
	prog->processBuffer = NULL;
	prog->stdOutFd = -1;
	prog->stdErrFd = -1;
	prog->stdOut = NULL;
	prog->stdErr = NULL;

	prog->args = (char **) malloc(ARGS_INCREMENT * sizeof(char *));
	prog->args[nb_args++] = prog->program;

	va_start(args, program);
	while ((param = va_arg(args, const char *)) != NULL)
	{
		if (nb_args % ARGS_INCREMENT == 0)
		{
			int newcount = (ARGS_INCREMENT * (nb_args / ARGS_INCREMENT + 1));
			size_t newsize = newcount * sizeof(char *);

			prog->args = (char **) realloc(prog->args, newsize);

			if (prog->args == NULL)
			{
				return NULL;
			}
		}
		prog->args[nb_args++] = strdup(param);
	}
	va_end(args);
	prog->args[nb_args] = NULL;

	execute_subprogram(prog);

	return prog;
}


/*
 * Initialize a program structure that can be executed later, allowing the
 * caller to manipulate the structure for itself. Safe to change are program,
 * args and setsid structure slots.
 */
void
initialize_program(Program *prog, char **args, bool setsid)
{
	int argsIndex, nb_args = 0;

	/* we want to have a deterministic starting point */
	*prog = (Program) { 0 };

	prog->returnCode = -1;
	prog->error = 0;
	prog->setsid = setsid;

	/* this could be changed by the caller before calling execute_program */
	prog->capture = true;
	prog->tty = false;
	prog->processBuffer = NULL;
	prog->stdOutFd = -1;
	prog->stdErrFd = -1;

	prog->stdOut = NULL;
	prog->stdErr = NULL;

	for (argsIndex = 0; args[argsIndex] != NULL; argsIndex++)
	{
		++nb_args;
	}

	/* add another one nb_args for the terminating NULL entry */
	prog->args = (char **) malloc(++nb_args * sizeof(char *));
	memset(prog->args, 0, nb_args * sizeof(char *));

	for (argsIndex = 0; args[argsIndex] != NULL; argsIndex++)
	{
		prog->args[argsIndex] = strdup(args[argsIndex]);
	}
	prog->program = prog->args[0];
}


/*
 * Run given program with its args, by doing the fork()/exec() dance, and also
 * capture the subprocess output by installing pipes. We accumulate the output
 * into a PQExpBuffer when prog->capture is true.
 */
void
execute_subprogram(Program *prog)
{
	pid_t pid;
	int outpipe[2] = { 0, 0 };
	int errpipe[2] = { 0, 0 };

	/* first level sanity check */
	if (access(prog->program, F_OK | X_OK) == -1)
	{
		fprintf(stderr, "Failed to find executable program at \"%s\": %s\n",
				prog->program,
				strerror(errno));

		prog->returnCode = -1;
		prog->error = errno;
		return;
	}

	/* Flush stdio channels just before fork, to avoid double-output problems */
	fflush(stdout);
	fflush(stderr);

	/* create the output capture pipes now */
	if (prog->capture)
	{
		if (pipe(outpipe) < 0)
		{
			prog->returnCode = -1;
			prog->error = errno;
			return;
		}

		if (pipe(errpipe) < 0)
		{
			prog->returnCode = -1;
			prog->error = errno;
			return;
		}
	}

	pid = fork();

	switch (pid)
	{
		case -1:
		{
			/* fork failed */
			prog->returnCode = -1;
			prog->error = errno;
			return;
		}

		case 0:
		{
			/* fork succeeded, in child */

			if (prog->tty == false)
			{
				/*
				 * We redirect /dev/null into stdIn rather than closing stdin,
				 * because apparently closing it may cause undefined behavior
				 * if any read was to happen.
				 */
				int stdIn = open(DEV_NULL, O_RDONLY);

				if (stdIn == -1)
				{
					(void) exit_internal_error();
				}

				(void) dup2_or_exit(stdIn, STDIN_FILENO);
				(void) close_or_exit(stdIn);

				/*
				 * Prepare either for capture the output in pipes, or redirect
				 * to the given open file descriptors.
				 */
				if (prog->capture)
				{
					(void) dup2_or_exit(outpipe[1], STDOUT_FILENO);
					(void) dup2(errpipe[1], STDERR_FILENO);

					(void) close_or_exit(outpipe[0]);
					(void) close_or_exit(outpipe[1]);
					(void) close_or_exit(errpipe[0]);
					(void) close_or_exit(errpipe[1]);
				}
				else
				{
					(void) dup2_or_exit(prog->stdOutFd, STDOUT_FILENO);
					(void) dup2_or_exit(prog->stdErrFd, STDERR_FILENO);
				}
			}

			/*
			 * When asked to do so, before creating the child process, we call
			 * setsid() to create our own session group and detach from the
			 * terminal. That's useful when starting a service in the
			 * background.
			 */
			if (prog->setsid)
			{
				if (setsid() == -1)
				{
					prog->returnCode = -1;
					prog->error = errno;
					return;
				}
			}

			if (execv(prog->program, prog->args) == -1)
			{
				prog->returnCode = -1;
				prog->error = errno;

				fprintf(stdout, "%s\n", strerror(errno));
				fprintf(stderr, "%s\n", strerror(errno));
				exit(EXIT_CODE_INTERNAL_ERROR);
			}
			return;
		}

		default:
		{
			/* fork succeeded, in parent */
			if (prog->capture)
			{
				read_from_pipes(prog, pid, outpipe, errpipe);
			}
			else
			{
				(void) waitprogram(prog, pid);
			}
			return;
		}
	}
}


/*
 * Run given program with its args, by using exec().
 *
 * Using exec() means that we replace the currently running program and will
 * take ownership of its standard input, output and error streams, etc. This
 * routine is not supposed to ever return, so in case when something goes
 * wrong, it exits the current process, which is assumed to be a sub-process
 * started with fork().
 *
 * When prog->tty is true we want to share the parent's program tty with the
 * subprocess, and then we refrain from doing any redirection of stdin, stdout,
 * or stderr.
 */
void
execute_program(Program *prog)
{
	if (prog->capture)
	{
		fprintf(stderr, "BUG: can't execute_program and capture the output");
		return;
	}

	/* first level sanity check */
	if (access(prog->program, F_OK | X_OK) == -1)
	{
		fprintf(stderr, "Failed to find executable program at \"%s\": %s\n",
				prog->program,
				strerror(errno));

		prog->returnCode = -1;
		prog->error = errno;
		return;
	}

	if (prog->tty == false)
	{
		/*
		 * We redirect /dev/null into stdIn rather than closing stdin, because
		 * apparently closing it may cause undefined behavior if any read was
		 * to happen.
		 */
		int stdIn = open(DEV_NULL, O_RDONLY);

        if (stdIn < 0) {
		    prog->returnCode = -1;
		    prog->error = errno;
		    return;
        }

		/* Avoid double-output problems */
		fflush(stdout);
		fflush(stderr);

		(void) dup2_or_exit(stdIn, STDIN_FILENO);
		(void) close_or_exit(stdIn);

		(void) dup2_or_exit(prog->stdOutFd, STDOUT_FILENO);
		(void) dup2_or_exit(prog->stdErrFd, STDERR_FILENO);
	}

	/*
	 * When asked to do so, before creating the child process, we call
	 * setsid() to create our own session group and detach from the
	 * terminal. That's useful when starting a service in the
	 * background.
	 */
	if (prog->setsid)
	{
		if (setsid() == -1)
		{
			prog->returnCode = -1;
			prog->error = errno;
			return;
		}
	}

	if (execv(prog->program, prog->args) == -1)
	{
		prog->returnCode = -1;
		prog->error = errno;

		(void) exit_internal_error();
	}

	/* now the parent should waitpid() and may use waitprogram() */
}


/*
 * Free our memory.
 */
void
free_program(Program *prog)
{
	/* don't free prog->program, it's the same pointer as prog->args[0] */
	for (int i = 0; prog->args[i] != NULL; i++)
	{
		free(prog->args[i]);
	}
	free(prog->args);

	if (prog->stdOut != NULL)
	{
		free(prog->stdOut);
	}

	if (prog->stdErr != NULL)
	{
		free(prog->stdErr);
	}
}


/*
 * exit_internal_error prints the strerror of the current errno to both stdin
 * and stdout and exits with the exit code EXIT_CODE_INTERNAL_ERROR.
 */
static void
exit_internal_error()
{
	fprintf(stdout, "%s\n", strerror(errno));
	fprintf(stderr, "%s\n", strerror(errno));
	exit(EXIT_CODE_INTERNAL_ERROR);
}


/*
 * dup2_or_exit calls dup2() on given arguments (file descriptors) and exits
 * when dup2() fails.
 */
static void
dup2_or_exit(int fildes, int fildes2)
{
	if (dup2(fildes, fildes2) == -1)
	{
		(void) exit_internal_error();
	}
}


/*
 * close_or_exit calls close() on given file descriptor and exits when close()
 * fails.
 */
static void
close_or_exit(int fildes)
{
	if (close(fildes) == -1)
	{
		(void) exit_internal_error();
	}
}


/*
 * read_from_pipes reads the output from the child process and sets the Program
 * slots stdOut and stdErr with the accumulated output we read.
 */
static void
read_from_pipes(Program *prog, pid_t childPid, int *outpipe, int *errpipe)
{
	bool doneReading = false;
	int countFdsReadyToRead, nfds; /* see man select(3) */
	fd_set readFileDescriptorSet;
	ssize_t bytes_out = BUFSIZE, bytes_err = BUFSIZE;
	PQExpBuffer outbuf, errbuf;

	/* We read from the other side of the pipe, close that part.  */
	close(outpipe[1]);
	close(errpipe[1]);

	nfds = MAX(outpipe[0], errpipe[0]) + 1;

	/*
	 * Ok. the child process is running, let's read the pipes content.
	 */
	outbuf = createPQExpBuffer();
	errbuf = createPQExpBuffer();

	while (!doneReading)
	{
		FD_ZERO(&readFileDescriptorSet);

		/* if we read 0 bytes on the previous run, we've reached EOF */
		if (bytes_out > 0)
		{
			FD_SET(outpipe[0], &readFileDescriptorSet);
		}

		if (bytes_err > 0)
		{
			FD_SET(errpipe[0], &readFileDescriptorSet);
		}

		countFdsReadyToRead =
			select(nfds, &readFileDescriptorSet, NULL, NULL, NULL);

		if (countFdsReadyToRead == -1)
		{
			switch (errno)
			{
				case EAGAIN:
				case EINTR:
				{
					/* just loop again */
					break;
				}

				case EBADF:
				case EINVAL:
				case ENOMEM:
				default:
				{
					/* that's unexpected, act as if doneReading */
					log_error("Failed to read from command \"%s\": %s",
							  prog->program, strerror(errno));
					doneReading = true;
					break;
				}
			}
		}
		else if (countFdsReadyToRead == 0)
		{
			continue;
		}
		else
		{
			if (FD_ISSET(outpipe[0], &readFileDescriptorSet))
			{
				bytes_out = read_into_buf(prog, outpipe[0], outbuf, false);

				if (bytes_out == -1 && errno != 0)
				{
					prog->returnCode = -1;
					prog->error = errno;
				}
			}

			if (FD_ISSET(errpipe[0], &readFileDescriptorSet))
			{
				bytes_err = read_into_buf(prog, errpipe[0], errbuf, true);

				if (bytes_err == -1 && errno != 0)
				{
					prog->returnCode = -1;
					prog->error = errno;
				}
			}
			doneReading = (bytes_out < BUFSIZE && bytes_err < BUFSIZE);
		}
	}

	if (outbuf->len > 0)
	{
		prog->stdOut = strndup(outbuf->data, outbuf->len);
	}

	if (errbuf->len > 0)
	{
		prog->stdErr = strndup(errbuf->data, errbuf->len);
	}

	destroyPQExpBuffer(outbuf);
	destroyPQExpBuffer(errbuf);

	/* now, wait until the child process is done. */
	(void) waitprogram(prog, childPid);

	/*
	 * Now we're done reading from both stdOut and stdErr of the child
	 * process, so close the file descriptors and prepare the char *
	 * strings output in our Program structure.
	 *
	 * We must close the pipe after the child process has exited,
	 * or the program may be terminated by SIGPIPE, i.e. writing to
	 * an closed pipe.
	 */
	close(outpipe[0]);
	close(errpipe[0]);
}


/*
 * Wait until our Program is done.
 */
static void
waitprogram(Program *prog, pid_t childPid)
{
	int status;

	do {
		if (waitpid(childPid, &status, WUNTRACED) == -1)
		{
			prog->returnCode = -1;
			prog->error = errno;
			return;
		}
	} while (!WIFEXITED(status) && !WIFSIGNALED(status));

	if (WIFEXITED(status))
	{
		prog->returnCode = WEXITSTATUS(status);
	}
	else if (WIFSIGNALED(status))
	{
		int signo = WTERMSIG(status);
		/* standard exit value with fatal error signal `n`: 128 + n */
		prog->returnCode = 128 + signo;
	}
	else
	{
		log_fatal("unknown exit status: 0X%X", status);
		prog->returnCode = -1;
	}
}


/*
 * Read from a file descriptor and directly appends to our buffer string.
 */
static size_t
read_into_buf(Program *prog, int filedes, PQExpBuffer buffer, bool error)
{
	char temp_buffer[BUFSIZE+1] = { 0 };
	size_t bytes = read(filedes, temp_buffer, BUFSIZE);

	if (bytes > 0)
	{
		/* terminate the buffer after the length we read */
		temp_buffer[bytes] = '\0';

		appendPQExpBufferStr(buffer, temp_buffer);

		if (prog->processBuffer)
		{
			(*prog->processBuffer)(temp_buffer, error);
		}
	}
	return bytes;
}


/*
 * Writes the full command line of the given program into the given
 * pre-allocated buffer of given size, and returns how many bytes would have
 * been written in the buffer if it was large enough, like snprintf would do.
 */
int
snprintf_program_command_line(Program *prog, char *buffer, int size)
{
	char *currentPtr = buffer;
	int index, remainingBytes = BUFSIZE;

	if (prog->args[0] == NULL)
	{
		return 0;
	}

	for (index = 0; prog->args[index] != NULL; index++)
	{
		int n;

		/* replace an empty char buffer with '' */
		if (prog->args[index][0] == '\0')
		{
			n = snprintf(currentPtr, remainingBytes, " ''");
		}
		/* single-quote are needed when argument contains special chars */
		else if (strchr(prog->args[index], ' ') != NULL ||
				 strchr(prog->args[index], '?') != NULL ||
				 strchr(prog->args[index], '!') != NULL)
		{
			n = snprintf(currentPtr, remainingBytes, " '%s'", prog->args[index]);
		}
		else
		{
			n = snprintf(currentPtr, remainingBytes, " %s", prog->args[index]);
		}

		if (n >= remainingBytes)
		{
			return BUFSIZE - remainingBytes + n;
		}
		currentPtr += n;
		remainingBytes -= n;
	}
	return BUFSIZE - remainingBytes;
}


#endif  /* RUN_PROGRAM_IMPLEMENTATION */
