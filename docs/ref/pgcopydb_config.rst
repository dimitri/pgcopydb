.. _pgcopydb_config:

pgcopydb config
===============

pgcopydb config - Get and Set configuration options for pgcopydb

This command prefixes the following sub-commands:

::

    pgcopydb config
      get  Get configuration option value
      set  Set configuration option value


The ``pgcopydb config`` commands are used to review or edit configuration
options for a currently running pgcopydb process. At start time, the
``pgcopydb clone`` command (and ``pgcopydb copy`` sub-commands) create a
configuration file with the pgcopydb.index-jobs and pgcopydb.table-jobs
settings that are used. It is possible to review those while the command is
running, and also to edit them using ``pgcopydb config set``.

The new value for the settings is only allowed to be greater than the
current value, that is to say, pgcopydb knows how to create new
sub-processes while running, but will not kill already running processes.

.. _pgcopydb_config_get:

pgcopydb config get
--------------------

pgcopydb config get - Get configuration option value

The command ``pgcopydb config get`` finds the configuration file created by
a previous pgcopydb command, possibly still running, and displays its
contents.

::

   pgcopydb config get: Get configuration option value
   usage: pgcopydb config get [ option-name ]

     --json    Format the output using JSON

.. _pgcopydb_config_set:

pgcopydb config set
--------------------

pgcopydb config set - Set configuration option value

The command ``pgcopydb config set`` finds the configuration file created by
a previous pgcopydb command, possibly still running, and edits the given
setting to the new value.

::

   pgcopydb config set: Set configuration option value
   usage: pgcopydb config set option-name value


Options
-------

--dir

  During its normal operations pgcopydb creates a lot of temporary files to
  track sub-processes progress. Temporary files are created in the directory
  location given by this option, or defaults to
  ``${TMPDIR}/pgcopydb`` when the environment variable is set, or
  then to ``/tmp/pgcopydb``.

--json

  The output of the command is formatted in JSON, when supported. Ignored
  otherwise.

--verbose, --notice

  Increase current verbosity. The default level of verbosity is INFO. In
  ascending order pgcopydb knows about the following verbosity levels:
  FATAL, ERROR, WARN, INFO, NOTICE, DEBUG, TRACE.

--debug

  Set current verbosity to DEBUG level.

--trace

  Set current verbosity to TRACE level.

--quiet

  Set current verbosity to ERROR level.


Examples
--------

::

   $ pgcopydb config get --json
   13:23:11 3342 INFO   Running pgcopydb version 0.11.37.g119d619.dirty from "/Applications/Postgres.app/Contents/Versions/12/bin/pgcopydb"
   13:23:11 3342 INFO   A previous run has run through completion
   {
       "pgcopydb": {
           "table-jobs": 4,
           "index-jobs": 8
       }
   }


::

   $ pgcopydb config set --quiet pgcopydb.index-jobs 4
   4
