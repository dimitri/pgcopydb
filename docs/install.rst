Installing pgcopydb
===================

Several distributions are available for pgcopydb.

debian packages
---------------

Binary packages for debian and derivatives (ubuntu) are available from
`apt.postgresql.org`__ repository, install by following the linked
documentation and then::

  $ sudo apt-get install pgcopydb

__ https://wiki.postgresql.org/wiki/Apt


RPM packages
------------

The Postgres community repository for RPM packages is `yum.postgresql.org`__
and does not include binary packages for pgcopydb at this time.

__ https://yum.postgresql.org

Docker Images
-------------

Docker images are maintained for each tagged release at dockerhub, and also
built from the CI/CD integration on GitHub at each commit to the `main`
branch.

The DockerHub `dimitri/pgcopydb`__ repository is where the tagged releases
are made available. The image uses the Postgres version currently in debian
stable.

To use this docker image::

  $ docker run --rm -it dimitri/pgcopydb:v0.4 pgcopydb --version

__ https://hub.docker.com/r/dimitri/pgcopydb#!


Or you can use the CI/CD integration that publishes packages from the main
branch to the GitHub docker repository::

  $ docker pull ghcr.io/dimitri/pgcopydb:latest
  $ docker run --rm -it ghcr.io/dimitri/pgcopydb:latest pgcopydb --version
  $ docker run --rm -it ghcr.io/dimitri/pgcopydb:latest pgcopydb --help


Build from sources
------------------

Building from source requires a list of build-dependencies that's comparable
to that of Postgres itself. The pgcopydb source code is written in C and the
build process uses a GNU Makefile.

See our main `Dockerfile`__ for a complete recipe to build pgcopydb when
using a debian environment.

__ https://github.com/dimitri/pgcopydb/blob/main/Dockerfile

Then the build process is pretty simple, in its simplest form you can just
use ``make clean install``, if you want to be more fancy consider also::

  $ make -s clean
  $ make -s -j12 install
