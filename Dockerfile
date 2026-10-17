# syntax=docker/dockerfile:latest
# Define a base image with all our build dependencies.
FROM --platform=${TARGETPLATFORM} debian:12-slim AS build

# multi-arch
ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH
ARG PGVERSION=18

RUN dpkg --add-architecture ${TARGETARCH:-arm64} && apt update \
  && apt install -qqy --no-install-recommends \
	curl \
	ca-certificates \
	gnupg

RUN curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN echo "deb http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main ${PGVERSION}" > /etc/apt/sources.list.d/pgdg.list

RUN dpkg --add-architecture ${TARGETARCH:-arm64} && apt update \
  && apt install -qqy --no-install-recommends \
    libncurses-dev \
    libxml2-dev \
    sudo \
    valgrind \
    build-essential \
    libedit-dev \
    libgc-dev \
    libicu-dev \
    libkrb5-dev \
    liblz4-dev \
    libncurses6 \
    libnuma-dev \
    libpam-dev \
    libpq-dev \
    libpq5 \
    libreadline-dev \
    libselinux1-dev \
    libssl-dev \
    libxslt1-dev \
    libzstd-dev \
    lsof \
    psmisc \
    gdb \
    strace \
    tmux \
    watch \
    make \
    openssl \
    postgresql-server-dev-${PGVERSION} \
    psutils \
    tmux \
    watch \
    zlib1g-dev

WORKDIR /usr/src/pgcopydb

COPY Makefile .
COPY GIT-VERSION-GEN .
COPY GIT-VERSION-FILE .
COPY version .

# Separate building SQLite lib (and binary) for docker cache benefits
COPY src/bin/lib/sqlite src/bin/lib/sqlite
RUN make -C src/bin/lib/sqlite clean sqlite3.o sqlite3
RUN install src/bin/lib/sqlite/sqlite3 /usr/local/bin/sqlite3
RUN sqlite3 --version

# The COPY --exclude flag is not yet available in Docker releases
#COPY --exclude src/bin/lib/sqlite src src

COPY src/bin/lib/jenkins src/bin/lib/jenkins
COPY src/bin/lib/libs src/bin/lib/libs
COPY src/bin/lib/log src/bin/lib/log
COPY src/bin/lib/parson src/bin/lib/parson
COPY src/bin/lib/pg src/bin/lib/pg
COPY src/bin/lib/subcommands.c src/bin/lib/subcommands.c
COPY src/bin/lib/uthash src/bin/lib/uthash

COPY src/bin/Makefile src/bin/Makefile
COPY src/bin/pgcopydb src/bin/pgcopydb

RUN make -s clean && make -s -j$(nproc) install

# When only tests are updated, reuse previous binary build
COPY tests tests

# Now the "run" image, as small as possible
FROM --platform=${TARGETPLATFORM} debian:12-slim AS run

# multi-arch
ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH
ARG PGVERSION=18

# Postgres client tool versions installed side by side in the run image.
#
# pgcopydb resolves psql, and then pg_dump/pg_restore/vacuumdb from that same
# directory, out of PATH (see find_pg_commands() in src/bin/pgcopydb/pgcmd.c),
# so PATH is what selects the client version used at run time.
#
# The client must match the *target* server: a pg_restore from PG17 or above
# unconditionally emits "SET transaction_timeout = 0", which a PG16 or older
# server rejects with "unrecognized configuration parameter".  In the other
# direction pg_dump refuses a source server newer than itself.
#
# Two clients cover the whole supported range: 16 handles target servers 14
# through 16, and 18 handles 17 and 18.
ARG PGCLIENTVERSIONS="16 18"

# used to configure Github Packages
LABEL org.opencontainers.image.source=https://github.com/dimitri/pgcopydb

RUN dpkg --add-architecture ${TARGETARCH:-arm64} && apt update \
  && apt install -qqy --no-install-recommends \
	curl \
	ca-certificates \
	gnupg

RUN curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN echo "deb http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main ${PGCLIENTVERSIONS}" > /etc/apt/sources.list.d/pgdg.list

RUN dpkg --add-architecture ${TARGETARCH:-arm64} && apt update \
  && apt install -qqy --no-install-suggests --no-install-recommends \
    sudo \
    passwd \
    ca-certificates \
    libgc1 \
    libpq5 \
    lsof \
    tmux \
    watch \
    psmisc \
    openssl \
    postgresql-common \
    postgresql-client-common \
    $(for v in ${PGCLIENTVERSIONS}; do echo postgresql-client-$v; done) \
    && apt clean \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -rm -d /var/lib/postgres -s /bin/bash -g postgres -G sudo docker
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# Pre-create the work directory so that a fresh Docker named volume mounted at
# /var/run/pgcopydb is pre-populated with the right ownership (docker:postgres)
# rather than starting as root-owned and unwritable by the test user.
RUN mkdir -p /var/run/pgcopydb && chown docker:postgres /var/run/pgcopydb

COPY --from=build --chmod=755 /usr/lib/postgresql/${PGVERSION}/bin/pgcopydb /usr/local/bin
COPY --from=build /usr/local/bin/sqlite3 /usr/local/bin/sqlite3

# Default to the newest installed client tools, and pin the directory
# explicitly rather than relying on Debian's pg_wrapper to pick a version.
ENV PATH=/usr/lib/postgresql/${PGVERSION}/bin:${PATH}

USER docker

ENTRYPOINT []
CMD []
HEALTHCHECK NONE
