# syntax=docker/dockerfile:latest
# Define a base image with all our build dependencies.
FROM --platform=${TARGETPLATFORM} debian:11-slim AS build

# multi-arch
ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH
ARG PGVERSION=16

RUN dpkg --add-architecture ${TARGETARCH:-arm64} && apt update \
  && apt install -qqy --no-install-recommends \
	curl \
	ca-certificates \
	gnupg

RUN curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN echo "deb http://apt.postgresql.org/pub/repos/apt bullseye-pgdg main ${PGVERSION}" > /etc/apt/sources.list.d/pgdg.list

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
FROM --platform=${TARGETPLATFORM} debian:11-slim AS run

# multi-arch
ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH
ARG PGVERSION=16

# used to configure Github Packages
LABEL org.opencontainers.image.source=https://github.com/dimitri/pgcopydb

RUN dpkg --add-architecture ${TARGETARCH:-arm64} && apt update \
  && apt install -qqy --no-install-recommends \
	curl \
	ca-certificates \
	gnupg

RUN curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN echo "deb http://apt.postgresql.org/pub/repos/apt bullseye-pgdg main ${PGVERSION}" > /etc/apt/sources.list.d/pgdg.list

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
    postgresql-client \
    postgresql-client-common \
    && apt clean \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -rm -d /var/lib/postgres -s /bin/bash -g postgres -G sudo docker
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

COPY --from=build --chmod=755 /usr/lib/postgresql/${PGVERSION}/bin/pgcopydb /usr/local/bin
COPY --from=build /usr/local/bin/sqlite3 /usr/local/bin/sqlite3

USER docker

ENTRYPOINT []
CMD []
HEALTHCHECK NONE
