# syntax=docker/dockerfile:latest
# Define a base image with all our build dependencies.
FROM --platform=${TARGETPLATFORM} debian:11-slim as build

# multi-arch
ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH

RUN dpkg --add-architecture ${TARGETARCH:-arm64} && apt update \
  && apt install -qqy --no-install-recommends \
    libncurses-dev \
    libxml2-dev \
    sudo \
    valgrind \
    build-essential \
    libedit-dev \
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
    lsof \
    make \
    openssl \
    postgresql-client-common \
    postgresql-common \
    postgresql-server-dev-all \
    psutils \
    tmux \
    watch \
    zlib1g-dev

WORKDIR /usr/src/pgcopydb

COPY ./GIT-VERSION-GEN .
COPY ./Makefile .
COPY ./src .

RUN make -s clean && make -s -j$(nproc) install

# Now the "run" image, as small as possible
FROM --platform=${TARGETPLATFORM} debian:11-slim as run

# multi-arch
ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH

RUN dpkg --add-architecture ${TARGETARCH:-arm64} && apt update \
  && apt install -qqy --no-install-suggests --no-install-recommends \
    sudo \
    ca-certificates \
    libpq5 \
    lsof \
    openssl \
    postgresql-client \
    postgresql-client-common \
    psutils \
    && apt clean \
    && rm -rf /var/lib/apt/lists/*

RUN adduser --disabled-password --gecos '' --home /var/lib/postgres docker \
    && adduser docker sudo

COPY --from=build --chmod=755 /usr/lib/postgresql/13/bin/pgcopydb /usr/local/bin

USER docker

ENTRYPOINT []
CMD []
HEALTHCHECK NONE
