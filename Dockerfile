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
    libsqlite3-dev \
    libssl-dev \
    libxslt1-dev \
    lsof \
    psmisc \
    gdb \
    strace \
    tmux \
    watch \
    make \
    openssl \
    postgresql-common \
    postgresql-client-common \
    postgresql-server-dev-all \
    psutils \
    tmux \
    watch \
    zlib1g-dev

WORKDIR /usr/src/pgcopydb

COPY . .

RUN make -s clean && make -s -j$(nproc) install

# Now the "run" image, as small as possible
FROM --platform=${TARGETPLATFORM} debian:11-slim as run

# multi-arch
ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH

# used to configure Github Packages
LABEL org.opencontainers.image.source https://github.com/dimitri/pgcopydb

RUN dpkg --add-architecture ${TARGETARCH:-arm64} && apt update \
  && apt install -qqy --no-install-suggests --no-install-recommends \
    sudo \
	passwd \
    ca-certificates \
    libpq5 \
	libsqlite3-0 \
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

COPY --from=build --chmod=755 /usr/lib/postgresql/13/bin/pgcopydb /usr/local/bin

USER docker

ENTRYPOINT []
CMD []
HEALTHCHECK NONE
