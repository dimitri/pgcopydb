#
# Define a base image with all our build dependencies.
#
FROM debian:bullseye-slim as build

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    build-essential \
    libicu-dev \
    libkrb5-dev \
    libssl-dev \
    libedit-dev \
    libreadline-dev \
    libpam-dev \
    zlib1g-dev \
    liblz4-dev \
	libxml2-dev \
    libxslt1-dev \
    libselinux1-dev \
	libncurses-dev \
    libncurses6 \
    make \
    openssl \
	sudo \
    tmux \
    watch \
    lsof \
    psutils \
	valgrind \
    postgresql-common \
    libpq5 \
    libpq-dev \
    postgresql-server-dev-all \
    postgresql-common \
    postgresql-client-common \
	&& rm -rf /var/lib/apt/lists/*

RUN adduser --disabled-password --gecos '' docker
RUN adduser docker sudo

WORKDIR /usr/src/pgcopydb

COPY Makefile ./
COPY GIT-VERSION-GEN ./
COPY version ./
COPY ./src/ ./src

RUN make -s clean && make -s -j8 install

#
# Now the "run" image, as small as possible
#
FROM debian:bullseye-slim as run

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates \
    openssl \
	sudo \
    tmux \
    watch \
    lsof \
    psutils \
    libpq5 \
    postgresql-client-common \
    postgresql-client-13 \
	&& rm -rf /var/lib/apt/lists/*

RUN adduser --disabled-password --gecos '' --home /var/lib/postgres docker
RUN adduser docker sudo

COPY --from=build /usr/lib/postgresql/13/bin/pgcopydb /usr/local/bin

USER docker
