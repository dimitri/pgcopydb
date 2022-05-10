FROM debian:bullseye-slim as pagila

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    git \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/
RUN git clone --depth 1 https://github.com/devrimgunduz/pagila.git


FROM pgcopydb

COPY --from=pagila /usr/src/pagila /usr/src/pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./include.ini include.ini
COPY ./exclude.ini exclude.ini

# unit tests
COPY ./sql ./test/sql
COPY ./expected ./test/expected

USER docker
WORKDIR /usr/src/pgcopydb/test/
CMD /usr/src/pgcopydb/copydb.sh
