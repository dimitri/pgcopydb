FROM pagila

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-13-wal2json \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./include.ini include.ini
COPY ./exclude.ini exclude.ini
COPY ./extra.sql extra.sql

# unit tests
COPY ./sql ./test/sql
COPY ./expected ./test/expected
COPY ./follow-mode ./test/follow-mode

USER docker
WORKDIR /usr/src/pgcopydb/test/
CMD /usr/src/pgcopydb/copydb.sh
