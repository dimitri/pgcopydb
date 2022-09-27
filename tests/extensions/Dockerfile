FROM pgcopydb

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends jq \
    && rm -rf /var/lib/apt/lists/*

COPY --from=pagila /usr/src/pagila /usr/src/pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./countries.sql countries.sql

USER docker
COPY ./.psqlrc /users/docker/.psqlrc
CMD /usr/src/pgcopydb/copydb.sh
