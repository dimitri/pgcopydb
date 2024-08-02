# that image is built by docker compose build: project-service
FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./setup ./test/setup
COPY ./sql ./test/sql
COPY ./script ./test/script
COPY ./expected ./test/expected

USER docker
WORKDIR /usr/src/pgcopydb/test/
CMD /usr/src/pgcopydb/copydb.sh
