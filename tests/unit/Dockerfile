FROM pgcopydb

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./setup ./test/setup
COPY ./sql ./test/sql
COPY ./expected ./test/expected

USER docker
WORKDIR /usr/src/pgcopydb/test/
CMD /usr/src/pgcopydb/copydb.sh
