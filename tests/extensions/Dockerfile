FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./countries.sql countries.sql

USER docker
CMD /usr/src/pgcopydb/copydb.sh
