FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./include.ini include.ini
COPY ./exclude.ini exclude.ini
COPY ./extra.sql extra.sql

# unit tests
COPY ./include ./test/include
COPY ./exclude ./test/exclude

USER docker
WORKDIR /usr/src/pgcopydb/test/
CMD ["/usr/src/pgcopydb/copydb.sh"]
