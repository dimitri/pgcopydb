FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./setup.sql  setup.sql
COPY ./copydb.sh  copydb.sh

USER docker
WORKDIR /usr/src/pgcopydb
CMD ["/usr/src/pgcopydb/copydb.sh"]
