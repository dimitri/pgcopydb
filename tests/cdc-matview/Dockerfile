FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./source.sql source.sql

USER docker
CMD ["/usr/src/pgcopydb/copydb.sh"]
