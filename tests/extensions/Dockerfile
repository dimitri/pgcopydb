FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./ddl.sql ddl.sql
COPY ./countries.sql countries.sql
COPY ./exclude-extension.ini exclude-extension.ini
COPY ./include-only-extension.ini include-only-extension.ini

USER docker
CMD ["/usr/src/pgcopydb/copydb.sh"]
