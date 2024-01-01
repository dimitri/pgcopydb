FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./dml.sql dml.sql
COPY ./ddl.sql ddl.sql

#RUN install -d -o docker /var/run/pgcopydb

USER docker
CMD /usr/src/pgcopydb/copydb.sh
