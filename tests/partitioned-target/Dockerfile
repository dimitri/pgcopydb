FROM pgcopydb

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./source.sql source.sql
COPY ./target.sql target.sql

USER docker
CMD ["/usr/src/pgcopydb/copydb.sh"]
