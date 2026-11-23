FROM pgcopydb

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./import.sql import.sql
COPY ./killer.sh killer.sh

USER docker
CMD ["/usr/src/pgcopydb/copydb.sh"]
