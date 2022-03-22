FROM pgcopydb

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./setup ./test/setup

USER docker
CMD /usr/src/pgcopydb/copydb.sh
