FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh

USER docker
CMD /usr/src/pgcopydb/copydb.sh
