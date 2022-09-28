FROM pgcopydb

COPY --from=pagila /usr/src/pagila /usr/src/pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh

USER docker
COPY .psqlrc /home/docker/.psqlrc
CMD /usr/src/pgcopydb/copydb.sh
