FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY pgcopydb.conf /usr/src/pgcopydb/.config/pgcopydb/pgcopydb.conf

USER docker
CMD ["/usr/src/pgcopydb/copydb.sh"]
