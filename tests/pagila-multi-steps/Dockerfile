FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY .env /usr/src/pgcopydb/.config/pgcopydb/.env

USER docker
CMD ["/usr/src/pgcopydb/copydb.sh"]
