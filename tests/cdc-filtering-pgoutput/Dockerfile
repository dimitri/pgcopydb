FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./filters.ini filters.ini

USER docker
CMD ["/usr/src/pgcopydb/copydb.sh"]
