# that image is built by docker compose build: project-service
FROM pagila

COPY --from=pgcopydb /usr/local/bin/pgcopydb /usr/local/bin

WORKDIR /usr/src/pgcopydb
COPY copydb.sh copydb.sh

USER docker
CMD ["/usr/src/pgcopydb/copydb.sh"]
