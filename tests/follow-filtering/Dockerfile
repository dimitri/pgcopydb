FROM pagila

# TMPDIR / XDG_DATA_HOME live under /var/run/pgcopydb (see tests/paths.env).
USER root
RUN mkdir -p /var/run/pgcopydb && chown -R docker /var/run/pgcopydb

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./dml.sql dml.sql
COPY ./ddl.sql ddl.sql
COPY ./filters.ini filters.ini

USER docker
CMD ["/usr/src/pgcopydb/copydb.sh"]
