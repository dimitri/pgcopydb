FROM pagila

# Make the work dir (TMPDIR/XDG under /var/run/pgcopydb) writable by the
# unprivileged "docker" user: we no longer mount a shared volume there, so the
# pagila base (which doesn't chown it) needs this. See CDC-TCP-SENTINEL-WORKLOG.
USER root
RUN mkdir -p /var/run/pgcopydb && chown -R docker /var/run/pgcopydb

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./dml.sql dml.sql
COPY ./ddl.sql ddl.sql

USER docker
CMD ["/usr/src/pgcopydb/copydb.sh"]
