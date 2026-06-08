FROM pagila

# TMPDIR / XDG_DATA_HOME live under /var/run/pgcopydb (see tests/paths.env).
# The pagila base does not make that path writable by the unprivileged "docker"
# user (only the pgcopydb image does).  Previously a shared named volume + the
# fix-volumes `sudo install -o docker` step masked this; now that the follow
# coordinator is reached over TCP we drop the shared volume, so the work dir
# must be writable in the image itself.
USER root
RUN mkdir -p /var/run/pgcopydb && chown -R docker /var/run/pgcopydb

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./dml.sql dml.sql
COPY ./ddl.sql ddl.sql

USER docker
CMD ["/usr/src/pgcopydb/copydb.sh"]
