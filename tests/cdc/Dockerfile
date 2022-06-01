FROM debian:bullseye-slim as pagila

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    git \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/
RUN git clone --depth 1 https://github.com/devrimgunduz/pagila.git


FROM pgcopydb

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends jq \
    && rm -rf /var/lib/apt/lists/*

COPY --from=pagila /usr/src/pagila /usr/src/pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./dml.sql dml.sql
COPY ./000000010000000000000002.json 000000010000000000000002.json

USER docker
CMD /usr/src/pgcopydb/copydb.sh
