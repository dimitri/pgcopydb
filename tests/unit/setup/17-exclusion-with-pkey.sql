DROP TABLE IF EXISTS excl_with_pkey;

CREATE TABLE excl_with_pkey (
    id  integer NOT NULL,
    val integer NOT NULL
);

ALTER TABLE excl_with_pkey ADD CONSTRAINT excl_with_pkey_pkey PRIMARY KEY (id);
ALTER TABLE excl_with_pkey ADD CONSTRAINT excl_with_pkey_excl EXCLUDE USING btree (val WITH =);

INSERT INTO excl_with_pkey VALUES (1, 10), (2, 20), (3, 30);
