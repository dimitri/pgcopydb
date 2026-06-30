\lo_import 'imgs/aj-robbie-BuQ1RZckYW4-unsplash.jpg'
\lo_import 'imgs/bisakha-datta--6SmukZ_w6s-unsplash.jpg'
\lo_import 'imgs/geran-de-klerk-AX9sJ-mPoL4-unsplash.jpg'
\lo_import 'imgs/nam-anh-QJbyG6O0ick-unsplash.jpg'
\lo_import 'imgs/redcharlie-Y--zr3CPaPs-unsplash.jpg'
\lo_import 'imgs/richard-jacobs-8oenpCXktqQ-unsplash.jpg'

-- Hand a single large object to a non-superuser role that is NOT the role
-- pgcopydb connects as.  The other blobs stay owned by the connecting role
-- (postgres), so the source has mixed ownership.  This proves pgcopydb carries
-- pg_largeobject_metadata.lomowner across per-blob: the reassigned object must
-- come back owned by blobowner and the rest must stay owned by postgres.
DO $$
DECLARE
    loid oid;
BEGIN
    SELECT min(oid) INTO loid FROM pg_largeobject_metadata;
    EXECUTE format('ALTER LARGE OBJECT %s OWNER TO blobowner', loid);
END
$$;
