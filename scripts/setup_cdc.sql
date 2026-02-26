-- Run this against the Aurora DSQL source database before starting the source connector.
-- Requires privileges to create replication publication and alter table replication identity.

CREATE PUBLICATION dbz_aurora_dsql_pub FOR TABLE public.customers, public.orders;

-- Optional but recommended when update/delete events may not include primary-key-only rows.
ALTER TABLE public.customers REPLICA IDENTITY DEFAULT;
ALTER TABLE public.orders REPLICA IDENTITY DEFAULT;
