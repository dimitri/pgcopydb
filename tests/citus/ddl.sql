--
-- Citus multi-tenant tutorial schema.
-- Tables are already in "Citus-native" form: the distribution column
-- (company_id) is present in every distributed table and is the first
-- column of every composite primary / foreign key.
--
-- Source: https://docs.citusdata.com/en/stable/use_cases/multi_tenant.html
--

create table companies (
    id          bigserial primary key,
    name        text not null,
    image_url   text,
    created_at  timestamptz not null default now(),
    updated_at  timestamptz not null default now()
);

create table campaigns (
    company_id  bigint not null references companies (id),
    id          bigserial,
    name        text not null,
    cost_model  text not null,
    state       text not null,
    monthly_budget bigint,
    created_at  timestamptz not null default now(),
    updated_at  timestamptz not null default now(),
    primary key (company_id, id)
);

create table ads (
    company_id        bigint not null,
    id                bigserial,
    campaign_id       bigint not null,
    name              text not null,
    image_url         text,
    target_url        text,
    impressions_count bigint default 0,
    clicks_count      bigint default 0,
    created_at        timestamptz not null default now(),
    updated_at        timestamptz not null default now(),
    primary key (company_id, id),
    foreign key (company_id, campaign_id) references campaigns (company_id, id)
);

create table clicks (
    company_id         bigint not null,
    id                 bigserial,
    ad_id              bigint not null,
    clicked_at         timestamptz not null,
    site_url           text not null,
    cost_per_click_usd numeric(20, 10),
    user_ip            inet not null,
    user_data          jsonb not null,
    primary key (company_id, id),
    foreign key (company_id, ad_id) references ads (company_id, id)
);

create table impressions (
    company_id              bigint not null,
    id                      bigserial,
    ad_id                   bigint not null,
    seen_at                 timestamptz not null,
    site_url                text not null,
    cost_per_impression_usd numeric(20, 10),
    user_ip                 inet not null,
    user_data               jsonb not null,
    primary key (company_id, id),
    foreign key (company_id, ad_id) references ads (company_id, id)
);

-- geo_ips is a reference table: the same rows appear on every node.
create table geo_ips (
    addrs     cidr not null,
    latitude  float not null,
    longitude float not null
);

-- Distribute tables.  The companies table is the co-location anchor; all
-- other per-tenant tables are co-located with it so that cross-table JOINs
-- that filter on company_id stay local to one shard.
select create_distributed_table('companies',  'id');
select create_distributed_table('campaigns',  'company_id', colocate_with := 'companies');
select create_distributed_table('ads',        'company_id', colocate_with := 'companies');
select create_distributed_table('clicks',     'company_id', colocate_with := 'companies');
select create_distributed_table('impressions','company_id', colocate_with := 'companies');

select create_reference_table('geo_ips');
