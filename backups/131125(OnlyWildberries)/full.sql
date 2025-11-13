--
-- PostgreSQL database dump
--

\restrict lTrOKOohzCCgrT7KFFnbhmeB9Bgr2voqKN95473HdPmSTcGZAQUedZgb4ALjRVq

-- Dumped from database version 17.6
-- Dumped by pg_dump version 18.0

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: _realtime; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA _realtime;


ALTER SCHEMA _realtime OWNER TO postgres;

--
-- Name: auth; Type: SCHEMA; Schema: -; Owner: supabase_admin
--

CREATE SCHEMA auth;


ALTER SCHEMA auth OWNER TO supabase_admin;

--
-- Name: extensions; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA extensions;


ALTER SCHEMA extensions OWNER TO postgres;

--
-- Name: graphql; Type: SCHEMA; Schema: -; Owner: supabase_admin
--

CREATE SCHEMA graphql;


ALTER SCHEMA graphql OWNER TO supabase_admin;

--
-- Name: graphql_public; Type: SCHEMA; Schema: -; Owner: supabase_admin
--

CREATE SCHEMA graphql_public;


ALTER SCHEMA graphql_public OWNER TO supabase_admin;

--
-- Name: pg_net; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_net WITH SCHEMA extensions;


--
-- Name: EXTENSION pg_net; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pg_net IS 'Async HTTP';


--
-- Name: pgbouncer; Type: SCHEMA; Schema: -; Owner: pgbouncer
--

CREATE SCHEMA pgbouncer;


ALTER SCHEMA pgbouncer OWNER TO pgbouncer;

--
-- Name: realtime; Type: SCHEMA; Schema: -; Owner: supabase_admin
--

CREATE SCHEMA realtime;


ALTER SCHEMA realtime OWNER TO supabase_admin;

--
-- Name: storage; Type: SCHEMA; Schema: -; Owner: supabase_admin
--

CREATE SCHEMA storage;


ALTER SCHEMA storage OWNER TO supabase_admin;

--
-- Name: supabase_functions; Type: SCHEMA; Schema: -; Owner: supabase_admin
--

CREATE SCHEMA supabase_functions;


ALTER SCHEMA supabase_functions OWNER TO supabase_admin;

--
-- Name: supabase_migrations; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA supabase_migrations;


ALTER SCHEMA supabase_migrations OWNER TO postgres;

--
-- Name: vault; Type: SCHEMA; Schema: -; Owner: supabase_admin
--

CREATE SCHEMA vault;


ALTER SCHEMA vault OWNER TO supabase_admin;

--
-- Name: pg_graphql; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_graphql WITH SCHEMA graphql;


--
-- Name: EXTENSION pg_graphql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pg_graphql IS 'pg_graphql: GraphQL support';


--
-- Name: pg_stat_statements; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA extensions;


--
-- Name: EXTENSION pg_stat_statements; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pg_stat_statements IS 'track planning and execution statistics of all SQL statements executed';


--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA extensions;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


--
-- Name: supabase_vault; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS supabase_vault WITH SCHEMA vault;


--
-- Name: EXTENSION supabase_vault; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION supabase_vault IS 'Supabase Vault Extension';


--
-- Name: uuid-ossp; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA extensions;


--
-- Name: EXTENSION "uuid-ossp"; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION "uuid-ossp" IS 'generate universally unique identifiers (UUIDs)';


--
-- Name: aal_level; Type: TYPE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TYPE auth.aal_level AS ENUM (
    'aal1',
    'aal2',
    'aal3'
);


ALTER TYPE auth.aal_level OWNER TO supabase_auth_admin;

--
-- Name: code_challenge_method; Type: TYPE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TYPE auth.code_challenge_method AS ENUM (
    's256',
    'plain'
);


ALTER TYPE auth.code_challenge_method OWNER TO supabase_auth_admin;

--
-- Name: factor_status; Type: TYPE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TYPE auth.factor_status AS ENUM (
    'unverified',
    'verified'
);


ALTER TYPE auth.factor_status OWNER TO supabase_auth_admin;

--
-- Name: factor_type; Type: TYPE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TYPE auth.factor_type AS ENUM (
    'totp',
    'webauthn',
    'phone'
);


ALTER TYPE auth.factor_type OWNER TO supabase_auth_admin;

--
-- Name: oauth_authorization_status; Type: TYPE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TYPE auth.oauth_authorization_status AS ENUM (
    'pending',
    'approved',
    'denied',
    'expired'
);


ALTER TYPE auth.oauth_authorization_status OWNER TO supabase_auth_admin;

--
-- Name: oauth_client_type; Type: TYPE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TYPE auth.oauth_client_type AS ENUM (
    'public',
    'confidential'
);


ALTER TYPE auth.oauth_client_type OWNER TO supabase_auth_admin;

--
-- Name: oauth_registration_type; Type: TYPE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TYPE auth.oauth_registration_type AS ENUM (
    'dynamic',
    'manual'
);


ALTER TYPE auth.oauth_registration_type OWNER TO supabase_auth_admin;

--
-- Name: oauth_response_type; Type: TYPE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TYPE auth.oauth_response_type AS ENUM (
    'code'
);


ALTER TYPE auth.oauth_response_type OWNER TO supabase_auth_admin;

--
-- Name: one_time_token_type; Type: TYPE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TYPE auth.one_time_token_type AS ENUM (
    'confirmation_token',
    'reauthentication_token',
    'recovery_token',
    'email_change_token_new',
    'email_change_token_current',
    'phone_change_token'
);


ALTER TYPE auth.one_time_token_type OWNER TO supabase_auth_admin;

--
-- Name: action; Type: TYPE; Schema: realtime; Owner: supabase_admin
--

CREATE TYPE realtime.action AS ENUM (
    'INSERT',
    'UPDATE',
    'DELETE',
    'TRUNCATE',
    'ERROR'
);


ALTER TYPE realtime.action OWNER TO supabase_admin;

--
-- Name: equality_op; Type: TYPE; Schema: realtime; Owner: supabase_admin
--

CREATE TYPE realtime.equality_op AS ENUM (
    'eq',
    'neq',
    'lt',
    'lte',
    'gt',
    'gte',
    'in'
);


ALTER TYPE realtime.equality_op OWNER TO supabase_admin;

--
-- Name: user_defined_filter; Type: TYPE; Schema: realtime; Owner: supabase_admin
--

CREATE TYPE realtime.user_defined_filter AS (
	column_name text,
	op realtime.equality_op,
	value text
);


ALTER TYPE realtime.user_defined_filter OWNER TO supabase_admin;

--
-- Name: wal_column; Type: TYPE; Schema: realtime; Owner: supabase_admin
--

CREATE TYPE realtime.wal_column AS (
	name text,
	type_name text,
	type_oid oid,
	value jsonb,
	is_pkey boolean,
	is_selectable boolean
);


ALTER TYPE realtime.wal_column OWNER TO supabase_admin;

--
-- Name: wal_rls; Type: TYPE; Schema: realtime; Owner: supabase_admin
--

CREATE TYPE realtime.wal_rls AS (
	wal jsonb,
	is_rls_enabled boolean,
	subscription_ids uuid[],
	errors text[]
);


ALTER TYPE realtime.wal_rls OWNER TO supabase_admin;

--
-- Name: buckettype; Type: TYPE; Schema: storage; Owner: supabase_storage_admin
--

CREATE TYPE storage.buckettype AS ENUM (
    'STANDARD',
    'ANALYTICS'
);


ALTER TYPE storage.buckettype OWNER TO supabase_storage_admin;

--
-- Name: email(); Type: FUNCTION; Schema: auth; Owner: supabase_auth_admin
--

CREATE FUNCTION auth.email() RETURNS text
    LANGUAGE sql STABLE
    AS $$
  select 
  coalesce(
    nullif(current_setting('request.jwt.claim.email', true), ''),
    (nullif(current_setting('request.jwt.claims', true), '')::jsonb ->> 'email')
  )::text
$$;


ALTER FUNCTION auth.email() OWNER TO supabase_auth_admin;

--
-- Name: FUNCTION email(); Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON FUNCTION auth.email() IS 'Deprecated. Use auth.jwt() -> ''email'' instead.';


--
-- Name: jwt(); Type: FUNCTION; Schema: auth; Owner: supabase_auth_admin
--

CREATE FUNCTION auth.jwt() RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  select 
    coalesce(
        nullif(current_setting('request.jwt.claim', true), ''),
        nullif(current_setting('request.jwt.claims', true), '')
    )::jsonb
$$;


ALTER FUNCTION auth.jwt() OWNER TO supabase_auth_admin;

--
-- Name: role(); Type: FUNCTION; Schema: auth; Owner: supabase_auth_admin
--

CREATE FUNCTION auth.role() RETURNS text
    LANGUAGE sql STABLE
    AS $$
  select 
  coalesce(
    nullif(current_setting('request.jwt.claim.role', true), ''),
    (nullif(current_setting('request.jwt.claims', true), '')::jsonb ->> 'role')
  )::text
$$;


ALTER FUNCTION auth.role() OWNER TO supabase_auth_admin;

--
-- Name: FUNCTION role(); Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON FUNCTION auth.role() IS 'Deprecated. Use auth.jwt() -> ''role'' instead.';


--
-- Name: uid(); Type: FUNCTION; Schema: auth; Owner: supabase_auth_admin
--

CREATE FUNCTION auth.uid() RETURNS uuid
    LANGUAGE sql STABLE
    AS $$
  select 
  coalesce(
    nullif(current_setting('request.jwt.claim.sub', true), ''),
    (nullif(current_setting('request.jwt.claims', true), '')::jsonb ->> 'sub')
  )::uuid
$$;


ALTER FUNCTION auth.uid() OWNER TO supabase_auth_admin;

--
-- Name: FUNCTION uid(); Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON FUNCTION auth.uid() IS 'Deprecated. Use auth.jwt() -> ''sub'' instead.';


--
-- Name: grant_pg_cron_access(); Type: FUNCTION; Schema: extensions; Owner: supabase_admin
--

CREATE FUNCTION extensions.grant_pg_cron_access() RETURNS event_trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF EXISTS (
    SELECT
    FROM pg_event_trigger_ddl_commands() AS ev
    JOIN pg_extension AS ext
    ON ev.objid = ext.oid
    WHERE ext.extname = 'pg_cron'
  )
  THEN
    grant usage on schema cron to postgres with grant option;

    alter default privileges in schema cron grant all on tables to postgres with grant option;
    alter default privileges in schema cron grant all on functions to postgres with grant option;
    alter default privileges in schema cron grant all on sequences to postgres with grant option;

    alter default privileges for user supabase_admin in schema cron grant all
        on sequences to postgres with grant option;
    alter default privileges for user supabase_admin in schema cron grant all
        on tables to postgres with grant option;
    alter default privileges for user supabase_admin in schema cron grant all
        on functions to postgres with grant option;

    grant all privileges on all tables in schema cron to postgres with grant option;
    revoke all on table cron.job from postgres;
    grant select on table cron.job to postgres with grant option;
  END IF;
END;
$$;


ALTER FUNCTION extensions.grant_pg_cron_access() OWNER TO supabase_admin;

--
-- Name: FUNCTION grant_pg_cron_access(); Type: COMMENT; Schema: extensions; Owner: supabase_admin
--

COMMENT ON FUNCTION extensions.grant_pg_cron_access() IS 'Grants access to pg_cron';


--
-- Name: grant_pg_graphql_access(); Type: FUNCTION; Schema: extensions; Owner: supabase_admin
--

CREATE FUNCTION extensions.grant_pg_graphql_access() RETURNS event_trigger
    LANGUAGE plpgsql
    AS $_$
DECLARE
    func_is_graphql_resolve bool;
BEGIN
    func_is_graphql_resolve = (
        SELECT n.proname = 'resolve'
        FROM pg_event_trigger_ddl_commands() AS ev
        LEFT JOIN pg_catalog.pg_proc AS n
        ON ev.objid = n.oid
    );

    IF func_is_graphql_resolve
    THEN
        -- Update public wrapper to pass all arguments through to the pg_graphql resolve func
        DROP FUNCTION IF EXISTS graphql_public.graphql;
        create or replace function graphql_public.graphql(
            "operationName" text default null,
            query text default null,
            variables jsonb default null,
            extensions jsonb default null
        )
            returns jsonb
            language sql
        as $$
            select graphql.resolve(
                query := query,
                variables := coalesce(variables, '{}'),
                "operationName" := "operationName",
                extensions := extensions
            );
        $$;

        -- This hook executes when `graphql.resolve` is created. That is not necessarily the last
        -- function in the extension so we need to grant permissions on existing entities AND
        -- update default permissions to any others that are created after `graphql.resolve`
        grant usage on schema graphql to postgres, anon, authenticated, service_role;
        grant select on all tables in schema graphql to postgres, anon, authenticated, service_role;
        grant execute on all functions in schema graphql to postgres, anon, authenticated, service_role;
        grant all on all sequences in schema graphql to postgres, anon, authenticated, service_role;
        alter default privileges in schema graphql grant all on tables to postgres, anon, authenticated, service_role;
        alter default privileges in schema graphql grant all on functions to postgres, anon, authenticated, service_role;
        alter default privileges in schema graphql grant all on sequences to postgres, anon, authenticated, service_role;

        -- Allow postgres role to allow granting usage on graphql and graphql_public schemas to custom roles
        grant usage on schema graphql_public to postgres with grant option;
        grant usage on schema graphql to postgres with grant option;
    END IF;

END;
$_$;


ALTER FUNCTION extensions.grant_pg_graphql_access() OWNER TO supabase_admin;

--
-- Name: FUNCTION grant_pg_graphql_access(); Type: COMMENT; Schema: extensions; Owner: supabase_admin
--

COMMENT ON FUNCTION extensions.grant_pg_graphql_access() IS 'Grants access to pg_graphql';


--
-- Name: grant_pg_net_access(); Type: FUNCTION; Schema: extensions; Owner: supabase_admin
--

CREATE FUNCTION extensions.grant_pg_net_access() RETURNS event_trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_event_trigger_ddl_commands() AS ev
    JOIN pg_extension AS ext
    ON ev.objid = ext.oid
    WHERE ext.extname = 'pg_net'
  )
  THEN
    GRANT USAGE ON SCHEMA net TO supabase_functions_admin, postgres, anon, authenticated, service_role;

    ALTER function net.http_get(url text, params jsonb, headers jsonb, timeout_milliseconds integer) SECURITY DEFINER;
    ALTER function net.http_post(url text, body jsonb, params jsonb, headers jsonb, timeout_milliseconds integer) SECURITY DEFINER;

    ALTER function net.http_get(url text, params jsonb, headers jsonb, timeout_milliseconds integer) SET search_path = net;
    ALTER function net.http_post(url text, body jsonb, params jsonb, headers jsonb, timeout_milliseconds integer) SET search_path = net;

    REVOKE ALL ON FUNCTION net.http_get(url text, params jsonb, headers jsonb, timeout_milliseconds integer) FROM PUBLIC;
    REVOKE ALL ON FUNCTION net.http_post(url text, body jsonb, params jsonb, headers jsonb, timeout_milliseconds integer) FROM PUBLIC;

    GRANT EXECUTE ON FUNCTION net.http_get(url text, params jsonb, headers jsonb, timeout_milliseconds integer) TO supabase_functions_admin, postgres, anon, authenticated, service_role;
    GRANT EXECUTE ON FUNCTION net.http_post(url text, body jsonb, params jsonb, headers jsonb, timeout_milliseconds integer) TO supabase_functions_admin, postgres, anon, authenticated, service_role;
  END IF;
END;
$$;


ALTER FUNCTION extensions.grant_pg_net_access() OWNER TO supabase_admin;

--
-- Name: FUNCTION grant_pg_net_access(); Type: COMMENT; Schema: extensions; Owner: supabase_admin
--

COMMENT ON FUNCTION extensions.grant_pg_net_access() IS 'Grants access to pg_net';


--
-- Name: pgrst_ddl_watch(); Type: FUNCTION; Schema: extensions; Owner: supabase_admin
--

CREATE FUNCTION extensions.pgrst_ddl_watch() RETURNS event_trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  cmd record;
BEGIN
  FOR cmd IN SELECT * FROM pg_event_trigger_ddl_commands()
  LOOP
    IF cmd.command_tag IN (
      'CREATE SCHEMA', 'ALTER SCHEMA'
    , 'CREATE TABLE', 'CREATE TABLE AS', 'SELECT INTO', 'ALTER TABLE'
    , 'CREATE FOREIGN TABLE', 'ALTER FOREIGN TABLE'
    , 'CREATE VIEW', 'ALTER VIEW'
    , 'CREATE MATERIALIZED VIEW', 'ALTER MATERIALIZED VIEW'
    , 'CREATE FUNCTION', 'ALTER FUNCTION'
    , 'CREATE TRIGGER'
    , 'CREATE TYPE', 'ALTER TYPE'
    , 'CREATE RULE'
    , 'COMMENT'
    )
    -- don't notify in case of CREATE TEMP table or other objects created on pg_temp
    AND cmd.schema_name is distinct from 'pg_temp'
    THEN
      NOTIFY pgrst, 'reload schema';
    END IF;
  END LOOP;
END; $$;


ALTER FUNCTION extensions.pgrst_ddl_watch() OWNER TO supabase_admin;

--
-- Name: pgrst_drop_watch(); Type: FUNCTION; Schema: extensions; Owner: supabase_admin
--

CREATE FUNCTION extensions.pgrst_drop_watch() RETURNS event_trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  obj record;
BEGIN
  FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
  LOOP
    IF obj.object_type IN (
      'schema'
    , 'table'
    , 'foreign table'
    , 'view'
    , 'materialized view'
    , 'function'
    , 'trigger'
    , 'type'
    , 'rule'
    )
    AND obj.is_temporary IS false -- no pg_temp objects
    THEN
      NOTIFY pgrst, 'reload schema';
    END IF;
  END LOOP;
END; $$;


ALTER FUNCTION extensions.pgrst_drop_watch() OWNER TO supabase_admin;

--
-- Name: set_graphql_placeholder(); Type: FUNCTION; Schema: extensions; Owner: supabase_admin
--

CREATE FUNCTION extensions.set_graphql_placeholder() RETURNS event_trigger
    LANGUAGE plpgsql
    AS $_$
    DECLARE
    graphql_is_dropped bool;
    BEGIN
    graphql_is_dropped = (
        SELECT ev.schema_name = 'graphql_public'
        FROM pg_event_trigger_dropped_objects() AS ev
        WHERE ev.schema_name = 'graphql_public'
    );

    IF graphql_is_dropped
    THEN
        create or replace function graphql_public.graphql(
            "operationName" text default null,
            query text default null,
            variables jsonb default null,
            extensions jsonb default null
        )
            returns jsonb
            language plpgsql
        as $$
            DECLARE
                server_version float;
            BEGIN
                server_version = (SELECT (SPLIT_PART((select version()), ' ', 2))::float);

                IF server_version >= 14 THEN
                    RETURN jsonb_build_object(
                        'errors', jsonb_build_array(
                            jsonb_build_object(
                                'message', 'pg_graphql extension is not enabled.'
                            )
                        )
                    );
                ELSE
                    RETURN jsonb_build_object(
                        'errors', jsonb_build_array(
                            jsonb_build_object(
                                'message', 'pg_graphql is only available on projects running Postgres 14 onwards.'
                            )
                        )
                    );
                END IF;
            END;
        $$;
    END IF;

    END;
$_$;


ALTER FUNCTION extensions.set_graphql_placeholder() OWNER TO supabase_admin;

--
-- Name: FUNCTION set_graphql_placeholder(); Type: COMMENT; Schema: extensions; Owner: supabase_admin
--

COMMENT ON FUNCTION extensions.set_graphql_placeholder() IS 'Reintroduces placeholder function for graphql_public.graphql';


--
-- Name: get_auth(text); Type: FUNCTION; Schema: pgbouncer; Owner: supabase_admin
--

CREATE FUNCTION pgbouncer.get_auth(p_usename text) RETURNS TABLE(username text, password text)
    LANGUAGE plpgsql SECURITY DEFINER
    AS $_$
begin
    raise debug 'PgBouncer auth request: %', p_usename;

    return query
    select 
        rolname::text, 
        case when rolvaliduntil < now() 
            then null 
            else rolpassword::text 
        end 
    from pg_authid 
    where rolname=$1 and rolcanlogin;
end;
$_$;


ALTER FUNCTION pgbouncer.get_auth(p_usename text) OWNER TO supabase_admin;

--
-- Name: aggregate_adv_params(date, date); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.aggregate_adv_params(p_date_from date DEFAULT NULL::date, p_date_to date DEFAULT NULL::date) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
  inserted_count INTEGER;
BEGIN
  INSERT INTO adv_params (
    nm_id,
    vendor_code,
    date,
    views,
    clicks,
    sum,
    cpc,
    cpm,
    ctr,
    orders,
    orders_sum
  )
  SELECT
    s.nm_id,
    s.vendor_code,
    s.date,
    SUM(s.views)::INTEGER AS views,
    SUM(s.clicks)::INTEGER AS clicks,
    SUM(s.sum) AS sum,
    -- CPC: средняя стоимость клика
    CASE
      WHEN SUM(s.clicks) > 0 THEN ROUND(SUM(s.sum) / SUM(s.clicks), 2)
      ELSE NULL
    END AS cpc,
    -- CPM: стоимость 1000 показов
    CASE
      WHEN SUM(s.views) > 0 THEN ROUND((SUM(s.sum) / SUM(s.views)) * 1000, 2)
      ELSE NULL
    END AS cpm,
    -- CTR: процент кликов от показов
    CASE
      WHEN SUM(s.views) > 0 THEN ROUND((SUM(s.clicks)::NUMERIC / SUM(s.views)) * 100, 2)
      ELSE NULL
    END AS ctr,
    SUM(s.orders)::INTEGER AS orders,
    SUM(s.orders_sum) AS orders_sum
  FROM
    adv_campaign_daily_stats s
  WHERE
    (p_date_from IS NULL OR s.date >= p_date_from)
    AND (p_date_to IS NULL OR s.date <= p_date_to)
  GROUP BY
    s.nm_id,
    s.vendor_code,
    s.date
  ON CONFLICT (nm_id, date) DO UPDATE SET
    vendor_code = EXCLUDED.vendor_code,
    views = EXCLUDED.views,
    clicks = EXCLUDED.clicks,
    sum = EXCLUDED.sum,
    cpc = EXCLUDED.cpc,
    cpm = EXCLUDED.cpm,
    ctr = EXCLUDED.ctr,
    orders = EXCLUDED.orders,
    orders_sum = EXCLUDED.orders_sum,
    -- Обновляем updated_at ТОЛЬКО если данные реально изменились
    updated_at = CASE 
      WHEN (
        adv_params.views IS DISTINCT FROM EXCLUDED.views OR
        adv_params.clicks IS DISTINCT FROM EXCLUDED.clicks OR
        adv_params.sum IS DISTINCT FROM EXCLUDED.sum OR
        adv_params.orders IS DISTINCT FROM EXCLUDED.orders OR
        adv_params.orders_sum IS DISTINCT FROM EXCLUDED.orders_sum
      )
      THEN NOW()
      ELSE adv_params.updated_at
    END;
  
  -- Возвращаем количество обработанных записей
  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$$;


ALTER FUNCTION public.aggregate_adv_params(p_date_from date, p_date_to date) OWNER TO postgres;

--
-- Name: FUNCTION aggregate_adv_params(p_date_from date, p_date_to date); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.aggregate_adv_params(p_date_from date, p_date_to date) IS 'Агрегирует данные из adv_campaign_daily_stats в adv_params, группируя по nm_id и date';


--
-- Name: update_updated_at_column(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.update_updated_at_column() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;


ALTER FUNCTION public.update_updated_at_column() OWNER TO postgres;

--
-- Name: FUNCTION update_updated_at_column(); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.update_updated_at_column() IS 'Автоматически обновляет поле updated_at при UPDATE';


--
-- Name: apply_rls(jsonb, integer); Type: FUNCTION; Schema: realtime; Owner: supabase_admin
--

CREATE FUNCTION realtime.apply_rls(wal jsonb, max_record_bytes integer DEFAULT (1024 * 1024)) RETURNS SETOF realtime.wal_rls
    LANGUAGE plpgsql
    AS $$
declare
-- Regclass of the table e.g. public.notes
entity_ regclass = (quote_ident(wal ->> 'schema') || '.' || quote_ident(wal ->> 'table'))::regclass;

-- I, U, D, T: insert, update ...
action realtime.action = (
    case wal ->> 'action'
        when 'I' then 'INSERT'
        when 'U' then 'UPDATE'
        when 'D' then 'DELETE'
        else 'ERROR'
    end
);

-- Is row level security enabled for the table
is_rls_enabled bool = relrowsecurity from pg_class where oid = entity_;

subscriptions realtime.subscription[] = array_agg(subs)
    from
        realtime.subscription subs
    where
        subs.entity = entity_;

-- Subscription vars
roles regrole[] = array_agg(distinct us.claims_role::text)
    from
        unnest(subscriptions) us;

working_role regrole;
claimed_role regrole;
claims jsonb;

subscription_id uuid;
subscription_has_access bool;
visible_to_subscription_ids uuid[] = '{}';

-- structured info for wal's columns
columns realtime.wal_column[];
-- previous identity values for update/delete
old_columns realtime.wal_column[];

error_record_exceeds_max_size boolean = octet_length(wal::text) > max_record_bytes;

-- Primary jsonb output for record
output jsonb;

begin
perform set_config('role', null, true);

columns =
    array_agg(
        (
            x->>'name',
            x->>'type',
            x->>'typeoid',
            realtime.cast(
                (x->'value') #>> '{}',
                coalesce(
                    (x->>'typeoid')::regtype, -- null when wal2json version <= 2.4
                    (x->>'type')::regtype
                )
            ),
            (pks ->> 'name') is not null,
            true
        )::realtime.wal_column
    )
    from
        jsonb_array_elements(wal -> 'columns') x
        left join jsonb_array_elements(wal -> 'pk') pks
            on (x ->> 'name') = (pks ->> 'name');

old_columns =
    array_agg(
        (
            x->>'name',
            x->>'type',
            x->>'typeoid',
            realtime.cast(
                (x->'value') #>> '{}',
                coalesce(
                    (x->>'typeoid')::regtype, -- null when wal2json version <= 2.4
                    (x->>'type')::regtype
                )
            ),
            (pks ->> 'name') is not null,
            true
        )::realtime.wal_column
    )
    from
        jsonb_array_elements(wal -> 'identity') x
        left join jsonb_array_elements(wal -> 'pk') pks
            on (x ->> 'name') = (pks ->> 'name');

for working_role in select * from unnest(roles) loop

    -- Update `is_selectable` for columns and old_columns
    columns =
        array_agg(
            (
                c.name,
                c.type_name,
                c.type_oid,
                c.value,
                c.is_pkey,
                pg_catalog.has_column_privilege(working_role, entity_, c.name, 'SELECT')
            )::realtime.wal_column
        )
        from
            unnest(columns) c;

    old_columns =
            array_agg(
                (
                    c.name,
                    c.type_name,
                    c.type_oid,
                    c.value,
                    c.is_pkey,
                    pg_catalog.has_column_privilege(working_role, entity_, c.name, 'SELECT')
                )::realtime.wal_column
            )
            from
                unnest(old_columns) c;

    if action <> 'DELETE' and count(1) = 0 from unnest(columns) c where c.is_pkey then
        return next (
            jsonb_build_object(
                'schema', wal ->> 'schema',
                'table', wal ->> 'table',
                'type', action
            ),
            is_rls_enabled,
            -- subscriptions is already filtered by entity
            (select array_agg(s.subscription_id) from unnest(subscriptions) as s where claims_role = working_role),
            array['Error 400: Bad Request, no primary key']
        )::realtime.wal_rls;

    -- The claims role does not have SELECT permission to the primary key of entity
    elsif action <> 'DELETE' and sum(c.is_selectable::int) <> count(1) from unnest(columns) c where c.is_pkey then
        return next (
            jsonb_build_object(
                'schema', wal ->> 'schema',
                'table', wal ->> 'table',
                'type', action
            ),
            is_rls_enabled,
            (select array_agg(s.subscription_id) from unnest(subscriptions) as s where claims_role = working_role),
            array['Error 401: Unauthorized']
        )::realtime.wal_rls;

    else
        output = jsonb_build_object(
            'schema', wal ->> 'schema',
            'table', wal ->> 'table',
            'type', action,
            'commit_timestamp', to_char(
                ((wal ->> 'timestamp')::timestamptz at time zone 'utc'),
                'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
            ),
            'columns', (
                select
                    jsonb_agg(
                        jsonb_build_object(
                            'name', pa.attname,
                            'type', pt.typname
                        )
                        order by pa.attnum asc
                    )
                from
                    pg_attribute pa
                    join pg_type pt
                        on pa.atttypid = pt.oid
                where
                    attrelid = entity_
                    and attnum > 0
                    and pg_catalog.has_column_privilege(working_role, entity_, pa.attname, 'SELECT')
            )
        )
        -- Add "record" key for insert and update
        || case
            when action in ('INSERT', 'UPDATE') then
                jsonb_build_object(
                    'record',
                    (
                        select
                            jsonb_object_agg(
                                -- if unchanged toast, get column name and value from old record
                                coalesce((c).name, (oc).name),
                                case
                                    when (c).name is null then (oc).value
                                    else (c).value
                                end
                            )
                        from
                            unnest(columns) c
                            full outer join unnest(old_columns) oc
                                on (c).name = (oc).name
                        where
                            coalesce((c).is_selectable, (oc).is_selectable)
                            and ( not error_record_exceeds_max_size or (octet_length((c).value::text) <= 64))
                    )
                )
            else '{}'::jsonb
        end
        -- Add "old_record" key for update and delete
        || case
            when action = 'UPDATE' then
                jsonb_build_object(
                        'old_record',
                        (
                            select jsonb_object_agg((c).name, (c).value)
                            from unnest(old_columns) c
                            where
                                (c).is_selectable
                                and ( not error_record_exceeds_max_size or (octet_length((c).value::text) <= 64))
                        )
                    )
            when action = 'DELETE' then
                jsonb_build_object(
                    'old_record',
                    (
                        select jsonb_object_agg((c).name, (c).value)
                        from unnest(old_columns) c
                        where
                            (c).is_selectable
                            and ( not error_record_exceeds_max_size or (octet_length((c).value::text) <= 64))
                            and ( not is_rls_enabled or (c).is_pkey ) -- if RLS enabled, we can't secure deletes so filter to pkey
                    )
                )
            else '{}'::jsonb
        end;

        -- Create the prepared statement
        if is_rls_enabled and action <> 'DELETE' then
            if (select 1 from pg_prepared_statements where name = 'walrus_rls_stmt' limit 1) > 0 then
                deallocate walrus_rls_stmt;
            end if;
            execute realtime.build_prepared_statement_sql('walrus_rls_stmt', entity_, columns);
        end if;

        visible_to_subscription_ids = '{}';

        for subscription_id, claims in (
                select
                    subs.subscription_id,
                    subs.claims
                from
                    unnest(subscriptions) subs
                where
                    subs.entity = entity_
                    and subs.claims_role = working_role
                    and (
                        realtime.is_visible_through_filters(columns, subs.filters)
                        or (
                          action = 'DELETE'
                          and realtime.is_visible_through_filters(old_columns, subs.filters)
                        )
                    )
        ) loop

            if not is_rls_enabled or action = 'DELETE' then
                visible_to_subscription_ids = visible_to_subscription_ids || subscription_id;
            else
                -- Check if RLS allows the role to see the record
                perform
                    -- Trim leading and trailing quotes from working_role because set_config
                    -- doesn't recognize the role as valid if they are included
                    set_config('role', trim(both '"' from working_role::text), true),
                    set_config('request.jwt.claims', claims::text, true);

                execute 'execute walrus_rls_stmt' into subscription_has_access;

                if subscription_has_access then
                    visible_to_subscription_ids = visible_to_subscription_ids || subscription_id;
                end if;
            end if;
        end loop;

        perform set_config('role', null, true);

        return next (
            output,
            is_rls_enabled,
            visible_to_subscription_ids,
            case
                when error_record_exceeds_max_size then array['Error 413: Payload Too Large']
                else '{}'
            end
        )::realtime.wal_rls;

    end if;
end loop;

perform set_config('role', null, true);
end;
$$;


ALTER FUNCTION realtime.apply_rls(wal jsonb, max_record_bytes integer) OWNER TO supabase_admin;

--
-- Name: broadcast_changes(text, text, text, text, text, record, record, text); Type: FUNCTION; Schema: realtime; Owner: supabase_admin
--

CREATE FUNCTION realtime.broadcast_changes(topic_name text, event_name text, operation text, table_name text, table_schema text, new record, old record, level text DEFAULT 'ROW'::text) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    -- Declare a variable to hold the JSONB representation of the row
    row_data jsonb := '{}'::jsonb;
BEGIN
    IF level = 'STATEMENT' THEN
        RAISE EXCEPTION 'function can only be triggered for each row, not for each statement';
    END IF;
    -- Check the operation type and handle accordingly
    IF operation = 'INSERT' OR operation = 'UPDATE' OR operation = 'DELETE' THEN
        row_data := jsonb_build_object('old_record', OLD, 'record', NEW, 'operation', operation, 'table', table_name, 'schema', table_schema);
        PERFORM realtime.send (row_data, event_name, topic_name);
    ELSE
        RAISE EXCEPTION 'Unexpected operation type: %', operation;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Failed to process the row: %', SQLERRM;
END;

$$;


ALTER FUNCTION realtime.broadcast_changes(topic_name text, event_name text, operation text, table_name text, table_schema text, new record, old record, level text) OWNER TO supabase_admin;

--
-- Name: build_prepared_statement_sql(text, regclass, realtime.wal_column[]); Type: FUNCTION; Schema: realtime; Owner: supabase_admin
--

CREATE FUNCTION realtime.build_prepared_statement_sql(prepared_statement_name text, entity regclass, columns realtime.wal_column[]) RETURNS text
    LANGUAGE sql
    AS $$
      /*
      Builds a sql string that, if executed, creates a prepared statement to
      tests retrive a row from *entity* by its primary key columns.
      Example
          select realtime.build_prepared_statement_sql('public.notes', '{"id"}'::text[], '{"bigint"}'::text[])
      */
          select
      'prepare ' || prepared_statement_name || ' as
          select
              exists(
                  select
                      1
                  from
                      ' || entity || '
                  where
                      ' || string_agg(quote_ident(pkc.name) || '=' || quote_nullable(pkc.value #>> '{}') , ' and ') || '
              )'
          from
              unnest(columns) pkc
          where
              pkc.is_pkey
          group by
              entity
      $$;


ALTER FUNCTION realtime.build_prepared_statement_sql(prepared_statement_name text, entity regclass, columns realtime.wal_column[]) OWNER TO supabase_admin;

--
-- Name: cast(text, regtype); Type: FUNCTION; Schema: realtime; Owner: supabase_admin
--

CREATE FUNCTION realtime."cast"(val text, type_ regtype) RETURNS jsonb
    LANGUAGE plpgsql IMMUTABLE
    AS $$
    declare
      res jsonb;
    begin
      execute format('select to_jsonb(%L::'|| type_::text || ')', val)  into res;
      return res;
    end
    $$;


ALTER FUNCTION realtime."cast"(val text, type_ regtype) OWNER TO supabase_admin;

--
-- Name: check_equality_op(realtime.equality_op, regtype, text, text); Type: FUNCTION; Schema: realtime; Owner: supabase_admin
--

CREATE FUNCTION realtime.check_equality_op(op realtime.equality_op, type_ regtype, val_1 text, val_2 text) RETURNS boolean
    LANGUAGE plpgsql IMMUTABLE
    AS $$
      /*
      Casts *val_1* and *val_2* as type *type_* and check the *op* condition for truthiness
      */
      declare
          op_symbol text = (
              case
                  when op = 'eq' then '='
                  when op = 'neq' then '!='
                  when op = 'lt' then '<'
                  when op = 'lte' then '<='
                  when op = 'gt' then '>'
                  when op = 'gte' then '>='
                  when op = 'in' then '= any'
                  else 'UNKNOWN OP'
              end
          );
          res boolean;
      begin
          execute format(
              'select %L::'|| type_::text || ' ' || op_symbol
              || ' ( %L::'
              || (
                  case
                      when op = 'in' then type_::text || '[]'
                      else type_::text end
              )
              || ')', val_1, val_2) into res;
          return res;
      end;
      $$;


ALTER FUNCTION realtime.check_equality_op(op realtime.equality_op, type_ regtype, val_1 text, val_2 text) OWNER TO supabase_admin;

--
-- Name: is_visible_through_filters(realtime.wal_column[], realtime.user_defined_filter[]); Type: FUNCTION; Schema: realtime; Owner: supabase_admin
--

CREATE FUNCTION realtime.is_visible_through_filters(columns realtime.wal_column[], filters realtime.user_defined_filter[]) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$
    /*
    Should the record be visible (true) or filtered out (false) after *filters* are applied
    */
        select
            -- Default to allowed when no filters present
            $2 is null -- no filters. this should not happen because subscriptions has a default
            or array_length($2, 1) is null -- array length of an empty array is null
            or bool_and(
                coalesce(
                    realtime.check_equality_op(
                        op:=f.op,
                        type_:=coalesce(
                            col.type_oid::regtype, -- null when wal2json version <= 2.4
                            col.type_name::regtype
                        ),
                        -- cast jsonb to text
                        val_1:=col.value #>> '{}',
                        val_2:=f.value
                    ),
                    false -- if null, filter does not match
                )
            )
        from
            unnest(filters) f
            join unnest(columns) col
                on f.column_name = col.name;
    $_$;


ALTER FUNCTION realtime.is_visible_through_filters(columns realtime.wal_column[], filters realtime.user_defined_filter[]) OWNER TO supabase_admin;

--
-- Name: list_changes(name, name, integer, integer); Type: FUNCTION; Schema: realtime; Owner: supabase_admin
--

CREATE FUNCTION realtime.list_changes(publication name, slot_name name, max_changes integer, max_record_bytes integer) RETURNS SETOF realtime.wal_rls
    LANGUAGE sql
    SET log_min_messages TO 'fatal'
    AS $$
      with pub as (
        select
          concat_ws(
            ',',
            case when bool_or(pubinsert) then 'insert' else null end,
            case when bool_or(pubupdate) then 'update' else null end,
            case when bool_or(pubdelete) then 'delete' else null end
          ) as w2j_actions,
          coalesce(
            string_agg(
              realtime.quote_wal2json(format('%I.%I', schemaname, tablename)::regclass),
              ','
            ) filter (where ppt.tablename is not null and ppt.tablename not like '% %'),
            ''
          ) w2j_add_tables
        from
          pg_publication pp
          left join pg_publication_tables ppt
            on pp.pubname = ppt.pubname
        where
          pp.pubname = publication
        group by
          pp.pubname
        limit 1
      ),
      w2j as (
        select
          x.*, pub.w2j_add_tables
        from
          pub,
          pg_logical_slot_get_changes(
            slot_name, null, max_changes,
            'include-pk', 'true',
            'include-transaction', 'false',
            'include-timestamp', 'true',
            'include-type-oids', 'true',
            'format-version', '2',
            'actions', pub.w2j_actions,
            'add-tables', pub.w2j_add_tables
          ) x
      )
      select
        xyz.wal,
        xyz.is_rls_enabled,
        xyz.subscription_ids,
        xyz.errors
      from
        w2j,
        realtime.apply_rls(
          wal := w2j.data::jsonb,
          max_record_bytes := max_record_bytes
        ) xyz(wal, is_rls_enabled, subscription_ids, errors)
      where
        w2j.w2j_add_tables <> ''
        and xyz.subscription_ids[1] is not null
    $$;


ALTER FUNCTION realtime.list_changes(publication name, slot_name name, max_changes integer, max_record_bytes integer) OWNER TO supabase_admin;

--
-- Name: quote_wal2json(regclass); Type: FUNCTION; Schema: realtime; Owner: supabase_admin
--

CREATE FUNCTION realtime.quote_wal2json(entity regclass) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $$
      select
        (
          select string_agg('' || ch,'')
          from unnest(string_to_array(nsp.nspname::text, null)) with ordinality x(ch, idx)
          where
            not (x.idx = 1 and x.ch = '"')
            and not (
              x.idx = array_length(string_to_array(nsp.nspname::text, null), 1)
              and x.ch = '"'
            )
        )
        || '.'
        || (
          select string_agg('' || ch,'')
          from unnest(string_to_array(pc.relname::text, null)) with ordinality x(ch, idx)
          where
            not (x.idx = 1 and x.ch = '"')
            and not (
              x.idx = array_length(string_to_array(nsp.nspname::text, null), 1)
              and x.ch = '"'
            )
          )
      from
        pg_class pc
        join pg_namespace nsp
          on pc.relnamespace = nsp.oid
      where
        pc.oid = entity
    $$;


ALTER FUNCTION realtime.quote_wal2json(entity regclass) OWNER TO supabase_admin;

--
-- Name: send(jsonb, text, text, boolean); Type: FUNCTION; Schema: realtime; Owner: supabase_admin
--

CREATE FUNCTION realtime.send(payload jsonb, event text, topic text, private boolean DEFAULT true) RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
  BEGIN
    -- Set the topic configuration
    EXECUTE format('SET LOCAL realtime.topic TO %L', topic);

    -- Attempt to insert the message
    INSERT INTO realtime.messages (payload, event, topic, private, extension)
    VALUES (payload, event, topic, private, 'broadcast');
  EXCEPTION
    WHEN OTHERS THEN
      -- Capture and notify the error
      RAISE WARNING 'ErrorSendingBroadcastMessage: %', SQLERRM;
  END;
END;
$$;


ALTER FUNCTION realtime.send(payload jsonb, event text, topic text, private boolean) OWNER TO supabase_admin;

--
-- Name: subscription_check_filters(); Type: FUNCTION; Schema: realtime; Owner: supabase_admin
--

CREATE FUNCTION realtime.subscription_check_filters() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    /*
    Validates that the user defined filters for a subscription:
    - refer to valid columns that the claimed role may access
    - values are coercable to the correct column type
    */
    declare
        col_names text[] = coalesce(
                array_agg(c.column_name order by c.ordinal_position),
                '{}'::text[]
            )
            from
                information_schema.columns c
            where
                format('%I.%I', c.table_schema, c.table_name)::regclass = new.entity
                and pg_catalog.has_column_privilege(
                    (new.claims ->> 'role'),
                    format('%I.%I', c.table_schema, c.table_name)::regclass,
                    c.column_name,
                    'SELECT'
                );
        filter realtime.user_defined_filter;
        col_type regtype;

        in_val jsonb;
    begin
        for filter in select * from unnest(new.filters) loop
            -- Filtered column is valid
            if not filter.column_name = any(col_names) then
                raise exception 'invalid column for filter %', filter.column_name;
            end if;

            -- Type is sanitized and safe for string interpolation
            col_type = (
                select atttypid::regtype
                from pg_catalog.pg_attribute
                where attrelid = new.entity
                      and attname = filter.column_name
            );
            if col_type is null then
                raise exception 'failed to lookup type for column %', filter.column_name;
            end if;

            -- Set maximum number of entries for in filter
            if filter.op = 'in'::realtime.equality_op then
                in_val = realtime.cast(filter.value, (col_type::text || '[]')::regtype);
                if coalesce(jsonb_array_length(in_val), 0) > 100 then
                    raise exception 'too many values for `in` filter. Maximum 100';
                end if;
            else
                -- raises an exception if value is not coercable to type
                perform realtime.cast(filter.value, col_type);
            end if;

        end loop;

        -- Apply consistent order to filters so the unique constraint on
        -- (subscription_id, entity, filters) can't be tricked by a different filter order
        new.filters = coalesce(
            array_agg(f order by f.column_name, f.op, f.value),
            '{}'
        ) from unnest(new.filters) f;

        return new;
    end;
    $$;


ALTER FUNCTION realtime.subscription_check_filters() OWNER TO supabase_admin;

--
-- Name: to_regrole(text); Type: FUNCTION; Schema: realtime; Owner: supabase_admin
--

CREATE FUNCTION realtime.to_regrole(role_name text) RETURNS regrole
    LANGUAGE sql IMMUTABLE
    AS $$ select role_name::regrole $$;


ALTER FUNCTION realtime.to_regrole(role_name text) OWNER TO supabase_admin;

--
-- Name: topic(); Type: FUNCTION; Schema: realtime; Owner: supabase_realtime_admin
--

CREATE FUNCTION realtime.topic() RETURNS text
    LANGUAGE sql STABLE
    AS $$
select nullif(current_setting('realtime.topic', true), '')::text;
$$;


ALTER FUNCTION realtime.topic() OWNER TO supabase_realtime_admin;

--
-- Name: add_prefixes(text, text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.add_prefixes(_bucket_id text, _name text) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE
    prefixes text[];
BEGIN
    prefixes := "storage"."get_prefixes"("_name");

    IF array_length(prefixes, 1) > 0 THEN
        INSERT INTO storage.prefixes (name, bucket_id)
        SELECT UNNEST(prefixes) as name, "_bucket_id" ON CONFLICT DO NOTHING;
    END IF;
END;
$$;


ALTER FUNCTION storage.add_prefixes(_bucket_id text, _name text) OWNER TO supabase_storage_admin;

--
-- Name: can_insert_object(text, text, uuid, jsonb); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.can_insert_object(bucketid text, name text, owner uuid, metadata jsonb) RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
  INSERT INTO "storage"."objects" ("bucket_id", "name", "owner", "metadata") VALUES (bucketid, name, owner, metadata);
  -- hack to rollback the successful insert
  RAISE sqlstate 'PT200' using
  message = 'ROLLBACK',
  detail = 'rollback successful insert';
END
$$;


ALTER FUNCTION storage.can_insert_object(bucketid text, name text, owner uuid, metadata jsonb) OWNER TO supabase_storage_admin;

--
-- Name: delete_leaf_prefixes(text[], text[]); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.delete_leaf_prefixes(bucket_ids text[], names text[]) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE
    v_rows_deleted integer;
BEGIN
    LOOP
        WITH candidates AS (
            SELECT DISTINCT
                t.bucket_id,
                unnest(storage.get_prefixes(t.name)) AS name
            FROM unnest(bucket_ids, names) AS t(bucket_id, name)
        ),
        uniq AS (
             SELECT
                 bucket_id,
                 name,
                 storage.get_level(name) AS level
             FROM candidates
             WHERE name <> ''
             GROUP BY bucket_id, name
        ),
        leaf AS (
             SELECT
                 p.bucket_id,
                 p.name,
                 p.level
             FROM storage.prefixes AS p
                  JOIN uniq AS u
                       ON u.bucket_id = p.bucket_id
                           AND u.name = p.name
                           AND u.level = p.level
             WHERE NOT EXISTS (
                 SELECT 1
                 FROM storage.objects AS o
                 WHERE o.bucket_id = p.bucket_id
                   AND o.level = p.level + 1
                   AND o.name COLLATE "C" LIKE p.name || '/%'
             )
             AND NOT EXISTS (
                 SELECT 1
                 FROM storage.prefixes AS c
                 WHERE c.bucket_id = p.bucket_id
                   AND c.level = p.level + 1
                   AND c.name COLLATE "C" LIKE p.name || '/%'
             )
        )
        DELETE
        FROM storage.prefixes AS p
            USING leaf AS l
        WHERE p.bucket_id = l.bucket_id
          AND p.name = l.name
          AND p.level = l.level;

        GET DIAGNOSTICS v_rows_deleted = ROW_COUNT;
        EXIT WHEN v_rows_deleted = 0;
    END LOOP;
END;
$$;


ALTER FUNCTION storage.delete_leaf_prefixes(bucket_ids text[], names text[]) OWNER TO supabase_storage_admin;

--
-- Name: delete_prefix(text, text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.delete_prefix(_bucket_id text, _name text) RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
BEGIN
    -- Check if we can delete the prefix
    IF EXISTS(
        SELECT FROM "storage"."prefixes"
        WHERE "prefixes"."bucket_id" = "_bucket_id"
          AND level = "storage"."get_level"("_name") + 1
          AND "prefixes"."name" COLLATE "C" LIKE "_name" || '/%'
        LIMIT 1
    )
    OR EXISTS(
        SELECT FROM "storage"."objects"
        WHERE "objects"."bucket_id" = "_bucket_id"
          AND "storage"."get_level"("objects"."name") = "storage"."get_level"("_name") + 1
          AND "objects"."name" COLLATE "C" LIKE "_name" || '/%'
        LIMIT 1
    ) THEN
    -- There are sub-objects, skip deletion
    RETURN false;
    ELSE
        DELETE FROM "storage"."prefixes"
        WHERE "prefixes"."bucket_id" = "_bucket_id"
          AND level = "storage"."get_level"("_name")
          AND "prefixes"."name" = "_name";
        RETURN true;
    END IF;
END;
$$;


ALTER FUNCTION storage.delete_prefix(_bucket_id text, _name text) OWNER TO supabase_storage_admin;

--
-- Name: delete_prefix_hierarchy_trigger(); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.delete_prefix_hierarchy_trigger() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    prefix text;
BEGIN
    prefix := "storage"."get_prefix"(OLD."name");

    IF coalesce(prefix, '') != '' THEN
        PERFORM "storage"."delete_prefix"(OLD."bucket_id", prefix);
    END IF;

    RETURN OLD;
END;
$$;


ALTER FUNCTION storage.delete_prefix_hierarchy_trigger() OWNER TO supabase_storage_admin;

--
-- Name: enforce_bucket_name_length(); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.enforce_bucket_name_length() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
    if length(new.name) > 100 then
        raise exception 'bucket name "%" is too long (% characters). Max is 100.', new.name, length(new.name);
    end if;
    return new;
end;
$$;


ALTER FUNCTION storage.enforce_bucket_name_length() OWNER TO supabase_storage_admin;

--
-- Name: extension(text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.extension(name text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
    AS $$
DECLARE
    _parts text[];
    _filename text;
BEGIN
    SELECT string_to_array(name, '/') INTO _parts;
    SELECT _parts[array_length(_parts,1)] INTO _filename;
    RETURN reverse(split_part(reverse(_filename), '.', 1));
END
$$;


ALTER FUNCTION storage.extension(name text) OWNER TO supabase_storage_admin;

--
-- Name: filename(text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.filename(name text) RETURNS text
    LANGUAGE plpgsql
    AS $$
DECLARE
_parts text[];
BEGIN
	select string_to_array(name, '/') into _parts;
	return _parts[array_length(_parts,1)];
END
$$;


ALTER FUNCTION storage.filename(name text) OWNER TO supabase_storage_admin;

--
-- Name: foldername(text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.foldername(name text) RETURNS text[]
    LANGUAGE plpgsql IMMUTABLE
    AS $$
DECLARE
    _parts text[];
BEGIN
    -- Split on "/" to get path segments
    SELECT string_to_array(name, '/') INTO _parts;
    -- Return everything except the last segment
    RETURN _parts[1 : array_length(_parts,1) - 1];
END
$$;


ALTER FUNCTION storage.foldername(name text) OWNER TO supabase_storage_admin;

--
-- Name: get_level(text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.get_level(name text) RETURNS integer
    LANGUAGE sql IMMUTABLE STRICT
    AS $$
SELECT array_length(string_to_array("name", '/'), 1);
$$;


ALTER FUNCTION storage.get_level(name text) OWNER TO supabase_storage_admin;

--
-- Name: get_prefix(text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.get_prefix(name text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $_$
SELECT
    CASE WHEN strpos("name", '/') > 0 THEN
             regexp_replace("name", '[\/]{1}[^\/]+\/?$', '')
         ELSE
             ''
        END;
$_$;


ALTER FUNCTION storage.get_prefix(name text) OWNER TO supabase_storage_admin;

--
-- Name: get_prefixes(text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.get_prefixes(name text) RETURNS text[]
    LANGUAGE plpgsql IMMUTABLE STRICT
    AS $$
DECLARE
    parts text[];
    prefixes text[];
    prefix text;
BEGIN
    -- Split the name into parts by '/'
    parts := string_to_array("name", '/');
    prefixes := '{}';

    -- Construct the prefixes, stopping one level below the last part
    FOR i IN 1..array_length(parts, 1) - 1 LOOP
            prefix := array_to_string(parts[1:i], '/');
            prefixes := array_append(prefixes, prefix);
    END LOOP;

    RETURN prefixes;
END;
$$;


ALTER FUNCTION storage.get_prefixes(name text) OWNER TO supabase_storage_admin;

--
-- Name: get_size_by_bucket(); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.get_size_by_bucket() RETURNS TABLE(size bigint, bucket_id text)
    LANGUAGE plpgsql STABLE
    AS $$
BEGIN
    return query
        select sum((metadata->>'size')::bigint) as size, obj.bucket_id
        from "storage".objects as obj
        group by obj.bucket_id;
END
$$;


ALTER FUNCTION storage.get_size_by_bucket() OWNER TO supabase_storage_admin;

--
-- Name: list_multipart_uploads_with_delimiter(text, text, text, integer, text, text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.list_multipart_uploads_with_delimiter(bucket_id text, prefix_param text, delimiter_param text, max_keys integer DEFAULT 100, next_key_token text DEFAULT ''::text, next_upload_token text DEFAULT ''::text) RETURNS TABLE(key text, id text, created_at timestamp with time zone)
    LANGUAGE plpgsql
    AS $_$
BEGIN
    RETURN QUERY EXECUTE
        'SELECT DISTINCT ON(key COLLATE "C") * from (
            SELECT
                CASE
                    WHEN position($2 IN substring(key from length($1) + 1)) > 0 THEN
                        substring(key from 1 for length($1) + position($2 IN substring(key from length($1) + 1)))
                    ELSE
                        key
                END AS key, id, created_at
            FROM
                storage.s3_multipart_uploads
            WHERE
                bucket_id = $5 AND
                key ILIKE $1 || ''%'' AND
                CASE
                    WHEN $4 != '''' AND $6 = '''' THEN
                        CASE
                            WHEN position($2 IN substring(key from length($1) + 1)) > 0 THEN
                                substring(key from 1 for length($1) + position($2 IN substring(key from length($1) + 1))) COLLATE "C" > $4
                            ELSE
                                key COLLATE "C" > $4
                            END
                    ELSE
                        true
                END AND
                CASE
                    WHEN $6 != '''' THEN
                        id COLLATE "C" > $6
                    ELSE
                        true
                    END
            ORDER BY
                key COLLATE "C" ASC, created_at ASC) as e order by key COLLATE "C" LIMIT $3'
        USING prefix_param, delimiter_param, max_keys, next_key_token, bucket_id, next_upload_token;
END;
$_$;


ALTER FUNCTION storage.list_multipart_uploads_with_delimiter(bucket_id text, prefix_param text, delimiter_param text, max_keys integer, next_key_token text, next_upload_token text) OWNER TO supabase_storage_admin;

--
-- Name: list_objects_with_delimiter(text, text, text, integer, text, text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.list_objects_with_delimiter(bucket_id text, prefix_param text, delimiter_param text, max_keys integer DEFAULT 100, start_after text DEFAULT ''::text, next_token text DEFAULT ''::text) RETURNS TABLE(name text, id uuid, metadata jsonb, updated_at timestamp with time zone)
    LANGUAGE plpgsql
    AS $_$
BEGIN
    RETURN QUERY EXECUTE
        'SELECT DISTINCT ON(name COLLATE "C") * from (
            SELECT
                CASE
                    WHEN position($2 IN substring(name from length($1) + 1)) > 0 THEN
                        substring(name from 1 for length($1) + position($2 IN substring(name from length($1) + 1)))
                    ELSE
                        name
                END AS name, id, metadata, updated_at
            FROM
                storage.objects
            WHERE
                bucket_id = $5 AND
                name ILIKE $1 || ''%'' AND
                CASE
                    WHEN $6 != '''' THEN
                    name COLLATE "C" > $6
                ELSE true END
                AND CASE
                    WHEN $4 != '''' THEN
                        CASE
                            WHEN position($2 IN substring(name from length($1) + 1)) > 0 THEN
                                substring(name from 1 for length($1) + position($2 IN substring(name from length($1) + 1))) COLLATE "C" > $4
                            ELSE
                                name COLLATE "C" > $4
                            END
                    ELSE
                        true
                END
            ORDER BY
                name COLLATE "C" ASC) as e order by name COLLATE "C" LIMIT $3'
        USING prefix_param, delimiter_param, max_keys, next_token, bucket_id, start_after;
END;
$_$;


ALTER FUNCTION storage.list_objects_with_delimiter(bucket_id text, prefix_param text, delimiter_param text, max_keys integer, start_after text, next_token text) OWNER TO supabase_storage_admin;

--
-- Name: lock_top_prefixes(text[], text[]); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.lock_top_prefixes(bucket_ids text[], names text[]) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE
    v_bucket text;
    v_top text;
BEGIN
    FOR v_bucket, v_top IN
        SELECT DISTINCT t.bucket_id,
            split_part(t.name, '/', 1) AS top
        FROM unnest(bucket_ids, names) AS t(bucket_id, name)
        WHERE t.name <> ''
        ORDER BY 1, 2
        LOOP
            PERFORM pg_advisory_xact_lock(hashtextextended(v_bucket || '/' || v_top, 0));
        END LOOP;
END;
$$;


ALTER FUNCTION storage.lock_top_prefixes(bucket_ids text[], names text[]) OWNER TO supabase_storage_admin;

--
-- Name: objects_delete_cleanup(); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.objects_delete_cleanup() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE
    v_bucket_ids text[];
    v_names      text[];
BEGIN
    IF current_setting('storage.gc.prefixes', true) = '1' THEN
        RETURN NULL;
    END IF;

    PERFORM set_config('storage.gc.prefixes', '1', true);

    SELECT COALESCE(array_agg(d.bucket_id), '{}'),
           COALESCE(array_agg(d.name), '{}')
    INTO v_bucket_ids, v_names
    FROM deleted AS d
    WHERE d.name <> '';

    PERFORM storage.lock_top_prefixes(v_bucket_ids, v_names);
    PERFORM storage.delete_leaf_prefixes(v_bucket_ids, v_names);

    RETURN NULL;
END;
$$;


ALTER FUNCTION storage.objects_delete_cleanup() OWNER TO supabase_storage_admin;

--
-- Name: objects_insert_prefix_trigger(); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.objects_insert_prefix_trigger() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    PERFORM "storage"."add_prefixes"(NEW."bucket_id", NEW."name");
    NEW.level := "storage"."get_level"(NEW."name");

    RETURN NEW;
END;
$$;


ALTER FUNCTION storage.objects_insert_prefix_trigger() OWNER TO supabase_storage_admin;

--
-- Name: objects_update_cleanup(); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.objects_update_cleanup() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE
    -- NEW - OLD (destinations to create prefixes for)
    v_add_bucket_ids text[];
    v_add_names      text[];

    -- OLD - NEW (sources to prune)
    v_src_bucket_ids text[];
    v_src_names      text[];
BEGIN
    IF TG_OP <> 'UPDATE' THEN
        RETURN NULL;
    END IF;

    -- 1) Compute NEW−OLD (added paths) and OLD−NEW (moved-away paths)
    WITH added AS (
        SELECT n.bucket_id, n.name
        FROM new_rows n
        WHERE n.name <> '' AND position('/' in n.name) > 0
        EXCEPT
        SELECT o.bucket_id, o.name FROM old_rows o WHERE o.name <> ''
    ),
    moved AS (
         SELECT o.bucket_id, o.name
         FROM old_rows o
         WHERE o.name <> ''
         EXCEPT
         SELECT n.bucket_id, n.name FROM new_rows n WHERE n.name <> ''
    )
    SELECT
        -- arrays for ADDED (dest) in stable order
        COALESCE( (SELECT array_agg(a.bucket_id ORDER BY a.bucket_id, a.name) FROM added a), '{}' ),
        COALESCE( (SELECT array_agg(a.name      ORDER BY a.bucket_id, a.name) FROM added a), '{}' ),
        -- arrays for MOVED (src) in stable order
        COALESCE( (SELECT array_agg(m.bucket_id ORDER BY m.bucket_id, m.name) FROM moved m), '{}' ),
        COALESCE( (SELECT array_agg(m.name      ORDER BY m.bucket_id, m.name) FROM moved m), '{}' )
    INTO v_add_bucket_ids, v_add_names, v_src_bucket_ids, v_src_names;

    -- Nothing to do?
    IF (array_length(v_add_bucket_ids, 1) IS NULL) AND (array_length(v_src_bucket_ids, 1) IS NULL) THEN
        RETURN NULL;
    END IF;

    -- 2) Take per-(bucket, top) locks: ALL prefixes in consistent global order to prevent deadlocks
    DECLARE
        v_all_bucket_ids text[];
        v_all_names text[];
    BEGIN
        -- Combine source and destination arrays for consistent lock ordering
        v_all_bucket_ids := COALESCE(v_src_bucket_ids, '{}') || COALESCE(v_add_bucket_ids, '{}');
        v_all_names := COALESCE(v_src_names, '{}') || COALESCE(v_add_names, '{}');

        -- Single lock call ensures consistent global ordering across all transactions
        IF array_length(v_all_bucket_ids, 1) IS NOT NULL THEN
            PERFORM storage.lock_top_prefixes(v_all_bucket_ids, v_all_names);
        END IF;
    END;

    -- 3) Create destination prefixes (NEW−OLD) BEFORE pruning sources
    IF array_length(v_add_bucket_ids, 1) IS NOT NULL THEN
        WITH candidates AS (
            SELECT DISTINCT t.bucket_id, unnest(storage.get_prefixes(t.name)) AS name
            FROM unnest(v_add_bucket_ids, v_add_names) AS t(bucket_id, name)
            WHERE name <> ''
        )
        INSERT INTO storage.prefixes (bucket_id, name)
        SELECT c.bucket_id, c.name
        FROM candidates c
        ON CONFLICT DO NOTHING;
    END IF;

    -- 4) Prune source prefixes bottom-up for OLD−NEW
    IF array_length(v_src_bucket_ids, 1) IS NOT NULL THEN
        -- re-entrancy guard so DELETE on prefixes won't recurse
        IF current_setting('storage.gc.prefixes', true) <> '1' THEN
            PERFORM set_config('storage.gc.prefixes', '1', true);
        END IF;

        PERFORM storage.delete_leaf_prefixes(v_src_bucket_ids, v_src_names);
    END IF;

    RETURN NULL;
END;
$$;


ALTER FUNCTION storage.objects_update_cleanup() OWNER TO supabase_storage_admin;

--
-- Name: objects_update_level_trigger(); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.objects_update_level_trigger() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Ensure this is an update operation and the name has changed
    IF TG_OP = 'UPDATE' AND (NEW."name" <> OLD."name" OR NEW."bucket_id" <> OLD."bucket_id") THEN
        -- Set the new level
        NEW."level" := "storage"."get_level"(NEW."name");
    END IF;
    RETURN NEW;
END;
$$;


ALTER FUNCTION storage.objects_update_level_trigger() OWNER TO supabase_storage_admin;

--
-- Name: objects_update_prefix_trigger(); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.objects_update_prefix_trigger() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    old_prefixes TEXT[];
BEGIN
    -- Ensure this is an update operation and the name has changed
    IF TG_OP = 'UPDATE' AND (NEW."name" <> OLD."name" OR NEW."bucket_id" <> OLD."bucket_id") THEN
        -- Retrieve old prefixes
        old_prefixes := "storage"."get_prefixes"(OLD."name");

        -- Remove old prefixes that are only used by this object
        WITH all_prefixes as (
            SELECT unnest(old_prefixes) as prefix
        ),
        can_delete_prefixes as (
             SELECT prefix
             FROM all_prefixes
             WHERE NOT EXISTS (
                 SELECT 1 FROM "storage"."objects"
                 WHERE "bucket_id" = OLD."bucket_id"
                   AND "name" <> OLD."name"
                   AND "name" LIKE (prefix || '%')
             )
         )
        DELETE FROM "storage"."prefixes" WHERE name IN (SELECT prefix FROM can_delete_prefixes);

        -- Add new prefixes
        PERFORM "storage"."add_prefixes"(NEW."bucket_id", NEW."name");
    END IF;
    -- Set the new level
    NEW."level" := "storage"."get_level"(NEW."name");

    RETURN NEW;
END;
$$;


ALTER FUNCTION storage.objects_update_prefix_trigger() OWNER TO supabase_storage_admin;

--
-- Name: operation(); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.operation() RETURNS text
    LANGUAGE plpgsql STABLE
    AS $$
BEGIN
    RETURN current_setting('storage.operation', true);
END;
$$;


ALTER FUNCTION storage.operation() OWNER TO supabase_storage_admin;

--
-- Name: prefixes_delete_cleanup(); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.prefixes_delete_cleanup() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE
    v_bucket_ids text[];
    v_names      text[];
BEGIN
    IF current_setting('storage.gc.prefixes', true) = '1' THEN
        RETURN NULL;
    END IF;

    PERFORM set_config('storage.gc.prefixes', '1', true);

    SELECT COALESCE(array_agg(d.bucket_id), '{}'),
           COALESCE(array_agg(d.name), '{}')
    INTO v_bucket_ids, v_names
    FROM deleted AS d
    WHERE d.name <> '';

    PERFORM storage.lock_top_prefixes(v_bucket_ids, v_names);
    PERFORM storage.delete_leaf_prefixes(v_bucket_ids, v_names);

    RETURN NULL;
END;
$$;


ALTER FUNCTION storage.prefixes_delete_cleanup() OWNER TO supabase_storage_admin;

--
-- Name: prefixes_insert_trigger(); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.prefixes_insert_trigger() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    PERFORM "storage"."add_prefixes"(NEW."bucket_id", NEW."name");
    RETURN NEW;
END;
$$;


ALTER FUNCTION storage.prefixes_insert_trigger() OWNER TO supabase_storage_admin;

--
-- Name: search(text, text, integer, integer, integer, text, text, text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.search(prefix text, bucketname text, limits integer DEFAULT 100, levels integer DEFAULT 1, offsets integer DEFAULT 0, search text DEFAULT ''::text, sortcolumn text DEFAULT 'name'::text, sortorder text DEFAULT 'asc'::text) RETURNS TABLE(name text, id uuid, updated_at timestamp with time zone, created_at timestamp with time zone, last_accessed_at timestamp with time zone, metadata jsonb)
    LANGUAGE plpgsql
    AS $$
declare
    can_bypass_rls BOOLEAN;
begin
    SELECT rolbypassrls
    INTO can_bypass_rls
    FROM pg_roles
    WHERE rolname = coalesce(nullif(current_setting('role', true), 'none'), current_user);

    IF can_bypass_rls THEN
        RETURN QUERY SELECT * FROM storage.search_v1_optimised(prefix, bucketname, limits, levels, offsets, search, sortcolumn, sortorder);
    ELSE
        RETURN QUERY SELECT * FROM storage.search_legacy_v1(prefix, bucketname, limits, levels, offsets, search, sortcolumn, sortorder);
    END IF;
end;
$$;


ALTER FUNCTION storage.search(prefix text, bucketname text, limits integer, levels integer, offsets integer, search text, sortcolumn text, sortorder text) OWNER TO supabase_storage_admin;

--
-- Name: search_legacy_v1(text, text, integer, integer, integer, text, text, text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.search_legacy_v1(prefix text, bucketname text, limits integer DEFAULT 100, levels integer DEFAULT 1, offsets integer DEFAULT 0, search text DEFAULT ''::text, sortcolumn text DEFAULT 'name'::text, sortorder text DEFAULT 'asc'::text) RETURNS TABLE(name text, id uuid, updated_at timestamp with time zone, created_at timestamp with time zone, last_accessed_at timestamp with time zone, metadata jsonb)
    LANGUAGE plpgsql STABLE
    AS $_$
declare
    v_order_by text;
    v_sort_order text;
begin
    case
        when sortcolumn = 'name' then
            v_order_by = 'name';
        when sortcolumn = 'updated_at' then
            v_order_by = 'updated_at';
        when sortcolumn = 'created_at' then
            v_order_by = 'created_at';
        when sortcolumn = 'last_accessed_at' then
            v_order_by = 'last_accessed_at';
        else
            v_order_by = 'name';
        end case;

    case
        when sortorder = 'asc' then
            v_sort_order = 'asc';
        when sortorder = 'desc' then
            v_sort_order = 'desc';
        else
            v_sort_order = 'asc';
        end case;

    v_order_by = v_order_by || ' ' || v_sort_order;

    return query execute
        'with folders as (
           select path_tokens[$1] as folder
           from storage.objects
             where objects.name ilike $2 || $3 || ''%''
               and bucket_id = $4
               and array_length(objects.path_tokens, 1) <> $1
           group by folder
           order by folder ' || v_sort_order || '
     )
     (select folder as "name",
            null as id,
            null as updated_at,
            null as created_at,
            null as last_accessed_at,
            null as metadata from folders)
     union all
     (select path_tokens[$1] as "name",
            id,
            updated_at,
            created_at,
            last_accessed_at,
            metadata
     from storage.objects
     where objects.name ilike $2 || $3 || ''%''
       and bucket_id = $4
       and array_length(objects.path_tokens, 1) = $1
     order by ' || v_order_by || ')
     limit $5
     offset $6' using levels, prefix, search, bucketname, limits, offsets;
end;
$_$;


ALTER FUNCTION storage.search_legacy_v1(prefix text, bucketname text, limits integer, levels integer, offsets integer, search text, sortcolumn text, sortorder text) OWNER TO supabase_storage_admin;

--
-- Name: search_v1_optimised(text, text, integer, integer, integer, text, text, text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.search_v1_optimised(prefix text, bucketname text, limits integer DEFAULT 100, levels integer DEFAULT 1, offsets integer DEFAULT 0, search text DEFAULT ''::text, sortcolumn text DEFAULT 'name'::text, sortorder text DEFAULT 'asc'::text) RETURNS TABLE(name text, id uuid, updated_at timestamp with time zone, created_at timestamp with time zone, last_accessed_at timestamp with time zone, metadata jsonb)
    LANGUAGE plpgsql STABLE
    AS $_$
declare
    v_order_by text;
    v_sort_order text;
begin
    case
        when sortcolumn = 'name' then
            v_order_by = 'name';
        when sortcolumn = 'updated_at' then
            v_order_by = 'updated_at';
        when sortcolumn = 'created_at' then
            v_order_by = 'created_at';
        when sortcolumn = 'last_accessed_at' then
            v_order_by = 'last_accessed_at';
        else
            v_order_by = 'name';
        end case;

    case
        when sortorder = 'asc' then
            v_sort_order = 'asc';
        when sortorder = 'desc' then
            v_sort_order = 'desc';
        else
            v_sort_order = 'asc';
        end case;

    v_order_by = v_order_by || ' ' || v_sort_order;

    return query execute
        'with folders as (
           select (string_to_array(name, ''/''))[level] as name
           from storage.prefixes
             where lower(prefixes.name) like lower($2 || $3) || ''%''
               and bucket_id = $4
               and level = $1
           order by name ' || v_sort_order || '
     )
     (select name,
            null as id,
            null as updated_at,
            null as created_at,
            null as last_accessed_at,
            null as metadata from folders)
     union all
     (select path_tokens[level] as "name",
            id,
            updated_at,
            created_at,
            last_accessed_at,
            metadata
     from storage.objects
     where lower(objects.name) like lower($2 || $3) || ''%''
       and bucket_id = $4
       and level = $1
     order by ' || v_order_by || ')
     limit $5
     offset $6' using levels, prefix, search, bucketname, limits, offsets;
end;
$_$;


ALTER FUNCTION storage.search_v1_optimised(prefix text, bucketname text, limits integer, levels integer, offsets integer, search text, sortcolumn text, sortorder text) OWNER TO supabase_storage_admin;

--
-- Name: search_v2(text, text, integer, integer, text, text, text, text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.search_v2(prefix text, bucket_name text, limits integer DEFAULT 100, levels integer DEFAULT 1, start_after text DEFAULT ''::text, sort_order text DEFAULT 'asc'::text, sort_column text DEFAULT 'name'::text, sort_column_after text DEFAULT ''::text) RETURNS TABLE(key text, name text, id uuid, updated_at timestamp with time zone, created_at timestamp with time zone, last_accessed_at timestamp with time zone, metadata jsonb)
    LANGUAGE plpgsql STABLE
    AS $_$
DECLARE
    sort_col text;
    sort_ord text;
    cursor_op text;
    cursor_expr text;
    sort_expr text;
BEGIN
    -- Validate sort_order
    sort_ord := lower(sort_order);
    IF sort_ord NOT IN ('asc', 'desc') THEN
        sort_ord := 'asc';
    END IF;

    -- Determine cursor comparison operator
    IF sort_ord = 'asc' THEN
        cursor_op := '>';
    ELSE
        cursor_op := '<';
    END IF;
    
    sort_col := lower(sort_column);
    -- Validate sort column  
    IF sort_col IN ('updated_at', 'created_at') THEN
        cursor_expr := format(
            '($5 = '''' OR ROW(date_trunc(''milliseconds'', %I), name COLLATE "C") %s ROW(COALESCE(NULLIF($6, '''')::timestamptz, ''epoch''::timestamptz), $5))',
            sort_col, cursor_op
        );
        sort_expr := format(
            'COALESCE(date_trunc(''milliseconds'', %I), ''epoch''::timestamptz) %s, name COLLATE "C" %s',
            sort_col, sort_ord, sort_ord
        );
    ELSE
        cursor_expr := format('($5 = '''' OR name COLLATE "C" %s $5)', cursor_op);
        sort_expr := format('name COLLATE "C" %s', sort_ord);
    END IF;

    RETURN QUERY EXECUTE format(
        $sql$
        SELECT * FROM (
            (
                SELECT
                    split_part(name, '/', $4) AS key,
                    name,
                    NULL::uuid AS id,
                    updated_at,
                    created_at,
                    NULL::timestamptz AS last_accessed_at,
                    NULL::jsonb AS metadata
                FROM storage.prefixes
                WHERE name COLLATE "C" LIKE $1 || '%%'
                    AND bucket_id = $2
                    AND level = $4
                    AND %s
                ORDER BY %s
                LIMIT $3
            )
            UNION ALL
            (
                SELECT
                    split_part(name, '/', $4) AS key,
                    name,
                    id,
                    updated_at,
                    created_at,
                    last_accessed_at,
                    metadata
                FROM storage.objects
                WHERE name COLLATE "C" LIKE $1 || '%%'
                    AND bucket_id = $2
                    AND level = $4
                    AND %s
                ORDER BY %s
                LIMIT $3
            )
        ) obj
        ORDER BY %s
        LIMIT $3
        $sql$,
        cursor_expr,    -- prefixes WHERE
        sort_expr,      -- prefixes ORDER BY
        cursor_expr,    -- objects WHERE
        sort_expr,      -- objects ORDER BY
        sort_expr       -- final ORDER BY
    )
    USING prefix, bucket_name, limits, levels, start_after, sort_column_after;
END;
$_$;


ALTER FUNCTION storage.search_v2(prefix text, bucket_name text, limits integer, levels integer, start_after text, sort_order text, sort_column text, sort_column_after text) OWNER TO supabase_storage_admin;

--
-- Name: update_updated_at_column(); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.update_updated_at_column() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW; 
END;
$$;


ALTER FUNCTION storage.update_updated_at_column() OWNER TO supabase_storage_admin;

--
-- Name: http_request(); Type: FUNCTION; Schema: supabase_functions; Owner: supabase_functions_admin
--

CREATE FUNCTION supabase_functions.http_request() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'supabase_functions'
    AS $$
  DECLARE
    request_id bigint;
    payload jsonb;
    url text := TG_ARGV[0]::text;
    method text := TG_ARGV[1]::text;
    headers jsonb DEFAULT '{}'::jsonb;
    params jsonb DEFAULT '{}'::jsonb;
    timeout_ms integer DEFAULT 1000;
  BEGIN
    IF url IS NULL OR url = 'null' THEN
      RAISE EXCEPTION 'url argument is missing';
    END IF;

    IF method IS NULL OR method = 'null' THEN
      RAISE EXCEPTION 'method argument is missing';
    END IF;

    IF TG_ARGV[2] IS NULL OR TG_ARGV[2] = 'null' THEN
      headers = '{"Content-Type": "application/json"}'::jsonb;
    ELSE
      headers = TG_ARGV[2]::jsonb;
    END IF;

    IF TG_ARGV[3] IS NULL OR TG_ARGV[3] = 'null' THEN
      params = '{}'::jsonb;
    ELSE
      params = TG_ARGV[3]::jsonb;
    END IF;

    IF TG_ARGV[4] IS NULL OR TG_ARGV[4] = 'null' THEN
      timeout_ms = 1000;
    ELSE
      timeout_ms = TG_ARGV[4]::integer;
    END IF;

    CASE
      WHEN method = 'GET' THEN
        SELECT http_get INTO request_id FROM net.http_get(
          url,
          params,
          headers,
          timeout_ms
        );
      WHEN method = 'POST' THEN
        payload = jsonb_build_object(
          'old_record', OLD,
          'record', NEW,
          'type', TG_OP,
          'table', TG_TABLE_NAME,
          'schema', TG_TABLE_SCHEMA
        );

        SELECT http_post INTO request_id FROM net.http_post(
          url,
          payload,
          params,
          headers,
          timeout_ms
        );
      ELSE
        RAISE EXCEPTION 'method argument % is invalid', method;
    END CASE;

    INSERT INTO supabase_functions.hooks
      (hook_table_id, hook_name, request_id)
    VALUES
      (TG_RELID, TG_NAME, request_id);

    RETURN NEW;
  END
$$;


ALTER FUNCTION supabase_functions.http_request() OWNER TO supabase_functions_admin;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: extensions; Type: TABLE; Schema: _realtime; Owner: supabase_admin
--

CREATE TABLE _realtime.extensions (
    id uuid NOT NULL,
    type text,
    settings jsonb,
    tenant_external_id text,
    inserted_at timestamp(0) without time zone NOT NULL,
    updated_at timestamp(0) without time zone NOT NULL
);


ALTER TABLE _realtime.extensions OWNER TO supabase_admin;

--
-- Name: schema_migrations; Type: TABLE; Schema: _realtime; Owner: supabase_admin
--

CREATE TABLE _realtime.schema_migrations (
    version bigint NOT NULL,
    inserted_at timestamp(0) without time zone
);


ALTER TABLE _realtime.schema_migrations OWNER TO supabase_admin;

--
-- Name: tenants; Type: TABLE; Schema: _realtime; Owner: supabase_admin
--

CREATE TABLE _realtime.tenants (
    id uuid NOT NULL,
    name text,
    external_id text,
    jwt_secret text,
    max_concurrent_users integer DEFAULT 200 NOT NULL,
    inserted_at timestamp(0) without time zone NOT NULL,
    updated_at timestamp(0) without time zone NOT NULL,
    max_events_per_second integer DEFAULT 100 NOT NULL,
    postgres_cdc_default text DEFAULT 'postgres_cdc_rls'::text,
    max_bytes_per_second integer DEFAULT 100000 NOT NULL,
    max_channels_per_client integer DEFAULT 100 NOT NULL,
    max_joins_per_second integer DEFAULT 500 NOT NULL,
    suspend boolean DEFAULT false,
    jwt_jwks jsonb,
    notify_private_alpha boolean DEFAULT false,
    private_only boolean DEFAULT false NOT NULL,
    migrations_ran integer DEFAULT 0,
    broadcast_adapter character varying(255) DEFAULT 'gen_rpc'::character varying,
    max_presence_events_per_second integer DEFAULT 10000,
    max_payload_size_in_kb integer DEFAULT 3000
);


ALTER TABLE _realtime.tenants OWNER TO supabase_admin;

--
-- Name: audit_log_entries; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.audit_log_entries (
    instance_id uuid,
    id uuid NOT NULL,
    payload json,
    created_at timestamp with time zone,
    ip_address character varying(64) DEFAULT ''::character varying NOT NULL
);


ALTER TABLE auth.audit_log_entries OWNER TO supabase_auth_admin;

--
-- Name: TABLE audit_log_entries; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.audit_log_entries IS 'Auth: Audit trail for user actions.';


--
-- Name: flow_state; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.flow_state (
    id uuid NOT NULL,
    user_id uuid,
    auth_code text NOT NULL,
    code_challenge_method auth.code_challenge_method NOT NULL,
    code_challenge text NOT NULL,
    provider_type text NOT NULL,
    provider_access_token text,
    provider_refresh_token text,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    authentication_method text NOT NULL,
    auth_code_issued_at timestamp with time zone
);


ALTER TABLE auth.flow_state OWNER TO supabase_auth_admin;

--
-- Name: TABLE flow_state; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.flow_state IS 'stores metadata for pkce logins';


--
-- Name: identities; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.identities (
    provider_id text NOT NULL,
    user_id uuid NOT NULL,
    identity_data jsonb NOT NULL,
    provider text NOT NULL,
    last_sign_in_at timestamp with time zone,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    email text GENERATED ALWAYS AS (lower((identity_data ->> 'email'::text))) STORED,
    id uuid DEFAULT gen_random_uuid() NOT NULL
);


ALTER TABLE auth.identities OWNER TO supabase_auth_admin;

--
-- Name: TABLE identities; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.identities IS 'Auth: Stores identities associated to a user.';


--
-- Name: COLUMN identities.email; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON COLUMN auth.identities.email IS 'Auth: Email is a generated column that references the optional email property in the identity_data';


--
-- Name: instances; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.instances (
    id uuid NOT NULL,
    uuid uuid,
    raw_base_config text,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);


ALTER TABLE auth.instances OWNER TO supabase_auth_admin;

--
-- Name: TABLE instances; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.instances IS 'Auth: Manages users across multiple sites.';


--
-- Name: mfa_amr_claims; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.mfa_amr_claims (
    session_id uuid NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    authentication_method text NOT NULL,
    id uuid NOT NULL
);


ALTER TABLE auth.mfa_amr_claims OWNER TO supabase_auth_admin;

--
-- Name: TABLE mfa_amr_claims; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.mfa_amr_claims IS 'auth: stores authenticator method reference claims for multi factor authentication';


--
-- Name: mfa_challenges; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.mfa_challenges (
    id uuid NOT NULL,
    factor_id uuid NOT NULL,
    created_at timestamp with time zone NOT NULL,
    verified_at timestamp with time zone,
    ip_address inet NOT NULL,
    otp_code text,
    web_authn_session_data jsonb
);


ALTER TABLE auth.mfa_challenges OWNER TO supabase_auth_admin;

--
-- Name: TABLE mfa_challenges; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.mfa_challenges IS 'auth: stores metadata about challenge requests made';


--
-- Name: mfa_factors; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.mfa_factors (
    id uuid NOT NULL,
    user_id uuid NOT NULL,
    friendly_name text,
    factor_type auth.factor_type NOT NULL,
    status auth.factor_status NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    secret text,
    phone text,
    last_challenged_at timestamp with time zone,
    web_authn_credential jsonb,
    web_authn_aaguid uuid
);


ALTER TABLE auth.mfa_factors OWNER TO supabase_auth_admin;

--
-- Name: TABLE mfa_factors; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.mfa_factors IS 'auth: stores metadata about factors';


--
-- Name: oauth_authorizations; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.oauth_authorizations (
    id uuid NOT NULL,
    authorization_id text NOT NULL,
    client_id uuid NOT NULL,
    user_id uuid,
    redirect_uri text NOT NULL,
    scope text NOT NULL,
    state text,
    resource text,
    code_challenge text,
    code_challenge_method auth.code_challenge_method,
    response_type auth.oauth_response_type DEFAULT 'code'::auth.oauth_response_type NOT NULL,
    status auth.oauth_authorization_status DEFAULT 'pending'::auth.oauth_authorization_status NOT NULL,
    authorization_code text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone DEFAULT (now() + '00:03:00'::interval) NOT NULL,
    approved_at timestamp with time zone,
    CONSTRAINT oauth_authorizations_authorization_code_length CHECK ((char_length(authorization_code) <= 255)),
    CONSTRAINT oauth_authorizations_code_challenge_length CHECK ((char_length(code_challenge) <= 128)),
    CONSTRAINT oauth_authorizations_expires_at_future CHECK ((expires_at > created_at)),
    CONSTRAINT oauth_authorizations_redirect_uri_length CHECK ((char_length(redirect_uri) <= 2048)),
    CONSTRAINT oauth_authorizations_resource_length CHECK ((char_length(resource) <= 2048)),
    CONSTRAINT oauth_authorizations_scope_length CHECK ((char_length(scope) <= 4096)),
    CONSTRAINT oauth_authorizations_state_length CHECK ((char_length(state) <= 4096))
);


ALTER TABLE auth.oauth_authorizations OWNER TO supabase_auth_admin;

--
-- Name: oauth_clients; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.oauth_clients (
    id uuid NOT NULL,
    client_secret_hash text,
    registration_type auth.oauth_registration_type NOT NULL,
    redirect_uris text NOT NULL,
    grant_types text NOT NULL,
    client_name text,
    client_uri text,
    logo_uri text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    deleted_at timestamp with time zone,
    client_type auth.oauth_client_type DEFAULT 'confidential'::auth.oauth_client_type NOT NULL,
    CONSTRAINT oauth_clients_client_name_length CHECK ((char_length(client_name) <= 1024)),
    CONSTRAINT oauth_clients_client_uri_length CHECK ((char_length(client_uri) <= 2048)),
    CONSTRAINT oauth_clients_logo_uri_length CHECK ((char_length(logo_uri) <= 2048))
);


ALTER TABLE auth.oauth_clients OWNER TO supabase_auth_admin;

--
-- Name: oauth_consents; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.oauth_consents (
    id uuid NOT NULL,
    user_id uuid NOT NULL,
    client_id uuid NOT NULL,
    scopes text NOT NULL,
    granted_at timestamp with time zone DEFAULT now() NOT NULL,
    revoked_at timestamp with time zone,
    CONSTRAINT oauth_consents_revoked_after_granted CHECK (((revoked_at IS NULL) OR (revoked_at >= granted_at))),
    CONSTRAINT oauth_consents_scopes_length CHECK ((char_length(scopes) <= 2048)),
    CONSTRAINT oauth_consents_scopes_not_empty CHECK ((char_length(TRIM(BOTH FROM scopes)) > 0))
);


ALTER TABLE auth.oauth_consents OWNER TO supabase_auth_admin;

--
-- Name: one_time_tokens; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.one_time_tokens (
    id uuid NOT NULL,
    user_id uuid NOT NULL,
    token_type auth.one_time_token_type NOT NULL,
    token_hash text NOT NULL,
    relates_to text NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    CONSTRAINT one_time_tokens_token_hash_check CHECK ((char_length(token_hash) > 0))
);


ALTER TABLE auth.one_time_tokens OWNER TO supabase_auth_admin;

--
-- Name: refresh_tokens; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.refresh_tokens (
    instance_id uuid,
    id bigint NOT NULL,
    token character varying(255),
    user_id character varying(255),
    revoked boolean,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    parent character varying(255),
    session_id uuid
);


ALTER TABLE auth.refresh_tokens OWNER TO supabase_auth_admin;

--
-- Name: TABLE refresh_tokens; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.refresh_tokens IS 'Auth: Store of tokens used to refresh JWT tokens once they expire.';


--
-- Name: refresh_tokens_id_seq; Type: SEQUENCE; Schema: auth; Owner: supabase_auth_admin
--

CREATE SEQUENCE auth.refresh_tokens_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE auth.refresh_tokens_id_seq OWNER TO supabase_auth_admin;

--
-- Name: refresh_tokens_id_seq; Type: SEQUENCE OWNED BY; Schema: auth; Owner: supabase_auth_admin
--

ALTER SEQUENCE auth.refresh_tokens_id_seq OWNED BY auth.refresh_tokens.id;


--
-- Name: saml_providers; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.saml_providers (
    id uuid NOT NULL,
    sso_provider_id uuid NOT NULL,
    entity_id text NOT NULL,
    metadata_xml text NOT NULL,
    metadata_url text,
    attribute_mapping jsonb,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    name_id_format text,
    CONSTRAINT "entity_id not empty" CHECK ((char_length(entity_id) > 0)),
    CONSTRAINT "metadata_url not empty" CHECK (((metadata_url = NULL::text) OR (char_length(metadata_url) > 0))),
    CONSTRAINT "metadata_xml not empty" CHECK ((char_length(metadata_xml) > 0))
);


ALTER TABLE auth.saml_providers OWNER TO supabase_auth_admin;

--
-- Name: TABLE saml_providers; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.saml_providers IS 'Auth: Manages SAML Identity Provider connections.';


--
-- Name: saml_relay_states; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.saml_relay_states (
    id uuid NOT NULL,
    sso_provider_id uuid NOT NULL,
    request_id text NOT NULL,
    for_email text,
    redirect_to text,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    flow_state_id uuid,
    CONSTRAINT "request_id not empty" CHECK ((char_length(request_id) > 0))
);


ALTER TABLE auth.saml_relay_states OWNER TO supabase_auth_admin;

--
-- Name: TABLE saml_relay_states; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.saml_relay_states IS 'Auth: Contains SAML Relay State information for each Service Provider initiated login.';


--
-- Name: schema_migrations; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.schema_migrations (
    version character varying(255) NOT NULL
);


ALTER TABLE auth.schema_migrations OWNER TO supabase_auth_admin;

--
-- Name: TABLE schema_migrations; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.schema_migrations IS 'Auth: Manages updates to the auth system.';


--
-- Name: sessions; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.sessions (
    id uuid NOT NULL,
    user_id uuid NOT NULL,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    factor_id uuid,
    aal auth.aal_level,
    not_after timestamp with time zone,
    refreshed_at timestamp without time zone,
    user_agent text,
    ip inet,
    tag text,
    oauth_client_id uuid
);


ALTER TABLE auth.sessions OWNER TO supabase_auth_admin;

--
-- Name: TABLE sessions; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.sessions IS 'Auth: Stores session data associated to a user.';


--
-- Name: COLUMN sessions.not_after; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON COLUMN auth.sessions.not_after IS 'Auth: Not after is a nullable column that contains a timestamp after which the session should be regarded as expired.';


--
-- Name: sso_domains; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.sso_domains (
    id uuid NOT NULL,
    sso_provider_id uuid NOT NULL,
    domain text NOT NULL,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    CONSTRAINT "domain not empty" CHECK ((char_length(domain) > 0))
);


ALTER TABLE auth.sso_domains OWNER TO supabase_auth_admin;

--
-- Name: TABLE sso_domains; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.sso_domains IS 'Auth: Manages SSO email address domain mapping to an SSO Identity Provider.';


--
-- Name: sso_providers; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.sso_providers (
    id uuid NOT NULL,
    resource_id text,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    disabled boolean,
    CONSTRAINT "resource_id not empty" CHECK (((resource_id = NULL::text) OR (char_length(resource_id) > 0)))
);


ALTER TABLE auth.sso_providers OWNER TO supabase_auth_admin;

--
-- Name: TABLE sso_providers; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.sso_providers IS 'Auth: Manages SSO identity provider information; see saml_providers for SAML.';


--
-- Name: COLUMN sso_providers.resource_id; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON COLUMN auth.sso_providers.resource_id IS 'Auth: Uniquely identifies a SSO provider according to a user-chosen resource ID (case insensitive), useful in infrastructure as code.';


--
-- Name: users; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.users (
    instance_id uuid,
    id uuid NOT NULL,
    aud character varying(255),
    role character varying(255),
    email character varying(255),
    encrypted_password character varying(255),
    email_confirmed_at timestamp with time zone,
    invited_at timestamp with time zone,
    confirmation_token character varying(255),
    confirmation_sent_at timestamp with time zone,
    recovery_token character varying(255),
    recovery_sent_at timestamp with time zone,
    email_change_token_new character varying(255),
    email_change character varying(255),
    email_change_sent_at timestamp with time zone,
    last_sign_in_at timestamp with time zone,
    raw_app_meta_data jsonb,
    raw_user_meta_data jsonb,
    is_super_admin boolean,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    phone text DEFAULT NULL::character varying,
    phone_confirmed_at timestamp with time zone,
    phone_change text DEFAULT ''::character varying,
    phone_change_token character varying(255) DEFAULT ''::character varying,
    phone_change_sent_at timestamp with time zone,
    confirmed_at timestamp with time zone GENERATED ALWAYS AS (LEAST(email_confirmed_at, phone_confirmed_at)) STORED,
    email_change_token_current character varying(255) DEFAULT ''::character varying,
    email_change_confirm_status smallint DEFAULT 0,
    banned_until timestamp with time zone,
    reauthentication_token character varying(255) DEFAULT ''::character varying,
    reauthentication_sent_at timestamp with time zone,
    is_sso_user boolean DEFAULT false NOT NULL,
    deleted_at timestamp with time zone,
    is_anonymous boolean DEFAULT false NOT NULL,
    CONSTRAINT users_email_change_confirm_status_check CHECK (((email_change_confirm_status >= 0) AND (email_change_confirm_status <= 2)))
);


ALTER TABLE auth.users OWNER TO supabase_auth_admin;

--
-- Name: TABLE users; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.users IS 'Auth: Stores user login data within a secure schema.';


--
-- Name: COLUMN users.is_sso_user; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON COLUMN auth.users.is_sso_user IS 'Auth: Set this column to true when the account comes from SSO. These accounts can have duplicate emails.';


--
-- Name: adv_campaign_daily_stats; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.adv_campaign_daily_stats (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    advert_id bigint NOT NULL,
    nm_id bigint NOT NULL,
    vendor_code text NOT NULL,
    date date NOT NULL,
    views integer DEFAULT 0,
    clicks integer DEFAULT 0,
    cpc numeric(10,2),
    ctr numeric(5,2),
    sum numeric(14,2) DEFAULT 0,
    orders integer DEFAULT 0,
    orders_sum numeric(14,2) DEFAULT 0,
    cpm numeric(10,2),
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.adv_campaign_daily_stats OWNER TO postgres;

--
-- Name: TABLE adv_campaign_daily_stats; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.adv_campaign_daily_stats IS 'Детальная статистика по рекламным кампаниям: каждая строка = один артикул в одной кампании за один день';


--
-- Name: COLUMN adv_campaign_daily_stats.views; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.adv_campaign_daily_stats.views IS 'Показы артикула (сумма по всем платформам из nms[])';


--
-- Name: COLUMN adv_campaign_daily_stats.orders; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.adv_campaign_daily_stats.orders IS 'Заказы из days[] - включает склейку (ассоциированные артикулы)';


--
-- Name: COLUMN adv_campaign_daily_stats.cpm; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.adv_campaign_daily_stats.cpm IS 'CPM (Cost Per Mille) = затраты на 1000 показов';


--
-- Name: adv_params; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.adv_params (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    nm_id bigint NOT NULL,
    vendor_code text NOT NULL,
    date date NOT NULL,
    views integer DEFAULT 0,
    clicks integer DEFAULT 0,
    sum numeric(14,2) DEFAULT 0,
    cpc numeric(10,2),
    cpm numeric(10,2),
    ctr numeric(5,2),
    orders integer DEFAULT 0,
    orders_sum numeric(14,2) DEFAULT 0,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.adv_params OWNER TO postgres;

--
-- Name: TABLE adv_params; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.adv_params IS 'Агрегированная рекламная статистика: каждая строка = один артикул за один день (суммируем все кампании)';


--
-- Name: COLUMN adv_params.views; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.adv_params.views IS 'Суммарные показы артикула во всех кампаниях';


--
-- Name: COLUMN adv_params.cpm; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.adv_params.cpm IS 'CPM = (sum / views) * 1000, NULL если views = 0';


--
-- Name: COLUMN adv_params.orders; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.adv_params.orders IS 'Суммарные заказы артикула во всех кампаниях (включая склейку)';


--
-- Name: cost_price; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.cost_price (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    product_id uuid,
    nm_id bigint NOT NULL,
    vendor_code text NOT NULL,
    date date NOT NULL,
    cost_price numeric(14,2) NOT NULL
);


ALTER TABLE public.cost_price OWNER TO postgres;

--
-- Name: cr_daily_stats; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.cr_daily_stats (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    product_id uuid NOT NULL,
    nm_id bigint NOT NULL,
    vendor_code text NOT NULL,
    date_of_period date NOT NULL,
    open_card_count integer,
    add_to_cart_count integer,
    orders_count integer,
    cancel_count integer,
    orders_sum_rub numeric(14,2),
    stocks_mp integer,
    stocks_wb integer,
    add_to_cart_percent numeric(5,2),
    cart_to_order_percent numeric(5,2),
    order_price numeric(14,2),
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.cr_daily_stats OWNER TO postgres;

--
-- Name: paid_acceptance; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.paid_acceptance (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    product_id uuid,
    nm_id bigint NOT NULL,
    vendor_code text NOT NULL,
    shk_create_date date NOT NULL,
    count integer NOT NULL,
    total numeric(14,2) NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.paid_acceptance OWNER TO postgres;

--
-- Name: TABLE paid_acceptance; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.paid_acceptance IS 'Daily paid acceptance per nm_id aggregated by shk_create_date';


--
-- Name: COLUMN paid_acceptance.total; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.paid_acceptance.total IS 'Sum of total acceptance cost for the day & article';


--
-- Name: paid_storage; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.paid_storage (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    product_id uuid,
    nm_id bigint NOT NULL,
    vendor_code text NOT NULL,
    date date NOT NULL,
    warehouse_price numeric(14,2) NOT NULL,
    gi_ids bigint[],
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.paid_storage OWNER TO postgres;

--
-- Name: TABLE paid_storage; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.paid_storage IS 'Daily paid storage cost per nm_id (aggregated by date & nm_id)';


--
-- Name: COLUMN paid_storage.warehouse_price; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.paid_storage.warehouse_price IS 'Sum of warehousePrice for the day & article';


--
-- Name: COLUMN paid_storage.gi_ids; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.paid_storage.gi_ids IS 'Array of related supply ids (giId) for the aggregated row';


--
-- Name: product_sizes; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.product_sizes (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    serial_id bigint NOT NULL,
    product_id uuid NOT NULL,
    barcode text NOT NULL,
    size text
);


ALTER TABLE public.product_sizes OWNER TO postgres;

--
-- Name: product_sizes_serial_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.product_sizes_serial_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.product_sizes_serial_id_seq OWNER TO postgres;

--
-- Name: product_sizes_serial_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.product_sizes_serial_id_seq OWNED BY public.product_sizes.serial_id;


--
-- Name: products; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.products (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    serial_id bigint NOT NULL,
    vendor_code text NOT NULL,
    nm_id bigint NOT NULL,
    imt_id bigint NOT NULL,
    category_wb text NOT NULL,
    main_photo_url text,
    title text NOT NULL
);


ALTER TABLE public.products OWNER TO postgres;

--
-- Name: products_serial_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.products_serial_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.products_serial_id_seq OWNER TO postgres;

--
-- Name: products_serial_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.products_serial_id_seq OWNED BY public.products.serial_id;


--
-- Name: week_reports_serial_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.week_reports_serial_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.week_reports_serial_id_seq OWNER TO postgres;

--
-- Name: week_reports; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.week_reports (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    serial_id integer DEFAULT nextval('public.week_reports_serial_id_seq'::regclass),
    report_type text,
    realizationreport_id integer NOT NULL,
    date_from date NOT NULL,
    date_to date NOT NULL,
    quantity_sells_total integer,
    quantity_return_total integer,
    cancels_total integer,
    retail_price_total numeric(14,2),
    retail_amount_total numeric(14,2),
    rub_discountwb_both_total numeric(14,2),
    perc_discountwb_both_total numeric(5,2),
    ppvz_for_pay_total numeric(14,2),
    rub_commision_both_total numeric(14,2),
    perc_commisian_both_total numeric(5,2),
    delivery_amount_total numeric(14,2),
    return_amount_total numeric(14,2),
    delivery_rub_total numeric(14,2),
    penalty_total numeric(14,2),
    storage_fee_total numeric(14,2),
    deduction_total numeric(14,2),
    acceptance_total numeric(14,2),
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.week_reports OWNER TO postgres;

--
-- Name: week_rows; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.week_rows (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    product_id uuid,
    realizationreport_id integer NOT NULL,
    report_type text,
    rr_id bigint,
    gi_id bigint,
    order_dt date,
    sale_dt date,
    srid text,
    barcode bigint,
    ts_name text,
    nm_id bigint,
    sa_name text,
    doc_type_name text,
    quantity integer,
    retail_price numeric(12,2),
    retail_amount numeric(12,2),
    rub_discountwb_both numeric(12,2),
    perc_discountwb_both numeric(12,2),
    ppvz_spp_prc numeric(12,2),
    rub_spp numeric(12,2),
    rub_wallet_dicount numeric(12,2),
    perc_wallet_discount numeric(12,2),
    ppvz_for_pay numeric(12,2),
    rub_commision_both numeric(12,2),
    perc_commisian_both numeric(12,2),
    commission_percent numeric(12,2),
    rub_commission numeric(12,2),
    rub_excess_comission numeric(12,2),
    perc_excess_comission numeric(12,2),
    supplier_oper_name text,
    bonus_type_name text,
    delivery_amount numeric(12,2),
    return_amount numeric(12,2),
    delivery_rub numeric(12,2),
    site_country text,
    office_name text,
    penalty numeric(12,2),
    storage_fee numeric(12,2),
    deduction numeric(12,2),
    acceptance numeric(12,2),
    kiz text,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.week_rows OWNER TO postgres;

--
-- Name: week_stats; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.week_stats (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    report_type text,
    realizationreport_id integer NOT NULL,
    product_id uuid,
    date_from date NOT NULL,
    date_to date NOT NULL,
    nm_id bigint NOT NULL,
    sa_name text,
    quantity_sells_nm integer,
    quantity_return_nm integer,
    cancels_nm integer,
    retail_price_nm numeric(12,2),
    retail_amount_nm numeric(12,2),
    rub_discountwb_both_nm numeric(12,2),
    perc_discountwb_both_nm numeric(12,2),
    rub_spp_nm numeric(12,2),
    perc_spp_nm numeric(12,2),
    perc_wallet_discount_nm numeric(12,2),
    ppvz_for_pay_nm numeric(12,2),
    rub_commision_both_nm numeric(12,2),
    perc_commisian_both_nm numeric(12,2),
    rub_commission_nm numeric(12,2),
    perc_commission_nm numeric(12,2),
    rub_excess_comission_nm numeric(12,2),
    perc_excess_comission_nm numeric(12,2),
    delivery_amount_nm numeric(12,2),
    return_amount_nm numeric(12,2),
    delivery_rub_nm numeric(12,2),
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.week_stats OWNER TO postgres;

--
-- Name: messages; Type: TABLE; Schema: realtime; Owner: supabase_realtime_admin
--

CREATE TABLE realtime.messages (
    topic text NOT NULL,
    extension text NOT NULL,
    payload jsonb,
    event text,
    private boolean DEFAULT false,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    id uuid DEFAULT gen_random_uuid() NOT NULL
)
PARTITION BY RANGE (inserted_at);


ALTER TABLE realtime.messages OWNER TO supabase_realtime_admin;

--
-- Name: messages_2025_11_12; Type: TABLE; Schema: realtime; Owner: supabase_admin
--

CREATE TABLE realtime.messages_2025_11_12 (
    topic text NOT NULL,
    extension text NOT NULL,
    payload jsonb,
    event text,
    private boolean DEFAULT false,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    id uuid DEFAULT gen_random_uuid() NOT NULL
);


ALTER TABLE realtime.messages_2025_11_12 OWNER TO supabase_admin;

--
-- Name: messages_2025_11_13; Type: TABLE; Schema: realtime; Owner: supabase_admin
--

CREATE TABLE realtime.messages_2025_11_13 (
    topic text NOT NULL,
    extension text NOT NULL,
    payload jsonb,
    event text,
    private boolean DEFAULT false,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    id uuid DEFAULT gen_random_uuid() NOT NULL
);


ALTER TABLE realtime.messages_2025_11_13 OWNER TO supabase_admin;

--
-- Name: messages_2025_11_14; Type: TABLE; Schema: realtime; Owner: supabase_admin
--

CREATE TABLE realtime.messages_2025_11_14 (
    topic text NOT NULL,
    extension text NOT NULL,
    payload jsonb,
    event text,
    private boolean DEFAULT false,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    id uuid DEFAULT gen_random_uuid() NOT NULL
);


ALTER TABLE realtime.messages_2025_11_14 OWNER TO supabase_admin;

--
-- Name: messages_2025_11_15; Type: TABLE; Schema: realtime; Owner: supabase_admin
--

CREATE TABLE realtime.messages_2025_11_15 (
    topic text NOT NULL,
    extension text NOT NULL,
    payload jsonb,
    event text,
    private boolean DEFAULT false,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    id uuid DEFAULT gen_random_uuid() NOT NULL
);


ALTER TABLE realtime.messages_2025_11_15 OWNER TO supabase_admin;

--
-- Name: messages_2025_11_16; Type: TABLE; Schema: realtime; Owner: supabase_admin
--

CREATE TABLE realtime.messages_2025_11_16 (
    topic text NOT NULL,
    extension text NOT NULL,
    payload jsonb,
    event text,
    private boolean DEFAULT false,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    id uuid DEFAULT gen_random_uuid() NOT NULL
);


ALTER TABLE realtime.messages_2025_11_16 OWNER TO supabase_admin;

--
-- Name: schema_migrations; Type: TABLE; Schema: realtime; Owner: supabase_admin
--

CREATE TABLE realtime.schema_migrations (
    version bigint NOT NULL,
    inserted_at timestamp(0) without time zone
);


ALTER TABLE realtime.schema_migrations OWNER TO supabase_admin;

--
-- Name: subscription; Type: TABLE; Schema: realtime; Owner: supabase_admin
--

CREATE TABLE realtime.subscription (
    id bigint NOT NULL,
    subscription_id uuid NOT NULL,
    entity regclass NOT NULL,
    filters realtime.user_defined_filter[] DEFAULT '{}'::realtime.user_defined_filter[] NOT NULL,
    claims jsonb NOT NULL,
    claims_role regrole GENERATED ALWAYS AS (realtime.to_regrole((claims ->> 'role'::text))) STORED NOT NULL,
    created_at timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL
);


ALTER TABLE realtime.subscription OWNER TO supabase_admin;

--
-- Name: subscription_id_seq; Type: SEQUENCE; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE realtime.subscription ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME realtime.subscription_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: buckets; Type: TABLE; Schema: storage; Owner: supabase_storage_admin
--

CREATE TABLE storage.buckets (
    id text NOT NULL,
    name text NOT NULL,
    owner uuid,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    public boolean DEFAULT false,
    avif_autodetection boolean DEFAULT false,
    file_size_limit bigint,
    allowed_mime_types text[],
    owner_id text,
    type storage.buckettype DEFAULT 'STANDARD'::storage.buckettype NOT NULL
);


ALTER TABLE storage.buckets OWNER TO supabase_storage_admin;

--
-- Name: COLUMN buckets.owner; Type: COMMENT; Schema: storage; Owner: supabase_storage_admin
--

COMMENT ON COLUMN storage.buckets.owner IS 'Field is deprecated, use owner_id instead';


--
-- Name: buckets_analytics; Type: TABLE; Schema: storage; Owner: supabase_storage_admin
--

CREATE TABLE storage.buckets_analytics (
    id text NOT NULL,
    type storage.buckettype DEFAULT 'ANALYTICS'::storage.buckettype NOT NULL,
    format text DEFAULT 'ICEBERG'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE storage.buckets_analytics OWNER TO supabase_storage_admin;

--
-- Name: iceberg_namespaces; Type: TABLE; Schema: storage; Owner: supabase_storage_admin
--

CREATE TABLE storage.iceberg_namespaces (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    bucket_id text NOT NULL,
    name text NOT NULL COLLATE pg_catalog."C",
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE storage.iceberg_namespaces OWNER TO supabase_storage_admin;

--
-- Name: iceberg_tables; Type: TABLE; Schema: storage; Owner: supabase_storage_admin
--

CREATE TABLE storage.iceberg_tables (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    namespace_id uuid NOT NULL,
    bucket_id text NOT NULL,
    name text NOT NULL COLLATE pg_catalog."C",
    location text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE storage.iceberg_tables OWNER TO supabase_storage_admin;

--
-- Name: migrations; Type: TABLE; Schema: storage; Owner: supabase_storage_admin
--

CREATE TABLE storage.migrations (
    id integer NOT NULL,
    name character varying(100) NOT NULL,
    hash character varying(40) NOT NULL,
    executed_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE storage.migrations OWNER TO supabase_storage_admin;

--
-- Name: objects; Type: TABLE; Schema: storage; Owner: supabase_storage_admin
--

CREATE TABLE storage.objects (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    bucket_id text,
    name text,
    owner uuid,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    last_accessed_at timestamp with time zone DEFAULT now(),
    metadata jsonb,
    path_tokens text[] GENERATED ALWAYS AS (string_to_array(name, '/'::text)) STORED,
    version text,
    owner_id text,
    user_metadata jsonb,
    level integer
);


ALTER TABLE storage.objects OWNER TO supabase_storage_admin;

--
-- Name: COLUMN objects.owner; Type: COMMENT; Schema: storage; Owner: supabase_storage_admin
--

COMMENT ON COLUMN storage.objects.owner IS 'Field is deprecated, use owner_id instead';


--
-- Name: prefixes; Type: TABLE; Schema: storage; Owner: supabase_storage_admin
--

CREATE TABLE storage.prefixes (
    bucket_id text NOT NULL,
    name text NOT NULL COLLATE pg_catalog."C",
    level integer GENERATED ALWAYS AS (storage.get_level(name)) STORED NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE storage.prefixes OWNER TO supabase_storage_admin;

--
-- Name: s3_multipart_uploads; Type: TABLE; Schema: storage; Owner: supabase_storage_admin
--

CREATE TABLE storage.s3_multipart_uploads (
    id text NOT NULL,
    in_progress_size bigint DEFAULT 0 NOT NULL,
    upload_signature text NOT NULL,
    bucket_id text NOT NULL,
    key text NOT NULL COLLATE pg_catalog."C",
    version text NOT NULL,
    owner_id text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    user_metadata jsonb
);


ALTER TABLE storage.s3_multipart_uploads OWNER TO supabase_storage_admin;

--
-- Name: s3_multipart_uploads_parts; Type: TABLE; Schema: storage; Owner: supabase_storage_admin
--

CREATE TABLE storage.s3_multipart_uploads_parts (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    upload_id text NOT NULL,
    size bigint DEFAULT 0 NOT NULL,
    part_number integer NOT NULL,
    bucket_id text NOT NULL,
    key text NOT NULL COLLATE pg_catalog."C",
    etag text NOT NULL,
    owner_id text,
    version text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE storage.s3_multipart_uploads_parts OWNER TO supabase_storage_admin;

--
-- Name: hooks; Type: TABLE; Schema: supabase_functions; Owner: supabase_functions_admin
--

CREATE TABLE supabase_functions.hooks (
    id bigint NOT NULL,
    hook_table_id integer NOT NULL,
    hook_name text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    request_id bigint
);


ALTER TABLE supabase_functions.hooks OWNER TO supabase_functions_admin;

--
-- Name: TABLE hooks; Type: COMMENT; Schema: supabase_functions; Owner: supabase_functions_admin
--

COMMENT ON TABLE supabase_functions.hooks IS 'Supabase Functions Hooks: Audit trail for triggered hooks.';


--
-- Name: hooks_id_seq; Type: SEQUENCE; Schema: supabase_functions; Owner: supabase_functions_admin
--

CREATE SEQUENCE supabase_functions.hooks_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE supabase_functions.hooks_id_seq OWNER TO supabase_functions_admin;

--
-- Name: hooks_id_seq; Type: SEQUENCE OWNED BY; Schema: supabase_functions; Owner: supabase_functions_admin
--

ALTER SEQUENCE supabase_functions.hooks_id_seq OWNED BY supabase_functions.hooks.id;


--
-- Name: migrations; Type: TABLE; Schema: supabase_functions; Owner: supabase_functions_admin
--

CREATE TABLE supabase_functions.migrations (
    version text NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE supabase_functions.migrations OWNER TO supabase_functions_admin;

--
-- Name: schema_migrations; Type: TABLE; Schema: supabase_migrations; Owner: postgres
--

CREATE TABLE supabase_migrations.schema_migrations (
    version text NOT NULL,
    statements text[],
    name text
);


ALTER TABLE supabase_migrations.schema_migrations OWNER TO postgres;

--
-- Name: messages_2025_11_12; Type: TABLE ATTACH; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages ATTACH PARTITION realtime.messages_2025_11_12 FOR VALUES FROM ('2025-11-12 00:00:00') TO ('2025-11-13 00:00:00');


--
-- Name: messages_2025_11_13; Type: TABLE ATTACH; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages ATTACH PARTITION realtime.messages_2025_11_13 FOR VALUES FROM ('2025-11-13 00:00:00') TO ('2025-11-14 00:00:00');


--
-- Name: messages_2025_11_14; Type: TABLE ATTACH; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages ATTACH PARTITION realtime.messages_2025_11_14 FOR VALUES FROM ('2025-11-14 00:00:00') TO ('2025-11-15 00:00:00');


--
-- Name: messages_2025_11_15; Type: TABLE ATTACH; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages ATTACH PARTITION realtime.messages_2025_11_15 FOR VALUES FROM ('2025-11-15 00:00:00') TO ('2025-11-16 00:00:00');


--
-- Name: messages_2025_11_16; Type: TABLE ATTACH; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages ATTACH PARTITION realtime.messages_2025_11_16 FOR VALUES FROM ('2025-11-16 00:00:00') TO ('2025-11-17 00:00:00');


--
-- Name: refresh_tokens id; Type: DEFAULT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.refresh_tokens ALTER COLUMN id SET DEFAULT nextval('auth.refresh_tokens_id_seq'::regclass);


--
-- Name: product_sizes serial_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.product_sizes ALTER COLUMN serial_id SET DEFAULT nextval('public.product_sizes_serial_id_seq'::regclass);


--
-- Name: products serial_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.products ALTER COLUMN serial_id SET DEFAULT nextval('public.products_serial_id_seq'::regclass);


--
-- Name: hooks id; Type: DEFAULT; Schema: supabase_functions; Owner: supabase_functions_admin
--

ALTER TABLE ONLY supabase_functions.hooks ALTER COLUMN id SET DEFAULT nextval('supabase_functions.hooks_id_seq'::regclass);


--
-- Data for Name: extensions; Type: TABLE DATA; Schema: _realtime; Owner: supabase_admin
--

COPY _realtime.extensions (id, type, settings, tenant_external_id, inserted_at, updated_at) FROM stdin;
6c4e17d9-319a-489f-8721-f53e6f7f2a3e	postgres_cdc_rls	{"region": "us-east-1", "db_host": "2UlYpoQDWZTe1Kf7JM5GzuAXjvlqI6phI/ZNikpKymE=", "db_name": "sWBpZNdjggEPTQVlI52Zfw==", "db_port": "+enMDFi1J/3IrrquHHwUmA==", "db_user": "uxbEq/zz8DXVD53TOI1zmw==", "slot_name": "supabase_realtime_replication_slot", "db_password": "sWBpZNdjggEPTQVlI52Zfw==", "publication": "supabase_realtime", "ssl_enforced": false, "poll_interval_ms": 100, "poll_max_changes": 100, "poll_max_record_bytes": 1048576}	realtime-dev	2025-11-13 12:04:30	2025-11-13 12:04:30
\.


--
-- Data for Name: schema_migrations; Type: TABLE DATA; Schema: _realtime; Owner: supabase_admin
--

COPY _realtime.schema_migrations (version, inserted_at) FROM stdin;
20210706140551	2025-11-13 12:04:15
20220329161857	2025-11-13 12:04:15
20220410212326	2025-11-13 12:04:15
20220506102948	2025-11-13 12:04:15
20220527210857	2025-11-13 12:04:15
20220815211129	2025-11-13 12:04:15
20220815215024	2025-11-13 12:04:15
20220818141501	2025-11-13 12:04:15
20221018173709	2025-11-13 12:04:15
20221102172703	2025-11-13 12:04:15
20221223010058	2025-11-13 12:04:15
20230110180046	2025-11-13 12:04:15
20230810220907	2025-11-13 12:04:15
20230810220924	2025-11-13 12:04:15
20231024094642	2025-11-13 12:04:15
20240306114423	2025-11-13 12:04:15
20240418082835	2025-11-13 12:04:15
20240625211759	2025-11-13 12:04:15
20240704172020	2025-11-13 12:04:15
20240902173232	2025-11-13 12:04:15
20241106103258	2025-11-13 12:04:15
20250424203323	2025-11-13 12:04:15
20250613072131	2025-11-13 12:04:15
20250711044927	2025-11-13 12:04:15
20250811121559	2025-11-13 12:04:15
\.


--
-- Data for Name: tenants; Type: TABLE DATA; Schema: _realtime; Owner: supabase_admin
--

COPY _realtime.tenants (id, name, external_id, jwt_secret, max_concurrent_users, inserted_at, updated_at, max_events_per_second, postgres_cdc_default, max_bytes_per_second, max_channels_per_client, max_joins_per_second, suspend, jwt_jwks, notify_private_alpha, private_only, migrations_ran, broadcast_adapter, max_presence_events_per_second, max_payload_size_in_kb) FROM stdin;
a0d4b696-2ad6-41cc-8c63-bce60123a2fa	realtime-dev	realtime-dev	iNjicxc4+llvc9wovDvqymwfnj9teWMlyOIbJ8Fh6j2WNU8CIJ2ZgjR6MUIKqSmeDmvpsKLsZ9jgXJmQPpwL8w==	200	2025-11-13 12:04:30	2025-11-13 12:04:30	100	postgres_cdc_rls	100000	100	100	f	{"keys": [{"k": "c3VwZXItc2VjcmV0LWp3dC10b2tlbi13aXRoLWF0LWxlYXN0LTMyLWNoYXJhY3RlcnMtbG9uZw", "kty": "oct"}]}	f	f	64	gen_rpc	10000	3000
\.


--
-- Data for Name: audit_log_entries; Type: TABLE DATA; Schema: auth; Owner: supabase_auth_admin
--

COPY auth.audit_log_entries (instance_id, id, payload, created_at, ip_address) FROM stdin;
\.


--
-- Data for Name: flow_state; Type: TABLE DATA; Schema: auth; Owner: supabase_auth_admin
--

COPY auth.flow_state (id, user_id, auth_code, code_challenge_method, code_challenge, provider_type, provider_access_token, provider_refresh_token, created_at, updated_at, authentication_method, auth_code_issued_at) FROM stdin;
\.


--
-- Data for Name: identities; Type: TABLE DATA; Schema: auth; Owner: supabase_auth_admin
--

COPY auth.identities (provider_id, user_id, identity_data, provider, last_sign_in_at, created_at, updated_at, id) FROM stdin;
\.


--
-- Data for Name: instances; Type: TABLE DATA; Schema: auth; Owner: supabase_auth_admin
--

COPY auth.instances (id, uuid, raw_base_config, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: mfa_amr_claims; Type: TABLE DATA; Schema: auth; Owner: supabase_auth_admin
--

COPY auth.mfa_amr_claims (session_id, created_at, updated_at, authentication_method, id) FROM stdin;
\.


--
-- Data for Name: mfa_challenges; Type: TABLE DATA; Schema: auth; Owner: supabase_auth_admin
--

COPY auth.mfa_challenges (id, factor_id, created_at, verified_at, ip_address, otp_code, web_authn_session_data) FROM stdin;
\.


--
-- Data for Name: mfa_factors; Type: TABLE DATA; Schema: auth; Owner: supabase_auth_admin
--

COPY auth.mfa_factors (id, user_id, friendly_name, factor_type, status, created_at, updated_at, secret, phone, last_challenged_at, web_authn_credential, web_authn_aaguid) FROM stdin;
\.


--
-- Data for Name: oauth_authorizations; Type: TABLE DATA; Schema: auth; Owner: supabase_auth_admin
--

COPY auth.oauth_authorizations (id, authorization_id, client_id, user_id, redirect_uri, scope, state, resource, code_challenge, code_challenge_method, response_type, status, authorization_code, created_at, expires_at, approved_at) FROM stdin;
\.


--
-- Data for Name: oauth_clients; Type: TABLE DATA; Schema: auth; Owner: supabase_auth_admin
--

COPY auth.oauth_clients (id, client_secret_hash, registration_type, redirect_uris, grant_types, client_name, client_uri, logo_uri, created_at, updated_at, deleted_at, client_type) FROM stdin;
\.


--
-- Data for Name: oauth_consents; Type: TABLE DATA; Schema: auth; Owner: supabase_auth_admin
--

COPY auth.oauth_consents (id, user_id, client_id, scopes, granted_at, revoked_at) FROM stdin;
\.


--
-- Data for Name: one_time_tokens; Type: TABLE DATA; Schema: auth; Owner: supabase_auth_admin
--

COPY auth.one_time_tokens (id, user_id, token_type, token_hash, relates_to, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: refresh_tokens; Type: TABLE DATA; Schema: auth; Owner: supabase_auth_admin
--

COPY auth.refresh_tokens (instance_id, id, token, user_id, revoked, created_at, updated_at, parent, session_id) FROM stdin;
\.


--
-- Data for Name: saml_providers; Type: TABLE DATA; Schema: auth; Owner: supabase_auth_admin
--

COPY auth.saml_providers (id, sso_provider_id, entity_id, metadata_xml, metadata_url, attribute_mapping, created_at, updated_at, name_id_format) FROM stdin;
\.


--
-- Data for Name: saml_relay_states; Type: TABLE DATA; Schema: auth; Owner: supabase_auth_admin
--

COPY auth.saml_relay_states (id, sso_provider_id, request_id, for_email, redirect_to, created_at, updated_at, flow_state_id) FROM stdin;
\.


--
-- Data for Name: schema_migrations; Type: TABLE DATA; Schema: auth; Owner: supabase_auth_admin
--

COPY auth.schema_migrations (version) FROM stdin;
20171026211738
20171026211808
20171026211834
20180103212743
20180108183307
20180119214651
20180125194653
00
20210710035447
20210722035447
20210730183235
20210909172000
20210927181326
20211122151130
20211124214934
20211202183645
20220114185221
20220114185340
20220224000811
20220323170000
20220429102000
20220531120530
20220614074223
20220811173540
20221003041349
20221003041400
20221011041400
20221020193600
20221021073300
20221021082433
20221027105023
20221114143122
20221114143410
20221125140132
20221208132122
20221215195500
20221215195800
20221215195900
20230116124310
20230116124412
20230131181311
20230322519590
20230402418590
20230411005111
20230508135423
20230523124323
20230818113222
20230914180801
20231027141322
20231114161723
20231117164230
20240115144230
20240214120130
20240306115329
20240314092811
20240427152123
20240612123726
20240729123726
20240802193726
20240806073726
20241009103726
20250717082212
20250731150234
20250804100000
20250901200500
20250903112500
20250904133000
\.


--
-- Data for Name: sessions; Type: TABLE DATA; Schema: auth; Owner: supabase_auth_admin
--

COPY auth.sessions (id, user_id, created_at, updated_at, factor_id, aal, not_after, refreshed_at, user_agent, ip, tag, oauth_client_id) FROM stdin;
\.


--
-- Data for Name: sso_domains; Type: TABLE DATA; Schema: auth; Owner: supabase_auth_admin
--

COPY auth.sso_domains (id, sso_provider_id, domain, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: sso_providers; Type: TABLE DATA; Schema: auth; Owner: supabase_auth_admin
--

COPY auth.sso_providers (id, resource_id, created_at, updated_at, disabled) FROM stdin;
\.


--
-- Data for Name: users; Type: TABLE DATA; Schema: auth; Owner: supabase_auth_admin
--

COPY auth.users (instance_id, id, aud, role, email, encrypted_password, email_confirmed_at, invited_at, confirmation_token, confirmation_sent_at, recovery_token, recovery_sent_at, email_change_token_new, email_change, email_change_sent_at, last_sign_in_at, raw_app_meta_data, raw_user_meta_data, is_super_admin, created_at, updated_at, phone, phone_confirmed_at, phone_change, phone_change_token, phone_change_sent_at, email_change_token_current, email_change_confirm_status, banned_until, reauthentication_token, reauthentication_sent_at, is_sso_user, deleted_at, is_anonymous) FROM stdin;
\.


--
-- Data for Name: adv_campaign_daily_stats; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.adv_campaign_daily_stats (id, advert_id, nm_id, vendor_code, date, views, clicks, cpc, ctr, sum, orders, orders_sum, cpm, created_at, updated_at) FROM stdin;
562c4866-b324-4aa4-865c-eba91030993b	27114105	456770543	rykzak_black	2025-11-03	5	0	\N	0.00	1.80	0	0.00	360.00	2025-11-13 12:28:48.509956+00	2025-11-13 12:37:08.659633+00
e5a9bb7f-144c-4939-bac9-9cd7082e9216	27114105	456770543	rykzak_black	2025-11-11	1	0	\N	0.00	0.45	0	0.00	450.00	2025-11-13 12:05:05.161241+00	2025-11-13 12:37:08.659633+00
1d7cb518-31d3-4bc5-b054-c67e116ffbdd	27114105	456770543	rykzak_black	2025-11-02	1	1	0.31	100.00	0.31	0	0.00	310.00	2025-11-13 12:28:48.509956+00	2025-11-13 12:37:08.659633+00
d03b15b6-0f95-4c8c-ac9d-412424002b85	27114105	456770543	rykzak_black	2025-11-12	1	0	\N	0.00	0.45	0	0.00	450.00	2025-11-13 12:05:05.161241+00	2025-11-13 12:37:08.659633+00
97573053-2608-4513-8510-0ce5c326f694	27114105	456770543	rykzak_black	2025-10-27	5	0	\N	0.00	1.67	0	0.00	334.00	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
5f3a5471-e72a-425b-b88c-95edaef42941	27114105	456770543	rykzak_black	2025-11-01	5	1	1.48	20.00	1.48	0	0.00	296.00	2025-11-13 12:28:48.509956+00	2025-11-13 12:37:08.659633+00
6aff0f59-ce83-42e8-a862-f6c5de05bff2	27114105	456770543	rykzak_black	2025-10-30	3	0	\N	0.00	0.29	0	0.00	96.67	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
168ce8cc-45be-4120-b5b6-fb61ce66a447	27114105	456770543	rykzak_black	2025-10-26	7	0	\N	0.00	2.42	0	0.00	345.71	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
d4f7e794-9e67-4534-9f23-0607933591f3	27114105	456770543	rykzak_black	2025-10-24	8	0	\N	0.00	2.53	0	0.00	316.25	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
2d9f7487-b6fe-432c-8fdd-56022e146ee7	27114105	456770543	rykzak_black	2025-10-15	47	1	16.48	2.13	16.48	0	0.00	350.64	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
3b1a1608-fcdc-4528-913b-ca9234a34fce	27114115	456770543	rykzak_black	2025-11-02	1867	108	11.58	5.78	1250.40	8	10200.00	669.74	2025-11-13 12:28:48.509956+00	2025-11-13 12:37:08.659633+00
ed9cbdc3-b578-42e5-8c93-4c97f54a5b53	27114115	456770543	rykzak_black	2025-10-21	3953	189	11.50	4.78	2174.19	4	5565.00	550.01	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
0f95feaa-cf3a-498f-9328-b22e9c4abdfc	27114115	456770543	rykzak_black	2025-10-17	4382	229	10.53	5.23	2410.23	9	12640.00	550.03	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
8adfba90-d113-4aa1-98a0-fa52e0fb69af	27114115	456770543	rykzak_black	2025-10-18	3882	184	12.00	4.74	2207.56	8	11104.00	568.67	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
f38d0761-c2a4-4a3e-8c29-1c7712fbdc5f	27114115	456770543	rykzak_black	2025-10-25	2721	111	12.27	4.08	1361.42	6	8400.00	500.34	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
0871c1df-945d-48c9-8df5-27fbc0b67307	27114115	456770543	rykzak_black	2025-11-03	21	0	\N	0.00	7.56	1	1275.00	360.00	2025-11-13 12:28:48.509956+00	2025-11-13 12:37:08.659633+00
123e0620-e4df-412c-b434-2ec7948f34fb	27114115	456770543	rykzak_black	2025-11-11	2	0	\N	0.00	1.08	0	0.00	540.00	2025-11-13 12:05:05.161241+00	2025-11-13 12:37:08.659633+00
31544357-2905-4c6e-87b8-20b368d146c1	27114105	456770543	rykzak_black	2025-10-23	9	0	\N	0.00	3.86	0	0.00	428.89	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
f56277d4-8448-41e2-9ff7-a75b67f4bc52	27114105	456770543	rykzak_black	2025-10-19	25	1	9.00	4.00	9.00	0	0.00	360.00	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
76a68d3e-6866-43ba-9a03-1b650f6c933f	27114105	456770543	rykzak_black	2025-10-31	1	0	\N	0.00	0.09	0	0.00	90.00	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
0f91997f-d5e3-4213-b855-b388000bc90c	27114105	456770543	rykzak_black	2025-10-16	31	3	3.39	9.68	10.17	0	0.00	328.06	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
e6a9e55f-c6c3-44cb-8091-665a52c397fa	27114105	456770543	rykzak_black	2025-10-29	89	1	11.10	1.12	11.10	0	0.00	124.72	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
7e1e288f-3495-418e-99e7-5b8d431cd57c	27114105	456770543	rykzak_black	2025-10-28	2	1	0.76	50.00	0.76	0	0.00	380.00	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
bfd76a7f-04c1-4ac4-9aeb-70754ad352a7	27114105	456770543	rykzak_black	2025-11-06	4	0	\N	0.00	1.57	0	0.00	392.50	2025-11-13 12:28:48.509956+00	2025-11-13 12:37:08.659633+00
f41b4df7-cd88-4fab-a5f3-669e56613ba3	27114105	456770543	rykzak_black	2025-10-20	9	0	\N	0.00	3.59	0	0.00	398.89	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
5f95a7ff-6457-4290-a34b-85bc5c68d5b5	27114105	456770543	rykzak_black	2025-11-07	5	0	\N	0.00	2.25	0	0.00	450.00	2025-11-13 12:28:48.509956+00	2025-11-13 12:37:08.659633+00
76dbbaf6-0e47-4c34-a131-3cd8be8d963f	27114105	456770543	rykzak_black	2025-11-04	1	0	\N	0.00	0.12	0	0.00	120.00	2025-11-13 12:28:48.509956+00	2025-11-13 12:37:08.659633+00
f2c52ab3-5eaf-4463-96ff-a3ce1f262e3d	27114105	456770543	rykzak_black	2025-11-10	1	0	\N	0.00	0.45	0	0.00	450.00	2025-11-13 12:28:48.509956+00	2025-11-13 12:37:08.659633+00
4b92f4c7-857f-4dae-9bc9-cf7b6ac69477	27114105	456770543	rykzak_black	2025-10-22	10	0	\N	0.00	3.46	0	0.00	346.00	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
8b518cd4-f5d8-45c7-8dfe-330f5c1e1548	27114105	456770543	rykzak_black	2025-10-21	3	0	\N	0.00	1.15	0	0.00	383.33	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
afd7adc3-7653-4541-9638-a5a2aee4510b	27114105	456770543	rykzak_black	2025-10-17	33	0	\N	0.00	8.22	1	1425.00	249.09	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
7f73f534-6d8e-4b05-a257-0ba90b369087	27114105	456770543	rykzak_black	2025-10-18	16	0	\N	0.00	5.13	0	0.00	320.62	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
830ce845-82d0-40d3-845e-1aebb9bc5406	27114105	456770543	rykzak_black	2025-10-25	5	0	\N	0.00	1.71	0	0.00	342.00	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
358bf9c1-42de-4a2c-9ab4-250431a6d407	27114115	456770543	rykzak_black	2025-11-10	654	32	27.07	4.89	866.34	2	2550.00	1324.68	2025-11-13 12:28:48.509956+00	2025-11-13 12:37:08.659633+00
7d09a395-5d86-4812-acc5-82743c0481dc	27114115	456770543	rykzak_black	2025-10-22	4147	161	14.16	3.88	2280.33	8	10876.00	549.87	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
5b6a63e9-b42b-4d56-aacc-a0f98a4818a3	27114115	456770543	rykzak_black	2025-11-07	4	0	\N	0.00	2.63	1	1275.00	657.50	2025-11-13 12:28:48.509956+00	2025-11-13 12:37:08.659633+00
bd6684c8-af16-4642-9fd0-54f0f685cc70	27114115	456770543	rykzak_black	2025-11-04	671	48	9.58	7.15	459.69	0	0.00	685.08	2025-11-13 12:28:48.509956+00	2025-11-13 12:37:08.659633+00
41c56a53-96f2-4965-a148-f46f83f771ca	27114115	456770543	rykzak_black	2025-10-20	4903	236	11.43	4.81	2696.87	9	12505.00	550.04	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
2ff35ba6-f959-4825-8b23-f8524f401cd9	27114115	456770543	rykzak_black	2025-11-05	174	11	6.30	6.32	69.29	1	1275.00	398.22	2025-11-13 12:28:48.509956+00	2025-11-13 12:37:08.659633+00
39856351-6eee-45fe-aacd-db1d76b2ddd6	27114115	456770543	rykzak_black	2025-10-16	5059	275	10.31	5.44	2836.22	16	23332.00	560.63	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
a2abf890-e08d-4e11-84c9-dad7d538c955	27114115	456770543	rykzak_black	2025-10-28	1681	51	16.11	3.03	821.73	3	4125.00	488.83	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
c47d908b-4016-474c-b4de-7814911f2335	27114115	456770543	rykzak_black	2025-10-29	68	1	31.28	1.47	31.28	1	1275.00	460.00	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
23b927fd-cef9-493f-bd35-23e9b476954a	27114115	456770543	rykzak_black	2025-11-06	2	0	\N	0.00	0.68	0	0.00	340.00	2025-11-13 12:28:48.509956+00	2025-11-13 12:37:08.659633+00
fe440c0d-95e0-4ba5-a4af-7000002e587f	27114115	456770543	rykzak_black	2025-10-19	3950	169	13.39	4.28	2263.69	4	5702.00	573.09	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
a3b9ab09-4722-4482-8e0c-f1637cafea5f	27114115	456770543	rykzak_black	2025-10-31	2854	78	9.40	2.73	733.25	2	2850.00	256.92	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
623f891c-850a-431e-a86a-9803da5a2266	27114115	456770543	rykzak_black	2025-10-23	4083	204	11.01	5.00	2245.36	11	15305.00	549.93	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
fd34b284-af46-4624-868e-6206fbf5c3ee	27114115	456770543	rykzak_black	2025-10-15	4193	220	10.75	5.25	2366.00	7	10127.00	564.27	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
45394a8c-1b2d-4a15-b535-bae1719905b9	27114115	456770543	rykzak_black	2025-11-08	278	22	6.81	7.91	149.91	4	5100.00	539.24	2025-11-13 12:28:48.509956+00	2025-11-13 12:37:08.659633+00
edd6bfc1-8310-480e-b232-a015a649b515	27114115	456770543	rykzak_black	2025-10-26	2287	103	11.11	4.50	1143.98	1	1275.00	500.21	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
1f91ae97-d788-4b2c-a1ab-68ffe71e50b9	27114115	456770543	rykzak_black	2025-10-24	3574	142	13.88	3.97	1971.44	8	11400.00	551.61	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
c3804cf2-0b3c-49a8-aee0-f0793e083f92	27114115	456770543	rykzak_black	2025-10-30	11	0	\N	0.00	5.11	1	1275.00	464.55	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
73f27670-4067-4ec8-a9e9-656b155350bd	27114115	456770543	rykzak_black	2025-11-09	692	31	12.80	4.48	396.73	1	1388.00	573.31	2025-11-13 12:28:48.509956+00	2025-11-13 12:37:08.659633+00
2b770ead-c8d8-4edc-b41a-f09b8c3ce373	27114115	456770543	rykzak_black	2025-11-01	2148	106	12.17	4.93	1290.20	7	9225.00	600.65	2025-11-13 12:28:48.509956+00	2025-11-13 12:37:08.659633+00
aa80935d-bd55-49d8-a951-ab0bf60cc9bb	27114115	456770543	rykzak_black	2025-10-27	16	0	\N	0.00	8.20	1	1275.00	512.50	2025-11-13 12:37:08.659633+00	2025-11-13 12:37:08.659633+00
7da4c4ce-b1f9-4696-975a-6deb4387a8e0	27114115	456770543	rykzak_black	2025-11-13	3	1	2.41	33.33	2.41	0	0.00	803.33	2025-11-13 12:05:05.161241+00	2025-11-13 12:37:08.659633+00
db27e152-0042-457b-b7c8-3d10850958cc	30167264	555528176	album_serdechki	2025-11-08	792	44	5.34	5.56	234.84	5	2785.00	296.52	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
bb08f3a6-6206-48d5-85d9-fbef54f069a7	30167264	555528176	album_serdechki	2025-11-12	3635	153	8.09	4.21	1237.57	9	5122.00	340.46	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
48f2099b-1123-42bb-9c7a-ec5eb00ea027	30167264	555528176	album_serdechki	2025-11-10	4116	376	5.67	9.14	2132.12	15	9725.00	518.01	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
b2638d54-a678-4c3b-9ebf-717e9e454366	30167264	555528176	album_serdechki	2025-11-11	3962	245	7.14	6.18	1748.64	25	14005.00	441.35	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
d94b931e-5aa3-4f8f-a46a-b377778c5786	30167264	555528176	album_serdechki	2025-11-09	1599	111	5.42	6.94	602.03	6	3342.00	376.50	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
14d29585-0761-462a-81a6-034e79ca3d7d	30167264	555528176	album_serdechki	2025-11-13	903	52	5.90	5.76	307.02	10	6249.00	340.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
29e37acb-3b90-413d-9bd1-a9edbcbb35d0	29774799	556311214	альбом_вырезанное_сердце_розовый	2025-10-29	558	42	2.66	7.53	111.60	0	0.00	200.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
5748aafe-9e8c-4839-88e5-ecfdb6384c89	29774799	556311214	альбом_вырезанное_сердце_розовый	2025-11-02	183	13	2.73	7.10	35.44	0	0.00	193.66	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
4638e897-06aa-43ec-9f84-55beafb7c153	29774799	556311214	альбом_вырезанное_сердце_розовый	2025-11-01	560	33	3.34	5.89	110.06	0	0.00	196.54	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
5959735b-dfe7-41b0-86aa-2b58a42aaeb2	29774799	556311214	альбом_вырезанное_сердце_розовый	2025-11-04	1	0	\N	0.00	0.20	0	0.00	200.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
fdb1a70a-1d63-4dca-8287-0ef9b9a4ab99	29774799	556311214	альбом_вырезанное_сердце_розовый	2025-10-31	635	40	3.12	6.30	124.87	4	3375.00	196.65	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
f681c8e3-abf7-4dde-888a-0d2de9d0a95b	29774799	556311214	альбом_вырезанное_сердце_розовый	2025-10-30	570	36	3.17	6.32	114.00	0	0.00	200.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
cfdc5487-0280-47cc-88f3-ea20409893d5	29774799	556311214	альбом_вырезанное_сердце_розовый	2025-10-28	387	20	3.41	5.17	68.17	0	0.00	176.15	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
b9ad2a10-d321-4d45-94c0-7d4b199f8967	29284679	473520914	penal_black_cat	2025-10-25	2	0	\N	0.00	0.26	0	0.00	130.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
7b63189a-d295-4465-bdf7-695a3f5f9e08	29284679	473520914	penal_black_cat	2025-10-16	2	0	\N	0.00	0.26	0	0.00	130.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
4dab97a6-1470-47c3-9672-a98958d8d207	29284679	473520914	penal_black_cat	2025-10-24	2	0	\N	0.00	0.26	0	0.00	130.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
fb0495a6-afa9-4577-b6fd-62064c236e69	29284679	473520914	penal_black_cat	2025-10-31	1	0	\N	0.00	0.13	0	0.00	130.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
fc87fa3c-5fc2-4d27-a3a3-044188991f6e	29284679	473520914	penal_black_cat	2025-10-18	1	0	\N	0.00	0.13	0	0.00	130.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
c1df34c4-021e-4b90-9509-26bdf4dd8dec	29284679	473520914	penal_black_cat	2025-11-08	1	0	\N	0.00	0.13	0	0.00	130.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
e33569ef-f02c-4130-9217-d64d5569d696	29284679	473520914	penal_black_cat	2025-10-17	1	0	\N	0.00	0.13	0	0.00	130.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
a1c029ab-6e0d-4928-a627-4fc2d83efbb2	29284679	473520914	penal_black_cat	2025-10-20	4	0	\N	0.00	0.50	0	0.00	125.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
a9c0fe97-26fa-41bb-8db7-7cb2c5679a19	29284679	473520914	penal_black_cat	2025-10-15	2	0	\N	0.00	0.25	0	0.00	125.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
ee1310d7-84f0-4706-a366-c23e58e7b6f5	27461912	472859758	holder_pink	2025-11-12	1	0	\N	0.00	0.20	0	0.00	200.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
21a205eb-329d-44fd-9609-d3f557846f4c	27461912	472859758	holder_pink	2025-10-20	1	1	0.20	100.00	0.20	0	0.00	200.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
5e0aec03-fd8b-4f6f-a121-d88f3770da96	27461912	472859758	holder_pink	2025-10-19	1	0	\N	0.00	0.12	0	0.00	120.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
56906e92-35ef-4326-a820-ecb91d78c77a	27461912	472859758	holder_pink	2025-10-28	2	0	\N	0.00	0.15	0	0.00	75.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
544dccba-a2dc-41b9-9e9f-9898c402309a	29334039	467102886	korzina_tiger	2025-10-18	391	7	19.55	1.79	136.85	0	0.00	350.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
8dbee05c-2ef9-4210-94ea-841bfd8a0672	29334039	467102886	korzina_tiger	2025-10-23	1	0	\N	0.00	0.35	0	0.00	350.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
8810c03e-0a57-4966-bbab-c49c462f42da	29334039	467102886	korzina_tiger	2025-10-27	1	0	\N	0.00	0.35	0	0.00	350.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
d7789a85-cb31-4dd7-b4be-3e8a801e96e1	29334039	467102886	korzina_tiger	2025-10-19	1	0	\N	0.00	0.35	0	0.00	350.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
e7b6b721-dc2e-40f3-b4bb-47145011b291	29334039	467102886	korzina_tiger	2025-10-20	1	0	\N	0.00	0.35	0	0.00	350.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
473467bd-fe40-480b-ab2a-d127dac4b392	29334039	467102886	korzina_tiger	2025-10-17	72	3	8.40	4.17	25.20	0	0.00	350.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
a9cdef73-80a4-4969-9068-ce03518faec2	29334039	467102886	korzina_tiger	2025-11-10	1	0	\N	0.00	0.35	0	0.00	350.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
b96cf440-c551-4c35-b552-47eb274ee87f	29980106	555528179	album_glyanec_grey	2025-11-11	2	0	\N	0.00	0.47	0	0.00	235.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
19509e1c-e80e-4e31-b795-6e835fcc5791	29980106	555528179	album_glyanec_grey	2025-11-05	622	27	6.57	4.34	177.35	3	8730.00	285.13	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
32adc0dd-7a10-4b5a-bcbb-77760efba7cc	29980106	555528179	album_glyanec_grey	2025-11-04	155	6	7.49	3.87	44.95	0	0.00	290.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
ed9ee8f8-01fd-4446-951d-150c0681e7bf	29980106	555528179	album_glyanec_grey	2025-11-08	1	0	\N	0.00	0.29	0	0.00	290.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
dada00ae-0ced-4833-b77c-00d9b69c200e	29980106	555528179	album_glyanec_grey	2025-11-03	252	13	5.62	5.16	73.08	1	2910.00	290.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
6d8c7ed4-e1ff-4d71-b5fe-467a4d702e4a	29980106	555528179	album_glyanec_grey	2025-11-06	1	0	\N	0.00	0.29	0	0.00	290.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
59ba2162-4bdf-4f72-9a7f-87837b3ab8a0	28134229	467102886	korzina_tiger	2025-10-18	2544	72	5.16	2.83	371.80	2	3120.00	146.15	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
7cc2c6fb-7131-46a4-a833-8b6f440f7b1d	28134229	467102886	korzina_tiger	2025-10-15	4	2	0.38	50.00	0.76	0	0.00	190.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
ccec178b-3f66-41d4-90b3-63dd734bba79	28134229	467102886	korzina_tiger	2025-11-01	1	0	\N	0.00	0.25	0	0.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
284fbafe-740a-485b-b287-b0bc59c1c26b	28134229	467102886	korzina_tiger	2025-10-25	3	0	\N	0.00	0.49	0	0.00	163.33	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
f90eabcb-44ea-480a-9ea5-d5b48b56d9bc	28134229	467102886	korzina_tiger	2025-10-29	2	0	\N	0.00	0.24	0	0.00	120.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
83acd0ea-6f86-43be-b68a-6d2847914805	28134229	467102886	korzina_tiger	2025-10-16	1	1	0.25	100.00	0.25	1	1512.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
703d3423-1949-42f9-a66e-b9155c822319	28134229	467102886	korzina_tiger	2025-10-23	19	0	\N	0.00	2.55	0	0.00	134.21	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
1a41ea49-c8cb-4aae-85af-2fa69377a6a6	28134229	467102886	korzina_tiger	2025-10-22	33	1	3.58	3.03	3.58	1	1512.00	108.48	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
3f45952a-3398-4654-834f-e491b4a6997d	28134229	467102886	korzina_tiger	2025-10-24	5	0	\N	0.00	0.48	1	1653.00	96.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
2a4e0aa6-4958-49ce-a192-f3466322a8ce	28134229	467102886	korzina_tiger	2025-10-27	1	0	\N	0.00	0.12	0	0.00	120.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
fd5ec7be-7d22-4afb-b91c-526d7d878f1b	28134229	467102886	korzina_tiger	2025-10-30	1	0	\N	0.00	0.25	0	0.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
4e9bfa8a-ee27-4bd6-b358-52f123d8a866	28134229	467102886	korzina_tiger	2025-11-03	4	0	\N	0.00	0.74	0	0.00	185.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
ba4e1c2c-f076-46bc-a910-d0870f9670ca	28134229	467102886	korzina_tiger	2025-10-19	2528	77	3.14	3.05	241.99	3	4783.00	95.72	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
8197dcdc-701e-4449-9884-0d1e84b9e0ea	28134229	467102886	korzina_tiger	2025-10-20	2365	77	2.97	3.26	228.41	0	0.00	96.58	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
5089d5ec-d9fe-4c03-a890-813f40f9a304	28134229	467102886	korzina_tiger	2025-10-21	1864	62	3.02	3.33	187.23	1	1512.00	100.45	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
2e0f74bd-fd93-4668-98e7-8f03a1d3fb8d	28134229	467102886	korzina_tiger	2025-10-17	796	22	7.21	2.76	158.54	1	1560.00	199.17	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
dbe7e7be-91c0-48b9-9d7b-ffcaabe7d19f	28134229	467102886	korzina_tiger	2025-11-10	1	0	\N	0.00	0.12	0	0.00	120.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
73dbf0b8-a4af-4c89-b91a-4a9e419c0cff	28134229	467102886	korzina_tiger	2025-11-13	1	0	\N	0.00	0.25	0	0.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
88f00a39-4d9b-4eb1-8e67-1252b245b40e	29728784	555528176	album_serdechki	2025-11-12	11	0	\N	0.00	4.41	0	0.00	400.91	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
371f8f50-d557-474d-92d9-848545bbbacb	29728784	555528176	album_serdechki	2025-11-08	1723	83	10.65	4.82	884.14	4	2228.00	513.14	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
4074baab-68e8-405f-a93a-f99c09b2967e	29728784	555528176	album_serdechki	2025-10-26	640	30	6.24	4.69	187.35	2	1260.00	292.73	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
2a54fb8f-b6ee-40a7-b66a-6439cc174bcd	29728784	555528176	album_serdechki	2025-11-05	729	23	10.89	3.16	250.41	1	630.00	343.50	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
a92e363d-6abd-4c7b-844f-18e7010c14f5	29728784	555528176	album_serdechki	2025-11-01	903	32	8.20	3.54	262.55	0	0.00	290.75	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
104e173e-502d-4bce-8ec2-46b97cd8a75c	29728784	555528176	album_serdechki	2025-11-07	946	114	7.31	12.05	833.16	7	4035.00	880.72	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
4eeb3fa5-53d9-4931-ad76-f72f384dfe0f	29728784	555528176	album_serdechki	2025-10-29	801	43	4.66	5.37	200.25	2	3540.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
148af1e7-7e6c-491b-a97d-0301ca5dda33	29728784	555528176	album_serdechki	2025-11-09	4591	168	9.45	3.66	1587.36	8	4679.00	345.75	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
5579bdf1-f2cd-4842-9209-51fc458da7f4	29728784	555528176	album_serdechki	2025-11-02	1716	63	6.69	3.67	421.72	4	2520.00	245.76	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
281af9ec-9749-40bd-be96-848d9fe74d19	29728784	555528176	album_serdechki	2025-11-11	17	0	\N	0.00	5.89	0	0.00	346.47	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
403d8efb-a9ef-4518-a439-93348f253473	29728784	555528176	album_serdechki	2025-10-28	820	38	5.40	4.63	205.10	4	2520.00	250.12	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
adcc6585-171d-4424-a3b9-cd45199e5860	29728784	555528176	album_serdechki	2025-11-03	1454	50	6.16	3.44	307.76	1	630.00	211.66	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
ecf2d9dd-c1eb-4dc3-936b-6733a7cf61ee	29728784	555528176	album_serdechki	2025-11-06	2494	219	8.02	8.78	1757.36	14	7839.00	704.64	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
34609658-f43d-4e59-b968-cfa0c82ec260	29728784	555528176	album_serdechki	2025-10-27	709	31	5.79	4.37	179.50	3	1890.00	253.17	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
96e8a949-8c6d-41b0-ae3d-0eb2e1214946	29728784	555528176	album_serdechki	2025-10-30	862	31	6.93	3.60	214.77	2	1260.00	249.15	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
b5b9baf4-27f7-47da-82b6-41407abfe496	29728784	555528176	album_serdechki	2025-11-04	20	1	5.30	5.00	5.30	0	0.00	265.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
2d631e66-fd16-4957-a95b-22347a40f6e8	29728784	555528176	album_serdechki	2025-11-10	77	3	9.88	3.90	29.65	1	557.00	385.06	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
a00d3cd3-a351-490e-b41e-c69c2ee1aa14	29728784	555528176	album_serdechki	2025-10-31	919	36	5.98	3.92	215.17	3	1890.00	234.13	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
f3e3e69a-d093-4912-a16c-aeb24b3f3d3f	29728784	555528176	album_serdechki	2025-11-13	2	0	\N	0.00	0.66	0	0.00	330.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
86317d0d-7a07-440d-87ed-91d270f0076b	29737510	555528192	альбом_военный_ зеленый_2цвета	2025-11-04	3	1	0.60	33.33	0.60	0	0.00	200.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
f66cb322-8621-4aaa-80f9-2811fa429192	29737510	555528192	альбом_военный_ зеленый_2цвета	2025-11-01	478	32	2.98	6.69	95.41	0	0.00	199.60	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
ed734818-fc85-4266-8db4-b969a8b43f2c	29737510	555528192	альбом_военный_ зеленый_2цвета	2025-11-05	3	1	0.60	33.33	0.60	0	0.00	200.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
6facd626-28da-4020-8d75-037dc98ef97b	29737510	555528192	альбом_военный_ зеленый_2цвета	2025-10-26	2	0	\N	0.00	0.40	0	0.00	200.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
7fc24b62-0878-4abf-bc15-b4d2a5e5ec0b	29737510	555528192	альбом_военный_ зеленый_2цвета	2025-11-02	662	37	3.54	5.59	131.04	3	2700.00	197.95	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
c9ad8584-0314-4cad-87d7-2fe2d035e32d	29737510	555528192	альбом_военный_ зеленый_2цвета	2025-10-29	417	20	4.17	4.80	83.40	2	1800.00	200.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
d0b4356e-dfc4-4531-909b-d5ba7dd9d649	29737510	555528192	альбом_военный_ зеленый_2цвета	2025-11-03	638	40	3.06	6.27	122.56	1	900.00	192.10	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
a77639cc-f2ed-428f-8da2-5ba5b6addd39	29737510	555528192	альбом_военный_ зеленый_2цвета	2025-11-06	2	2	0.20	100.00	0.40	0	0.00	200.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
26df1c96-b2b7-48c6-bd38-e4dda3fc7744	29737510	555528192	альбом_военный_ зеленый_2цвета	2025-10-28	446	26	3.43	5.83	89.20	1	900.00	200.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
57f15754-3178-4d13-ad3a-ab0daa170cc1	29737510	555528192	альбом_военный_ зеленый_2цвета	2025-10-30	411	19	4.32	4.62	82.01	0	0.00	199.54	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
985152a0-819e-45c4-ba9b-cd14a4f9d2bd	29737510	555528192	альбом_военный_ зеленый_2цвета	2025-10-27	425	27	3.15	6.35	85.00	0	0.00	200.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
d4735661-5f79-4391-84ae-604e704c05c6	29737510	555528192	альбом_военный_ зеленый_2цвета	2025-10-31	481	35	2.73	7.28	95.62	0	0.00	198.79	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
bb25f3e4-f368-47ab-83d4-f83311a24e30	28123036	473520918	penal_bezh_plush	2025-10-27	1	0	\N	0.00	0.25	0	0.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
270cebe5-ecbf-4549-b510-0605732280c3	28123036	473520918	penal_bezh_plush	2025-10-30	1	0	\N	0.00	0.25	0	0.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
9990449f-7ded-414e-8ec7-e0cabdadcc5c	28123036	473520918	penal_bezh_plush	2025-11-12	1	0	\N	0.00	0.25	0	0.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
e182e9c9-b33f-48d6-8eab-e45b17752f9c	28123036	473520918	penal_bezh_plush	2025-11-07	1	0	\N	0.00	0.25	0	0.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
9b31d7ae-54fc-4950-a29d-0b6009d2d3af	29336284	473520920	penal_braun_plush	2025-10-16	6	0	\N	0.00	0.72	0	0.00	120.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
949692ee-880c-48dc-b5e1-acc0b9372bfd	29336284	473520920	penal_braun_plush	2025-10-29	1	0	\N	0.00	0.12	0	0.00	120.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
57602c78-5c5d-4ef5-8fc9-212a7ed04279	29336284	473520920	penal_braun_plush	2025-10-27	1	0	\N	0.00	0.12	0	0.00	120.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
99c90bf9-62e2-400b-9cbf-141275eae514	29336284	473520920	penal_braun_plush	2025-10-30	1	0	\N	0.00	0.12	0	0.00	120.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
b41c0a53-38c8-4aa0-bd99-f0d3b2fafd7f	29336284	473520920	penal_braun_plush	2025-10-19	1	0	\N	0.00	0.12	0	0.00	120.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
b39be0d4-3f86-49ba-beac-15aff9c8e16f	29336284	473520920	penal_braun_plush	2025-10-17	1	0	\N	0.00	0.12	0	0.00	120.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
d3b1bad8-72b5-402b-8f99-7c88275cfe89	28170874	467207049	korzina_big	2025-10-25	1	0	\N	0.00	0.25	0	0.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
80d7e106-8eec-4699-8aba-dea208365240	28170874	467207049	korzina_big	2025-11-12	1	0	\N	0.00	0.16	0	0.00	160.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
71da9186-0df6-43c2-a9cb-e083614e3182	28170874	467207049	korzina_big	2025-10-17	3	0	\N	0.00	0.65	0	0.00	216.67	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
76017c5e-6e85-4392-92a3-89421e52f205	28170874	467207049	korzina_big	2025-10-15	1	0	\N	0.00	0.25	0	0.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
eeb02fb8-09b0-4a8c-9f90-aa467a176a13	27682379	473520917	penal_white_plush	2025-10-19	2	0	\N	0.00	0.25	0	0.00	125.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
c49c95d4-86c3-4062-b43c-e3b69404cbda	27682379	473520917	penal_white_plush	2025-10-29	1	0	\N	0.00	0.11	0	0.00	110.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
62458ab7-48c7-4083-928c-df1df1b54f8a	27682379	473520917	penal_white_plush	2025-10-22	1	0	\N	0.00	0.12	0	0.00	120.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
fe952935-5110-4d37-bb67-6af039928936	27682379	473520917	penal_white_plush	2025-11-01	1	0	\N	0.00	0.12	0	0.00	120.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
13be360c-682b-4349-ba57-b62dcd4292b3	27682379	473520917	penal_white_plush	2025-10-20	2	0	\N	0.00	0.23	0	0.00	115.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
9166d4f2-fa6b-4032-a87b-070114c68028	27682379	473520917	penal_white_plush	2025-10-21	1	1	0.11	100.00	0.11	0	0.00	110.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
2267dc64-ca44-4e73-bb4b-40dd82ca7d77	27682379	473520917	penal_white_plush	2025-11-08	1	0	\N	0.00	0.12	0	0.00	120.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
08d10aae-b04e-4cfb-9993-1abc05c95196	27682379	473520917	penal_white_plush	2025-10-28	1	0	\N	0.00	0.11	0	0.00	110.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
72999e4f-7606-4747-972a-958d0a1d663a	27682379	473520917	penal_white_plush	2025-10-24	1	0	\N	0.00	0.12	0	0.00	120.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
4996df55-126a-43d5-8463-35d6fdbabc7a	27682379	473520917	penal_white_plush	2025-11-13	1	0	\N	0.00	0.11	0	0.00	110.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
ac21213b-be2d-4d67-800b-152a651d4455	27915530	473520918	penal_bezh_plush	2025-10-26	2	0	\N	0.00	0.50	0	0.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
3f4f7aa7-5f7a-40f8-8e9d-dffe4103813e	27915530	473520918	penal_bezh_plush	2025-11-08	2	0	\N	0.00	0.38	0	0.00	190.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
0198f20e-a044-4b5e-ab82-c01bd31cd3f0	27915530	473520918	penal_bezh_plush	2025-11-12	1	0	\N	0.00	0.25	0	0.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
f2fafa72-a615-4a3b-b832-604dd02d929f	27915530	473520918	penal_bezh_plush	2025-10-18	827	29	7.00	3.51	202.91	0	0.00	245.36	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
ec1672fa-540d-479b-871f-f4c1c249c1f3	27915530	473520918	penal_bezh_plush	2025-10-31	1	0	\N	0.00	0.15	0	0.00	150.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
8fe251eb-da26-4564-80af-12110d68ff43	27915530	473520918	penal_bezh_plush	2025-10-20	2	0	\N	0.00	0.38	0	0.00	190.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
37c015ef-2dfa-4e5e-94fa-a3b587f80758	27915530	473520918	penal_bezh_plush	2025-10-21	1	0	\N	0.00	0.13	0	0.00	130.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
beb64868-ecf2-42e2-b478-fcd9dad73f9e	27915530	473520918	penal_bezh_plush	2025-10-17	1	0	\N	0.00	0.25	0	0.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
061c326d-bf97-4536-90f2-431bd9777e6c	27915530	473520918	penal_bezh_plush	2025-11-04	7	0	\N	0.00	1.75	0	0.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
ad9e2bf0-a497-4d58-b449-b5c4d53d9f3c	27915530	473520918	penal_bezh_plush	2025-11-10	1	0	\N	0.00	0.35	0	0.00	350.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
d6d1edfb-b901-4a4d-8f61-61c779811c0e	27915530	473520918	penal_bezh_plush	2025-10-19	11	0	\N	0.00	2.75	0	0.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
3dc4bb1b-c5eb-4756-87ca-f8a3aeebddb8	27915530	473520918	penal_bezh_plush	2025-10-24	4	0	\N	0.00	0.89	0	0.00	222.50	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
2fe14d47-747d-4f65-b7cc-93726ba404d5	27915530	473520918	penal_bezh_plush	2025-11-03	36	0	\N	0.00	8.03	0	0.00	223.06	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
5a496252-b416-4169-9b43-eb7811e3b5a9	27915530	473520918	penal_bezh_plush	2025-11-06	2	0	\N	0.00	0.50	0	0.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
409eeaa6-8e77-4ce5-8213-5eac9a1b6f5c	27915530	473520918	penal_bezh_plush	2025-11-02	1752	43	8.20	2.45	352.42	0	0.00	201.15	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
b4c1e8a4-cc21-43b9-b61b-51e7eb305e4f	27915530	473520918	penal_bezh_plush	2025-11-11	2	0	\N	0.00	0.40	0	0.00	200.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
57bd2671-ace6-4410-8c13-35a9e161270a	27915530	473520918	penal_bezh_plush	2025-10-22	5	0	\N	0.00	1.25	0	0.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
15c65b02-3207-4b9f-a9a0-fe6e6604de4e	27915530	473520918	penal_bezh_plush	2025-11-09	2	0	\N	0.00	0.40	0	0.00	200.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
992d5b4b-9892-4b29-8bc0-3f7e9afca1a1	27915530	473520918	penal_bezh_plush	2025-10-16	2	0	\N	0.00	0.50	0	0.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
3694cc2f-4c05-44d4-b26d-139f56fee6d1	27915530	473520918	penal_bezh_plush	2025-10-25	2	0	\N	0.00	0.40	0	0.00	200.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
bc7a1e7c-ca87-4caa-b768-5fd74c0cff04	27915530	473520918	penal_bezh_plush	2025-10-23	1	0	\N	0.00	0.15	0	0.00	150.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
bee945e0-fdac-45f4-80ed-e120fae0a1c5	27915530	473520918	penal_bezh_plush	2025-11-07	3	0	\N	0.00	0.75	0	0.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
7c45d971-61ad-459b-b156-2921c9427050	27915530	473520918	penal_bezh_plush	2025-11-05	8	0	\N	0.00	2.00	0	0.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
878d1d88-40eb-483e-968d-f233376fab00	27915530	473520918	penal_bezh_plush	2025-10-15	4	0	\N	0.00	1.00	0	0.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
e665981e-92c4-449a-9287-73da8ba50ec2	27461890	472859758	holder_pink	2025-11-03	2	0	\N	0.00	0.50	0	0.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
61877004-a525-451f-8650-120c553491ec	27461890	472859758	holder_pink	2025-10-16	1	0	\N	0.00	0.25	0	0.00	250.00	2025-11-13 12:38:09.831657+00	2025-11-13 12:38:09.831657+00
\.


--
-- Data for Name: adv_params; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.adv_params (id, nm_id, vendor_code, date, views, clicks, sum, cpc, cpm, ctr, orders, orders_sum, created_at, updated_at) FROM stdin;
7905cc2a-d0ba-4030-afc5-540cb269a515	456770543	rykzak_black	2025-11-10	655	32	866.79	27.09	1323.34	4.89	2	2550.00	2025-11-13 12:36:32.441458+00	2025-11-13 12:36:32.441458+00
959e6192-d4bd-444d-8439-c41261dea603	456770543	rykzak_black	2025-10-27	21	0	9.87	\N	470.00	0.00	1	1275.00	2025-11-13 12:37:08.677956+00	2025-11-13 12:37:08.677956+00
1c30c6cf-a7e4-4146-a3b2-d27cc776dd6f	456770543	rykzak_black	2025-11-05	174	11	69.29	6.30	398.22	6.32	1	1275.00	2025-11-13 12:36:32.441458+00	2025-11-13 12:36:32.441458+00
6b290a8e-374c-480f-90b4-22f2f66179db	456770543	rykzak_black	2025-10-26	2294	103	1146.40	11.13	499.74	4.49	1	1275.00	2025-11-13 12:37:08.677956+00	2025-11-13 12:37:08.677956+00
e2354110-7615-4a2a-9000-e249bab5eb96	456770543	rykzak_black	2025-11-09	692	31	396.73	12.80	573.31	4.48	1	1388.00	2025-11-13 12:36:32.441458+00	2025-11-13 12:36:32.441458+00
3256eeac-18b5-43a2-97ef-7736cfa5f6de	456770543	rykzak_black	2025-10-28	1683	52	822.49	15.82	488.70	3.09	3	4125.00	2025-11-13 12:37:08.677956+00	2025-11-13 12:37:08.677956+00
8ef3f126-319f-4c84-a16d-6e0ed00f7853	456770543	rykzak_black	2025-10-25	2726	111	1363.13	12.28	500.05	4.07	6	8400.00	2025-11-13 12:37:08.677956+00	2025-11-13 12:37:08.677956+00
05d87ab4-55a7-446a-9710-077a7e6c0c2f	456770543	rykzak_black	2025-10-16	5090	278	2846.39	10.24	559.21	5.46	16	23332.00	2025-11-13 12:37:08.677956+00	2025-11-13 12:37:08.677956+00
4a151e63-0b30-417c-b9c5-7eb437e9ac20	456770543	rykzak_black	2025-11-11	3	0	1.53	\N	510.00	0.00	0	0.00	2025-11-13 12:37:08.677956+00	2025-11-13 12:37:08.677956+00
ee822ce2-1e9e-4a93-918c-840d33522a3c	467102886	korzina_tiger	2025-10-15	4	2	0.76	0.38	190.00	50.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
3b20e4d2-8b96-4be2-9fb5-5c223b92abf5	473520918	penal_bezh_plush	2025-10-18	827	29	202.91	7.00	245.36	3.51	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
3be4e218-2c25-409c-90d3-f1b49cdf4091	555528192	альбом_военный_ зеленый_2цвета	2025-10-31	481	35	95.62	2.73	198.79	7.28	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
0fd1d06f-e1d2-47c4-85a6-fa671af9951c	472859758	holder_pink	2025-11-03	2	0	0.50	\N	250.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
00381792-8cd0-438f-95f4-91de51b78c96	555528176	album_serdechki	2025-11-06	2494	219	1757.36	8.02	704.64	8.78	14	7839.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
cbe0c63b-5892-4bd5-bad0-2b1a99c8773e	555528192	альбом_военный_ зеленый_2цвета	2025-11-01	478	32	95.41	2.98	199.60	6.69	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
1d388eba-c47e-4093-af4c-4daf5aa39253	467207049	korzina_big	2025-10-25	1	0	0.25	\N	250.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
e6822607-5390-4922-9667-873b592acc56	555528176	album_serdechki	2025-10-29	801	43	200.25	4.66	250.00	5.37	2	3540.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
fe3010ea-4d51-4947-9ac6-3055c5de0164	456770543	rykzak_black	2025-10-19	3975	170	2272.69	13.37	571.75	4.28	4	5702.00	2025-11-13 12:37:08.677956+00	2025-11-13 12:37:08.677956+00
17f94208-ea1e-4ce3-8fb4-23c56dd38a19	467207049	korzina_big	2025-10-17	3	0	0.65	\N	216.67	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
2512a30d-8519-4ccc-9a32-41538677d1fc	467102886	korzina_tiger	2025-10-25	3	0	0.49	\N	163.33	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
1224a645-3f34-4aa3-bf62-e825c8bb8e53	555528176	album_serdechki	2025-11-09	6190	279	2189.39	7.85	353.70	4.51	14	8021.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
e763c57b-b0fa-4649-aa01-461638801d74	473520917	penal_white_plush	2025-10-28	1	0	0.11	\N	110.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
fe5eb7e0-73d8-42c4-84d3-61233da1a7e6	556311214	альбом_вырезанное_сердце_розовый	2025-10-31	635	40	124.87	3.12	196.65	6.30	4	3375.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
9bc9de73-0859-4248-8623-5e51e878d6a0	456770543	rykzak_black	2025-11-02	1868	109	1250.71	11.47	669.54	5.84	8	10200.00	2025-11-13 12:36:32.441458+00	2025-11-13 12:36:32.441458+00
3687af6c-bb95-48b3-b5cf-28192f9ebcca	473520918	penal_bezh_plush	2025-10-21	1	0	0.13	\N	130.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
4154db99-5bbc-4a3a-b5b6-4020f9530a19	555528179	album_glyanec_grey	2025-11-04	155	6	44.95	7.49	290.00	3.87	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
053a7c13-d725-43b6-910c-aaf88962df66	556311214	альбом_вырезанное_сердце_розовый	2025-10-28	387	20	68.17	3.41	176.15	5.17	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
a8f4a273-0522-4bdf-b36c-8f0948c083e5	555528176	album_serdechki	2025-11-12	3646	153	1241.98	8.12	340.64	4.20	9	5122.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
2cd92063-6408-40f7-bb82-3309c7de09d5	473520918	penal_bezh_plush	2025-10-15	4	0	1.00	\N	250.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
f7220847-40af-4fb3-adc6-94ccea423f17	555528192	альбом_военный_ зеленый_2цвета	2025-10-26	2	0	0.40	\N	200.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
3caec3f9-28a3-4d8e-96bd-bdb2cb66abd5	467102886	korzina_tiger	2025-10-17	868	25	183.74	7.35	211.68	2.88	1	1560.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
52f71ff6-7488-4a3f-9dc1-cf21aa6e7fff	472859758	holder_pink	2025-10-19	1	0	0.12	\N	120.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
db8e617e-ab93-4483-b8a2-50136b430302	473520917	penal_white_plush	2025-10-22	1	0	0.12	\N	120.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
23e55bef-ff27-40ba-9252-3bd2dcf229b1	473520918	penal_bezh_plush	2025-10-17	1	0	0.25	\N	250.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
165a92a4-3f33-4e26-8d8b-b5d8ce002644	473520914	penal_black_cat	2025-10-31	1	0	0.13	\N	130.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
dd96364e-577d-4886-b175-8aec039560da	467102886	korzina_tiger	2025-10-29	2	0	0.24	\N	120.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
382665be-5c6c-40f5-be0f-e38a5bb12b89	456770543	rykzak_black	2025-10-30	14	0	5.40	\N	385.71	0.00	1	1275.00	2025-11-13 12:37:08.677956+00	2025-11-13 12:37:08.677956+00
1a470aee-9c09-4dc7-84e5-52ca5aef6358	556311214	альбом_вырезанное_сердце_розовый	2025-11-02	183	13	35.44	2.73	193.66	7.10	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
36c19fc6-0c8e-4555-98fd-12c1d21fbbd6	456770543	rykzak_black	2025-11-08	278	22	149.91	6.81	539.24	7.91	4	5100.00	2025-11-13 12:36:32.441458+00	2025-11-13 12:36:32.441458+00
5ebecb3b-ffd0-4706-ac48-7b534ba0c66d	555528176	album_serdechki	2025-10-30	862	31	214.77	6.93	249.15	3.60	2	1260.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
4653f249-99e9-4e48-8eda-4deaf01afd4f	472859758	holder_pink	2025-10-16	1	0	0.25	\N	250.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
94221acd-2b1c-40c5-847b-0a4f69631c7d	467102886	korzina_tiger	2025-10-30	1	0	0.25	\N	250.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
bdcf3261-9cc2-4f8f-a228-0e58ff515c34	456770543	rykzak_black	2025-10-20	4912	236	2700.46	11.44	549.77	4.80	9	12505.00	2025-11-13 12:37:08.677956+00	2025-11-13 12:37:08.677956+00
2c0f4318-4a30-4736-beec-1dad6e5f5bb9	473520918	penal_bezh_plush	2025-11-03	36	0	8.03	\N	223.06	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
bbf44017-ace2-4487-8189-7fac08ed8f37	473520918	penal_bezh_plush	2025-10-31	1	0	0.15	\N	150.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
57eb229b-32d2-4065-a632-b6bf0c2bdb85	467102886	korzina_tiger	2025-10-20	2366	77	228.76	2.97	96.69	3.25	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
65489c47-2f3b-4eea-9cb8-4be22f4f1b8f	555528176	album_serdechki	2025-11-13	905	52	307.68	5.92	339.98	5.75	10	6249.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
1fff9df1-2990-495a-ae53-dc945eb6f9b8	467102886	korzina_tiger	2025-10-22	33	1	3.58	3.58	108.48	3.03	1	1512.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
f4f59691-8bb3-4458-b170-55d1da78263d	467102886	korzina_tiger	2025-10-23	20	0	2.90	\N	145.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
d232d538-b606-4a8d-9b03-eedd4c774da9	473520914	penal_black_cat	2025-10-25	2	0	0.26	\N	130.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
f8b2fdb8-5882-4d77-81de-7b7aebc4ec7d	473520914	penal_black_cat	2025-10-15	2	0	0.25	\N	125.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
f1050c17-f9e6-40cd-a200-46d371eae2de	555528192	альбом_военный_ зеленый_2цвета	2025-10-28	446	26	89.20	3.43	200.00	5.83	1	900.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
13146159-cba0-42a1-9bcd-659c181a1f22	473520917	penal_white_plush	2025-10-24	1	0	0.12	\N	120.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
f59a946e-979a-45b5-95a5-563265df651d	472859758	holder_pink	2025-10-20	1	1	0.20	0.20	200.00	100.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
baf372fe-0d76-48d8-8d48-4bde457eb068	456770543	rykzak_black	2025-11-01	2153	107	1291.68	12.07	599.94	4.97	7	9225.00	2025-11-13 12:36:32.441458+00	2025-11-13 12:36:32.441458+00
20025ca0-c06a-42b4-b080-c881e12195b8	456770543	rykzak_black	2025-11-03	26	0	9.36	\N	360.00	0.00	1	1275.00	2025-11-13 12:36:32.441458+00	2025-11-13 12:36:32.441458+00
467e4c3a-e84b-4974-a0e7-bf3c63b16f29	473520918	penal_bezh_plush	2025-11-11	2	0	0.40	\N	200.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
2028b733-ac47-431f-8bfd-342ae060b293	473520918	penal_bezh_plush	2025-10-30	1	0	0.25	\N	250.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
294072e5-b5f9-4673-b923-528b73a13ddf	467102886	korzina_tiger	2025-10-21	1864	62	187.23	3.02	100.45	3.33	1	1512.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
8b48d197-a2ed-4ea7-91a3-9456eb35eb85	473520918	penal_bezh_plush	2025-10-26	2	0	0.50	\N	250.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
b33ab362-5a0c-4f40-b4a6-f23692528609	555528176	album_serdechki	2025-10-27	709	31	179.50	5.79	253.17	4.37	3	1890.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
c0662a0d-1199-43f7-8459-2c6b0fd7d371	555528179	album_glyanec_grey	2025-11-06	1	0	0.29	\N	290.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
e09715f2-cfe0-4dfc-a5da-e7250b253c12	473520917	penal_white_plush	2025-10-19	2	0	0.25	\N	125.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
1b401715-039f-44b4-a726-5ee119c027ef	456770543	rykzak_black	2025-10-15	4240	221	2382.48	10.78	561.91	5.21	7	10127.00	2025-11-13 12:37:08.677956+00	2025-11-13 12:37:08.677956+00
0ccf927c-9f65-4ab0-aa49-582a408becee	555528192	альбом_военный_ зеленый_2цвета	2025-11-06	2	2	0.40	0.20	200.00	100.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
958c2d2a-d04e-42e0-9a41-3ee09d9d2aa8	467102886	korzina_tiger	2025-10-24	5	0	0.48	\N	96.00	0.00	1	1653.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
1fa257bf-0099-4dc6-8904-93784b2caddb	456770543	rykzak_black	2025-10-17	4415	229	2418.45	10.56	547.78	5.19	10	14065.00	2025-11-13 12:37:08.677956+00	2025-11-13 12:37:08.677956+00
7ea17bac-b892-4fe2-89d8-7eeee1ab46b3	467102886	korzina_tiger	2025-10-19	2529	77	242.34	3.15	95.82	3.04	3	4783.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
e9eb9d61-d89c-4fe1-87b5-61a3a65041cf	555528179	album_glyanec_grey	2025-11-05	622	27	177.35	6.57	285.13	4.34	3	8730.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
fb1562ef-1533-4797-9425-9c907d704894	467102886	korzina_tiger	2025-11-13	1	0	0.25	\N	250.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
7c438e12-4991-49ee-b19b-cfc8758664dd	456770543	rykzak_black	2025-11-04	672	48	459.81	9.58	684.24	7.14	0	0.00	2025-11-13 12:36:32.441458+00	2025-11-13 12:36:32.441458+00
d149e057-738d-4c11-9b60-6b74b696d7a5	473520918	penal_bezh_plush	2025-11-04	7	0	1.75	\N	250.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
1043189a-4d20-4284-b54a-c7ec01a3cd6a	473520914	penal_black_cat	2025-10-20	4	0	0.50	\N	125.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
e6fb6a8a-6be2-499f-bfeb-462b807f8573	467102886	korzina_tiger	2025-11-01	1	0	0.25	\N	250.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
99be4601-55e4-41d4-b343-7f4f4048f07d	473520920	penal_braun_plush	2025-10-27	1	0	0.12	\N	120.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
bae89858-6c35-4cf0-aaef-45a92d879e12	473520914	penal_black_cat	2025-10-17	1	0	0.13	\N	130.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
f2c4658c-f205-4494-9492-76d7d1cdee18	473520917	penal_white_plush	2025-11-01	1	0	0.12	\N	120.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
e338d047-1749-42cc-a738-e7c9a4288834	456770543	rykzak_black	2025-10-22	4157	161	2283.79	14.19	549.38	3.87	8	10876.00	2025-11-13 12:37:08.677956+00	2025-11-13 12:37:08.677956+00
c288495f-c160-4872-a492-82591fbc253d	473520918	penal_bezh_plush	2025-11-06	2	0	0.50	\N	250.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
0527b1a1-21d6-4ab5-8acd-a53dba444513	467207049	korzina_big	2025-10-15	1	0	0.25	\N	250.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
1501c1ea-2c4a-4118-aa8d-a1090660c50f	472859758	holder_pink	2025-10-28	2	0	0.15	\N	75.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
0936ac10-3e5a-4e4d-ae69-5146fc79691c	456770543	rykzak_black	2025-11-12	1	0	0.45	\N	450.00	0.00	0	0.00	2025-11-13 12:37:08.677956+00	2025-11-13 12:37:08.677956+00
565ef031-88ef-451d-a549-486da7689c4a	472859758	holder_pink	2025-11-12	1	0	0.20	\N	200.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
7b44d9ca-93d6-4fdc-89d2-35aaec943718	456770543	rykzak_black	2025-10-21	3956	189	2175.34	11.51	549.88	4.78	4	5565.00	2025-11-13 12:37:08.677956+00	2025-11-13 12:37:08.677956+00
1625aaf3-c592-4582-9cff-bd495c64be55	456770543	rykzak_black	2025-11-07	9	0	4.88	\N	542.22	0.00	1	1275.00	2025-11-13 12:36:32.441458+00	2025-11-13 12:36:32.441458+00
e952c683-8a21-474f-8d82-b9f5097b202d	473520920	penal_braun_plush	2025-10-30	1	0	0.12	\N	120.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
5847af0d-9794-4d29-83bc-c79e9b6d99a3	555528192	альбом_военный_ зеленый_2цвета	2025-11-04	3	1	0.60	0.60	200.00	33.33	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
79ac2682-c7a3-4b33-9b69-aa63bc2877c2	555528192	альбом_военный_ зеленый_2цвета	2025-11-03	638	40	122.56	3.06	192.10	6.27	1	900.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
c11b5621-6893-4bf3-8a13-5a35d54de522	473520918	penal_bezh_plush	2025-11-08	2	0	0.38	\N	190.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
94d2f58c-c191-4756-b04f-d46160bbb5c4	556311214	альбом_вырезанное_сердце_розовый	2025-10-29	558	42	111.60	2.66	200.00	7.53	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
6231924a-b24a-4ef6-8164-88d6f2d908c5	473520918	penal_bezh_plush	2025-10-24	4	0	0.89	\N	222.50	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
f579aaaf-c12f-4ec2-8dfb-47f9e6d51030	556311214	альбом_вырезанное_сердце_розовый	2025-11-04	1	0	0.20	\N	200.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
0403d8c9-0dba-468b-bf3b-ff4c1a9bbf5a	473520914	penal_black_cat	2025-10-18	1	0	0.13	\N	130.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
c5b382e8-b2de-4e73-b24d-b2de771a6a40	555528176	album_serdechki	2025-11-05	729	23	250.41	10.89	343.50	3.16	1	630.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
402b99ac-8e63-4529-862c-e5a06cc3ee09	555528176	album_serdechki	2025-10-28	820	38	205.10	5.40	250.12	4.63	4	2520.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
42ce9682-1ba3-4c36-9107-ae19213b8b97	555528179	album_glyanec_grey	2025-11-11	2	0	0.47	\N	235.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
ff6a4b1a-2515-4912-9298-a37f372641c4	456770543	rykzak_black	2025-10-18	3898	184	2212.69	12.03	567.65	4.72	8	11104.00	2025-11-13 12:37:08.677956+00	2025-11-13 12:37:08.677956+00
02294c77-d348-4cca-9540-067333ebda88	473520918	penal_bezh_plush	2025-10-27	1	0	0.25	\N	250.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
f3aa82d9-12a8-46d2-b7ee-134841b2f70f	456770543	rykzak_black	2025-10-31	2855	78	733.34	9.40	256.86	2.73	2	2850.00	2025-11-13 12:37:08.677956+00	2025-11-13 12:37:08.677956+00
41373d21-fe69-49a3-8a43-e23a9972453b	555528176	album_serdechki	2025-11-10	4193	379	2161.77	5.70	515.57	9.04	16	10282.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
5ed38042-311a-45e7-b11f-6c979f92b9a7	473520918	penal_bezh_plush	2025-10-20	2	0	0.38	\N	190.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
53e12e8f-dc7c-4cd0-b5ad-fdf0eeb91ffd	473520918	penal_bezh_plush	2025-11-05	8	0	2.00	\N	250.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
afa81a4f-70b6-47dc-8ce2-85853d8edf28	473520917	penal_white_plush	2025-10-21	1	1	0.11	0.11	110.00	100.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
01cab7cb-0eaa-4a2b-94d2-02758a9b4367	467102886	korzina_tiger	2025-10-16	1	1	0.25	0.25	250.00	100.00	1	1512.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
f04fe0a9-3e13-4139-9b71-27e9463f88f5	555528176	album_serdechki	2025-10-26	640	30	187.35	6.25	292.73	4.69	2	1260.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
6521c826-26ae-4c21-9bea-da9d50065633	473520918	penal_bezh_plush	2025-10-16	2	0	0.50	\N	250.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
a26b259c-155d-426e-a111-190a5df6c85a	555528192	альбом_военный_ зеленый_2цвета	2025-11-05	3	1	0.60	0.60	200.00	33.33	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
afefeeb3-1c38-486c-8a0b-80d06325c02a	555528176	album_serdechki	2025-11-02	1716	63	421.72	6.69	245.76	3.67	4	2520.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
1fbac8c3-9bb4-49b2-b2bb-a3c564e22954	473520920	penal_braun_plush	2025-10-29	1	0	0.12	\N	120.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
f29e7029-9407-4075-ae56-42bf79a1f4e1	555528176	album_serdechki	2025-11-11	3979	245	1754.53	7.16	440.95	6.16	25	14005.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
a24dd029-99ca-4308-87d9-0b0cbe37d76a	555528176	album_serdechki	2025-11-01	903	32	262.55	8.20	290.75	3.54	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
292437d6-35da-46f8-ae49-3a89f7937036	555528192	альбом_военный_ зеленый_2цвета	2025-11-02	662	37	131.04	3.54	197.95	5.59	3	2700.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
92d1d8e0-23f4-41b6-b9da-1fbba1f919bb	473520918	penal_bezh_plush	2025-11-09	2	0	0.40	\N	200.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
59cad4a1-d1fe-4a42-9a6d-a1db3d2c546a	473520918	penal_bezh_plush	2025-10-23	1	0	0.15	\N	150.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
581b1342-56b0-4b38-830c-835c52308143	456770543	rykzak_black	2025-11-13	3	1	2.41	2.41	803.33	33.33	0	0.00	2025-11-13 12:37:08.677956+00	2025-11-13 12:37:08.677956+00
3b711e2d-9c55-4b00-a704-6cf8c8df78f9	555528176	album_serdechki	2025-11-07	946	114	833.16	7.31	880.72	12.05	7	4035.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
4b3affc8-062e-487c-83c1-04dcbec64872	467102886	korzina_tiger	2025-10-27	2	0	0.47	\N	235.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
66994e8f-31eb-4142-beb2-9c2b2cb69dd9	473520914	penal_black_cat	2025-10-16	2	0	0.26	\N	130.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
cab62847-b72e-49d0-ba19-258dfa5f9aad	473520917	penal_white_plush	2025-11-13	1	0	0.11	\N	110.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
fa7e087c-ac99-41a0-a882-a9239aa33412	473520917	penal_white_plush	2025-10-20	2	0	0.23	\N	115.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
d0505946-2040-4286-8df9-843a6d0f943e	555528192	альбом_военный_ зеленый_2цвета	2025-10-27	425	27	85.00	3.15	200.00	6.35	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
93560c77-8d98-4a17-a422-afecb2bfd679	473520917	penal_white_plush	2025-11-08	1	0	0.12	\N	120.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
f68e2b1c-2872-40b4-94c2-df8144353e7a	473520918	penal_bezh_plush	2025-11-07	4	0	1.00	\N	250.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
778095a5-7735-49f4-a592-a82470ddd3d9	473520918	penal_bezh_plush	2025-11-12	2	0	0.50	\N	250.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
b5423fcf-5183-4fd1-b68e-2c09c565fa9f	473520920	penal_braun_plush	2025-10-17	1	0	0.12	\N	120.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
e455ad0e-d59d-43cc-a7df-0e7fb7807178	473520918	penal_bezh_plush	2025-10-25	2	0	0.40	\N	200.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
a78dfc96-4612-4589-9d3f-98ea6262724a	467102886	korzina_tiger	2025-11-03	4	0	0.74	\N	185.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
ca500c86-06a6-49fb-aa9b-a249d64f000f	555528176	album_serdechki	2025-11-08	2515	127	1118.98	8.81	444.92	5.05	9	5013.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
57bd7316-398d-44cd-a63f-ae7533821965	556311214	альбом_вырезанное_сердце_розовый	2025-11-01	560	33	110.06	3.34	196.54	5.89	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
d8a447f5-740b-40f8-8554-693e86b28274	456770543	rykzak_black	2025-11-06	6	0	2.25	\N	375.00	0.00	0	0.00	2025-11-13 12:36:32.441458+00	2025-11-13 12:36:32.441458+00
6ff4ae39-1c14-4b61-8328-63bb316b5af1	456770543	rykzak_black	2025-10-24	3582	142	1973.97	13.90	551.08	3.96	8	11400.00	2025-11-13 12:37:08.677956+00	2025-11-13 12:37:08.677956+00
7c27ed9a-db94-41f8-b899-5ba64c5479d5	556311214	альбом_вырезанное_сердце_розовый	2025-10-30	570	36	114.00	3.17	200.00	6.32	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
e072480e-0bf7-42fa-bbc4-3cb95ae0298d	473520918	penal_bezh_plush	2025-11-02	1752	43	352.42	8.20	201.15	2.45	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
4f48f707-c8fb-44d5-87ee-d005bc283000	555528176	album_serdechki	2025-11-03	1454	50	307.76	6.16	211.66	3.44	1	630.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
8dfb7753-7222-4939-89ec-3c28a3d7072d	473520920	penal_braun_plush	2025-10-16	6	0	0.72	\N	120.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
3b58efcc-93c7-4bd1-8d2c-54b1287548a6	473520918	penal_bezh_plush	2025-11-10	1	0	0.35	\N	350.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
6fea8dbe-ed0e-4801-9dce-e94727025389	473520914	penal_black_cat	2025-11-08	1	0	0.13	\N	130.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
d13bf0f8-ebd1-4143-af40-ffc27280170d	473520918	penal_bezh_plush	2025-10-22	5	0	1.25	\N	250.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
558033a4-eeb0-4022-a445-c0fc58e54f05	473520918	penal_bezh_plush	2025-10-19	11	0	2.75	\N	250.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
19b92048-02c6-49e0-8954-71ea3749f9a3	473520914	penal_black_cat	2025-10-24	2	0	0.26	\N	130.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
c6881ac4-2a23-471f-930d-3f8c0de17bc2	555528192	альбом_военный_ зеленый_2цвета	2025-10-29	417	20	83.40	4.17	200.00	4.80	2	1800.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
db659b6e-1aeb-437c-82a3-5a2af0dbc86a	467102886	korzina_tiger	2025-11-10	2	0	0.47	\N	235.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
987f88b0-78a5-4608-838d-a698c88a9bb2	555528176	album_serdechki	2025-10-31	919	36	215.17	5.98	234.13	3.92	3	1890.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
1925f897-e7b9-4f4a-b30f-d0c609867254	473520917	penal_white_plush	2025-10-29	1	0	0.11	\N	110.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
e75f5edb-0693-4d29-8639-902618f03cbc	473520920	penal_braun_plush	2025-10-19	1	0	0.12	\N	120.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
9e2ef743-47d1-45bb-bcd8-5ea5cbb612e4	467207049	korzina_big	2025-11-12	1	0	0.16	\N	160.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
0ef8732a-050a-4d22-a47b-6208f1b10b38	555528179	album_glyanec_grey	2025-11-08	1	0	0.29	\N	290.00	0.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
dff3139f-c482-4445-af41-eb8b598841d3	456770543	rykzak_black	2025-10-29	157	2	42.38	21.19	269.94	1.27	1	1275.00	2025-11-13 12:37:08.677956+00	2025-11-13 12:37:08.677956+00
1dc5643c-ba78-45a3-bc97-3572976ee6db	467102886	korzina_tiger	2025-10-18	2935	79	508.65	6.44	173.30	2.69	2	3120.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
7f9001ed-94bd-45b2-9dd7-ff4f8e3b2dbc	555528192	альбом_военный_ зеленый_2цвета	2025-10-30	411	19	82.01	4.32	199.54	4.62	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
6d2fe18e-9555-4027-8cda-09e7924be8f0	555528176	album_serdechki	2025-11-04	20	1	5.30	5.30	265.00	5.00	0	0.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
55bb9cfc-6cb8-4b89-a928-fba4ed283090	555528179	album_glyanec_grey	2025-11-03	252	13	73.08	5.62	290.00	5.16	1	2910.00	2025-11-13 12:38:09.861696+00	2025-11-13 12:38:09.861696+00
48d83e86-1f6a-43e8-b8fa-29b62e1bf0ef	456770543	rykzak_black	2025-10-23	4092	204	2249.22	11.03	549.66	4.99	11	15305.00	2025-11-13 12:37:08.677956+00	2025-11-13 12:37:08.677956+00
\.


--
-- Data for Name: cost_price; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.cost_price (id, product_id, nm_id, vendor_code, date, cost_price) FROM stdin;
\.


--
-- Data for Name: cr_daily_stats; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.cr_daily_stats (id, product_id, nm_id, vendor_code, date_of_period, open_card_count, add_to_cart_count, orders_count, cancel_count, orders_sum_rub, stocks_mp, stocks_wb, add_to_cart_percent, cart_to_order_percent, order_price, created_at, updated_at) FROM stdin;
f26e88a6-89d3-4710-b1d2-3c61b9ff1644	09c49f5f-0130-4bad-8865-ae4236ce00ca	456770543	rykzak_black	2025-11-13	253	37	17	0	21674.00	\N	\N	15.00	46.00	1274.94	2025-11-13 12:05:17.329912+00	2025-11-13 12:05:17.329912+00
e3b66ad7-0f94-441b-a707-b1f4b4ef2957	09c49f5f-0130-4bad-8865-ae4236ce00ca	456770543	rykzak_black	2025-11-12	585	82	28	0	35701.00	\N	\N	14.00	34.00	1275.04	2025-11-13 12:05:17.336355+00	2025-11-13 12:05:17.336355+00
123bf475-14e5-4aea-92a8-99e643f7fa83	09c49f5f-0130-4bad-8865-ae4236ce00ca	456770543	rykzak_black	2025-11-11	724	112	33	2	42075.00	\N	\N	15.00	29.00	1275.00	2025-11-13 12:40:16.270818+00	2025-11-13 12:40:16.270818+00
ca0eac63-4547-4221-b507-1695f19dc039	09c49f5f-0130-4bad-8865-ae4236ce00ca	456770543	rykzak_black	2025-11-10	702	76	33	3	42379.00	\N	\N	11.00	43.00	1284.21	2025-11-13 12:40:16.282311+00	2025-11-13 12:40:16.282311+00
0a4a6ab1-75a2-45a2-addb-8690c705fc95	09c49f5f-0130-4bad-8865-ae4236ce00ca	456770543	rykzak_black	2025-11-09	652	96	30	2	38363.00	\N	\N	15.00	31.00	1278.77	2025-11-13 12:40:28.411323+00	2025-11-13 12:40:28.411323+00
df96bfaa-09cb-416f-838c-5404c9a1b8cc	09c49f5f-0130-4bad-8865-ae4236ce00ca	456770543	rykzak_black	2025-11-08	501	87	32	4	40800.00	\N	\N	17.00	37.00	1275.00	2025-11-13 12:40:28.417934+00	2025-11-13 12:40:28.417934+00
856092ec-b825-47d2-9b5c-7218667090f2	09c49f5f-0130-4bad-8865-ae4236ce00ca	456770543	rykzak_black	2025-11-07	494	78	26	5	33149.00	\N	\N	16.00	33.00	1274.96	2025-11-13 12:40:33.603397+00	2025-11-13 12:40:33.603397+00
46549ac2-c996-46d0-bd76-bd78519146df	09c49f5f-0130-4bad-8865-ae4236ce00ca	456770543	rykzak_black	2025-11-06	373	54	17	4	21675.00	\N	\N	14.00	31.00	1275.00	2025-11-13 12:40:33.608353+00	2025-11-13 12:40:33.608353+00
56171f29-7002-4457-9748-f2b4139b4860	09c49f5f-0130-4bad-8865-ae4236ce00ca	456770543	rykzak_black	2025-11-05	245	31	14	2	17850.00	\N	\N	13.00	45.00	1275.00	2025-11-13 12:40:39.021028+00	2025-11-13 12:40:39.021028+00
aa65ceee-6b11-4c23-9095-9dbf557cb5c4	09c49f5f-0130-4bad-8865-ae4236ce00ca	456770543	rykzak_black	2025-11-04	284	29	6	2	7650.00	\N	\N	10.00	21.00	1275.00	2025-11-13 12:40:39.025922+00	2025-11-13 12:40:58.78742+00
566b1841-8cac-4afe-8751-49a3364734c1	09c49f5f-0130-4bad-8865-ae4236ce00ca	456770543	rykzak_black	2025-11-03	247	31	13	4	16575.00	\N	\N	13.00	42.00	1275.00	2025-11-13 12:40:58.794671+00	2025-11-13 12:40:58.794671+00
fc973513-a8ee-4d63-8c3e-e9ebc63c7ac3	09c49f5f-0130-4bad-8865-ae4236ce00ca	456770543	rykzak_black	2025-11-02	313	41	21	3	26775.00	\N	\N	13.00	51.00	1275.00	2025-11-13 12:41:21.490531+00	2025-11-13 12:41:21.490531+00
41a1a50f-91e5-4357-9aa1-215cfc3f045c	09c49f5f-0130-4bad-8865-ae4236ce00ca	456770543	rykzak_black	2025-11-01	216	22	9	1	12074.00	\N	\N	10.00	41.00	1341.56	2025-11-13 12:41:21.495883+00	2025-11-13 12:41:21.495883+00
2028e36b-1baa-4967-8e5e-e428c21628ba	09c49f5f-0130-4bad-8865-ae4236ce00ca	456770543	rykzak_black	2025-10-31	171	18	5	2	7125.00	\N	\N	11.00	28.00	1425.00	2025-11-13 12:41:37.692213+00	2025-11-13 12:41:37.692213+00
99ddbf71-7e87-4952-a6ed-32b66d4f5a0e	09c49f5f-0130-4bad-8865-ae4236ce00ca	456770543	rykzak_black	2025-10-30	207	18	3	1	3825.00	\N	\N	9.00	17.00	1275.00	2025-11-13 12:41:37.706313+00	2025-11-13 12:41:37.706313+00
e9aedd30-88bf-440a-a7a9-8dfbb9834f1f	09c49f5f-0130-4bad-8865-ae4236ce00ca	456770543	rykzak_black	2025-10-29	297	28	15	6	19125.00	\N	\N	9.00	54.00	1275.00	2025-11-13 12:41:59.850868+00	2025-11-13 12:41:59.850868+00
cc7544ea-36e0-43fa-9a3b-119cbb221bcf	09c49f5f-0130-4bad-8865-ae4236ce00ca	456770543	rykzak_black	2025-10-28	252	25	8	2	10650.00	\N	\N	10.00	32.00	1331.25	2025-11-13 12:41:59.862628+00	2025-11-13 12:41:59.862628+00
e24d23f6-d30c-4ca3-9c18-7e1e6c47006a	09c49f5f-0130-4bad-8865-ae4236ce00ca	456770543	rykzak_black	2025-10-27	419	57	14	9	17850.00	\N	\N	14.00	25.00	1275.00	2025-11-13 12:42:20.163315+00	2025-11-13 12:42:20.163315+00
adb1f592-a1c6-4b78-8d07-de7300890ac4	09c49f5f-0130-4bad-8865-ae4236ce00ca	456770543	rykzak_black	2025-10-26	349	27	6	1	8100.00	\N	\N	8.00	22.00	1350.00	2025-11-13 12:42:20.171324+00	2025-11-13 12:42:20.171324+00
e61fb0d6-f093-4f41-9ca7-c383934feb87	09c49f5f-0130-4bad-8865-ae4236ce00ca	456770543	rykzak_black	2025-10-25	297	30	12	1	16500.00	\N	\N	10.00	40.00	1375.00	2025-11-13 12:42:43.722726+00	2025-11-13 12:42:43.722726+00
78ffc3ba-1305-4277-b054-a48fbef8e8d9	09c49f5f-0130-4bad-8865-ae4236ce00ca	456770543	rykzak_black	2025-10-24	309	32	15	5	21375.00	\N	\N	10.00	47.00	1425.00	2025-11-13 12:42:43.733347+00	2025-11-13 12:42:43.733347+00
34c8a685-6a57-49cf-bcc1-9cbe728d7d66	675a4e11-aafc-4d40-a112-4b4ec05e0411	467207049	korzina_big	2025-10-25	158	11	0	0	0.00	\N	\N	7.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
d8bb7abf-3f60-47be-bba7-e4c4979212df	04d8589f-d352-4c7c-893a-6e2d41c567dd	507758935	korzina_svetlaya	2025-10-25	139	10	3	0	5075.00	\N	\N	7.00	30.00	1691.67	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
5f7afcb4-05bf-4e9b-8573-ab4c9f66aae9	68b4da00-36ec-4cea-a8ff-367f1d27604c	467102886	korzina_tiger	2025-10-25	115	8	3	0	4608.00	\N	\N	7.00	38.00	1536.00	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
4a8c8c0e-2037-48e1-81ae-63c5c964683f	855090a0-7e1a-45c1-8403-fe70a7c64942	473520920	penal_braun_plush	2025-10-25	40	8	0	0	0.00	\N	\N	20.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
c53cefcc-a945-49e9-84e9-873d80ef9f3c	f2779a56-27de-4904-84db-ff5f25f3110d	473520918	penal_bezh_plush	2025-10-25	18	4	0	0	0.00	\N	\N	22.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
e6380ab9-cc1b-47b5-881f-21c3cedae066	a89f8530-97cc-49d5-89b3-dd37c5370f64	467102887	korzina_mini	2025-10-25	13	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
88f059bc-8058-4eac-9ad4-e5f393bb2ed2	4997211b-836f-4450-be30-b1c5f0449ac3	467207051	korzina_grey	2025-10-25	12	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
0ffbfcb4-b7f1-4532-9754-28c14753977f	829b0392-47e7-44d7-a671-cd419b25d2b1	467207055	korzina_pink	2025-10-25	11	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
aae59a87-ff54-4124-9eab-dc566eb5a448	61d3359d-3112-42fb-9685-73c39b74dc92	556953442	альбом_военный _зеленый_4цвета	2025-10-25	9	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
75f99cd7-6f50-4161-9af5-d8cac0c51a19	622ab6c1-6d24-44c7-90a5-1a0d66d33768	473520917	penal_white_plush	2025-10-25	7	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
828cbdce-1fcd-4b94-a8cb-0fcbb6ee159e	a6b9bc34-aa25-4f84-aa91-f1d303c8643b	473520914	penal_black_cat	2025-10-25	6	1	0	0	0.00	\N	\N	17.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
0f44af75-a4e5-4c11-9041-9cb1ac8a4a54	8dda1b27-c73e-4a12-a3ea-e36ffae2c0b8	473520919	penal_black_plush	2025-10-25	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
63fd6ad2-4764-4acd-8825-239240809f7e	d28d47c9-76eb-406d-af4a-17fbb8066281	473520915	penal_blue_cat	2025-10-25	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
84deb2f7-52b1-4c72-8c0e-cb930abe79bf	90621643-12da-46d9-b967-8abc695e5652	473520916	penal_pink_cat	2025-10-25	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
eba91ebb-00fa-4803-809d-5d101c0aa936	686420b8-63de-4638-abaf-cc89e4604bd2	555528192	альбом_военный_ зеленый_2цвета	2025-10-25	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
fc2aaf69-17d5-4dcb-b058-3b7f0f5e0e47	2ca071cb-1e7f-4a50-aebf-307d36c4e3c3	556902796	альбом_вырезанное_ сердце_ голубой	2025-10-25	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
bbecb1d6-8bfc-489b-b5f4-872472cfa050	b0361398-d919-47ba-8d2c-c8ba8dc852c8	555528182	album_wave_old	2025-10-25	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
be4a9038-5a8b-49f0-8099-5b72ede45c6b	510f2e6e-1486-4082-91a2-154eddcd30a5	556902797	альбом_вырезанное_сердце_желтый	2025-10-25	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
7ca82153-4bd0-487b-9538-f95603594f0b	2e67e4ac-1929-40a1-9f86-2c6dc73d548d	558118816	альбом_военный _коричневый	2025-10-25	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
8e079cc1-459c-432b-9cd7-a1415a3f6fe4	1a6edc21-1692-40ab-9f25-e01dd782082d	558118820	album_grey_heart	2025-10-25	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
dd5de9fd-e9ae-4905-81a0-2b1f2042867b	98319b3c-67ff-44e7-b4fc-deb47179c6d7	558118822	album_big_hearts	2025-10-25	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
8770209f-1788-49d3-97b5-df4a71a0edb6	aa9526c1-47a1-4cda-8ba3-1ac82917018a	555528176	album_serdechki	2025-10-25	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
e1f5cf98-09d5-4280-bc5a-4afb5c85605c	2dae1d26-2788-4a1f-b439-18510a6d45de	555528179	album_glyanec_grey	2025-10-25	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
731b614c-1457-49b8-8df8-84d263e90daa	ca604ed0-475c-4d42-90b5-6b62234e0e17	555528188	album_leaves	2025-10-25	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
2e970dae-2562-4565-93f1-24ad547de4dd	ba729003-0857-4378-91eb-7712858964b6	556311214	альбом_вырезанное_сердце_розовый	2025-10-25	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
7e0a1de0-552a-4435-bd55-d15f888a93ce	d3ed53c9-4b5d-4dd1-9647-f5369b4105de	556311217	альбом_папоротник_Биг	2025-10-25	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
5db2a432-a5db-4b48-9c39-8b771510b19d	1e009158-8d26-4300-8f5a-cd5c53825801	558118815	альбом _военный_краснопесчаный	2025-10-25	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
f1ca45b6-7be8-4354-a8eb-139587f85280	8db391e6-9ed3-489d-9cf9-ec1cbd0be071	555528177	album_big_hearts_Big	2025-10-25	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
bcea1dbb-f68e-4e5b-b828-569a1dc4f8a8	1e4d4fe5-14df-45a2-8d2e-bf07c078cbab	555528178	album_vetochki	2025-10-25	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
0a297058-d71c-4ddc-b9c7-f62e4139179d	bbab353e-6266-44fc-b2b5-fe615d67c650	556311216	альбом_лен_голубой	2025-10-25	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
79f9ba6d-3168-41e7-8dc3-c188348b0fea	6afe8407-24e1-4ae2-b973-859cdb04a549	558118817	album_kaktus	2025-10-25	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
9ffa54f6-51a0-47d3-9e48-77e655e3fe47	31f0df7a-98a3-4c01-a5a5-f73fc359bd28	558118819	альбом_веточки_Биг	2025-10-25	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
6e652ab6-4497-4127-8011-6eb1c0e3d8a8	707722f9-8229-4dc2-ba2a-82dac805e6a4	558118821	album_glyanec_pink	2025-10-25	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.313731+00	2025-11-13 12:42:50.313731+00
bd8dc4a6-a089-4b35-8b65-3015cac2682c	675a4e11-aafc-4d40-a112-4b4ec05e0411	467207049	korzina_big	2025-10-24	130	11	2	1	3422.00	\N	\N	8.00	18.00	1711.00	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
2a49b390-b64a-4e8c-8f3d-4412970eb886	04d8589f-d352-4c7c-893a-6e2d41c567dd	507758935	korzina_svetlaya	2025-10-24	157	15	4	1	6612.00	\N	\N	10.00	27.00	1653.00	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
bfc8f740-c609-41cc-b55c-8e4b462cc5c9	68b4da00-36ec-4cea-a8ff-367f1d27604c	467102886	korzina_tiger	2025-10-24	104	10	3	1	4608.00	\N	\N	10.00	30.00	1536.00	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
7f2cbe52-623d-4225-9ff9-526d62897e05	855090a0-7e1a-45c1-8403-fe70a7c64942	473520920	penal_braun_plush	2025-10-24	45	5	0	0	0.00	\N	\N	11.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
938fa93a-3d03-4a3e-b5f9-fe4d7caedfa3	f2779a56-27de-4904-84db-ff5f25f3110d	473520918	penal_bezh_plush	2025-10-24	29	2	0	0	0.00	\N	\N	7.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
0a8e32cc-d2b3-4c9b-9554-b2a4e8268b9d	a89f8530-97cc-49d5-89b3-dd37c5370f64	467102887	korzina_mini	2025-10-24	26	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
3a247402-f6da-4179-bf98-0d36f9445423	4997211b-836f-4450-be30-b1c5f0449ac3	467207051	korzina_grey	2025-10-24	16	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
9b2b356f-b0c5-4262-9dc1-df28c8e16d0d	829b0392-47e7-44d7-a671-cd419b25d2b1	467207055	korzina_pink	2025-10-24	15	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
f82c337b-987a-42e3-a469-ceae9f9c288d	61d3359d-3112-42fb-9685-73c39b74dc92	556953442	альбом_военный _зеленый_4цвета	2025-10-24	6	1	0	0	0.00	\N	\N	17.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
f40a0a4b-359e-4768-9da8-c6cf0c53b589	622ab6c1-6d24-44c7-90a5-1a0d66d33768	473520917	penal_white_plush	2025-10-24	19	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
8699e64f-c9bc-41e1-91cc-284d3d2bea1e	a6b9bc34-aa25-4f84-aa91-f1d303c8643b	473520914	penal_black_cat	2025-10-24	13	1	0	0	0.00	\N	\N	8.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
6a8bc259-4a39-406e-9c15-0a1a7ae74732	8dda1b27-c73e-4a12-a3ea-e36ffae2c0b8	473520919	penal_black_plush	2025-10-24	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
e103a8c9-203a-4994-96d2-7cdbe6354218	d28d47c9-76eb-406d-af4a-17fbb8066281	473520915	penal_blue_cat	2025-10-24	8	2	0	0	0.00	\N	\N	25.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
24d76e7f-b375-4a30-b68f-28d08174af86	90621643-12da-46d9-b967-8abc695e5652	473520916	penal_pink_cat	2025-10-24	9	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
458f75ec-c594-4ad1-839b-34c6657a5020	686420b8-63de-4638-abaf-cc89e4604bd2	555528192	альбом_военный_ зеленый_2цвета	2025-10-24	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
6bc531dd-0116-4b3d-a302-2b288cb652cd	2ca071cb-1e7f-4a50-aebf-307d36c4e3c3	556902796	альбом_вырезанное_ сердце_ голубой	2025-10-24	1	1	0	0	0.00	\N	\N	100.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
a43f9335-9efc-4c52-81ef-34179f4960e8	b0361398-d919-47ba-8d2c-c8ba8dc852c8	555528182	album_wave_old	2025-10-24	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
ce4a2a74-6787-4ce4-aeed-1eef3f515602	510f2e6e-1486-4082-91a2-154eddcd30a5	556902797	альбом_вырезанное_сердце_желтый	2025-10-24	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
0e012147-2897-4a14-a9a1-de6b1b4cfb60	2e67e4ac-1929-40a1-9f86-2c6dc73d548d	558118816	альбом_военный _коричневый	2025-10-24	5	1	0	0	0.00	\N	\N	20.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
e98d3138-302d-4a3c-bcfe-6ef8a41232cb	1a6edc21-1692-40ab-9f25-e01dd782082d	558118820	album_grey_heart	2025-10-24	0	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
6f5336fb-8c6c-47d5-94cd-adaa1d2c0a72	98319b3c-67ff-44e7-b4fc-deb47179c6d7	558118822	album_big_hearts	2025-10-24	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
386630c0-7a59-49d2-9046-3f82de544fee	aa9526c1-47a1-4cda-8ba3-1ac82917018a	555528176	album_serdechki	2025-10-24	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
af58f8d6-b6ee-4156-a963-3cb5fd2bee59	2dae1d26-2788-4a1f-b439-18510a6d45de	555528179	album_glyanec_grey	2025-10-24	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
43db4d47-adc8-4640-8c5a-24b1551dfafc	ca604ed0-475c-4d42-90b5-6b62234e0e17	555528188	album_leaves	2025-10-24	0	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
b03af01a-ff67-44cd-aff5-54655e68775e	ba729003-0857-4378-91eb-7712858964b6	556311214	альбом_вырезанное_сердце_розовый	2025-10-24	0	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
a4e4ffe6-0b5a-40a2-ad94-8b966bbdb8d9	d3ed53c9-4b5d-4dd1-9647-f5369b4105de	556311217	альбом_папоротник_Биг	2025-10-24	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
0b37ffc2-5c5a-4270-8d27-932804e547f2	1e009158-8d26-4300-8f5a-cd5c53825801	558118815	альбом _военный_краснопесчаный	2025-10-24	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
3d30901a-9d3d-47ef-ac4b-0b299a943e09	8db391e6-9ed3-489d-9cf9-ec1cbd0be071	555528177	album_big_hearts_Big	2025-10-24	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
73d4adfd-8123-4b00-b84c-7b58e0937987	1e4d4fe5-14df-45a2-8d2e-bf07c078cbab	555528178	album_vetochki	2025-10-24	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
8b8cc8f6-7a08-48a6-b97d-b2a16335aeba	bbab353e-6266-44fc-b2b5-fe615d67c650	556311216	альбом_лен_голубой	2025-10-24	0	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
ba3d2b41-a31d-4021-816c-5e7297c485c4	6afe8407-24e1-4ae2-b973-859cdb04a549	558118817	album_kaktus	2025-10-24	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
d96206da-d403-4d02-b8cb-bcb2f6408fdf	31f0df7a-98a3-4c01-a5a5-f73fc359bd28	558118819	альбом_веточки_Биг	2025-10-24	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
13b3051d-41ea-4146-a734-602099048cbc	707722f9-8229-4dc2-ba2a-82dac805e6a4	558118821	album_glyanec_pink	2025-10-24	0	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:50.32453+00	2025-11-13 12:42:50.32453+00
e2eef83f-e00c-431e-9401-d0bd1d47b29b	675a4e11-aafc-4d40-a112-4b4ec05e0411	467207049	korzina_big	2025-10-27	173	12	1	1	1711.00	\N	\N	7.00	8.00	1711.00	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
a805c67c-bf9f-4794-9d7c-2278a1ecbc48	04d8589f-d352-4c7c-893a-6e2d41c567dd	507758935	korzina_svetlaya	2025-10-27	153	11	1	1	1711.00	\N	\N	7.00	9.00	1711.00	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
5e9b8745-d5d4-4e7d-90ac-80af70d9283d	68b4da00-36ec-4cea-a8ff-367f1d27604c	467102886	korzina_tiger	2025-10-27	127	10	3	2	4608.00	\N	\N	8.00	30.00	1536.00	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
c1d695ce-ce9d-489c-bc27-508079a8b1e2	aa9526c1-47a1-4cda-8ba3-1ac82917018a	555528176	album_serdechki	2025-10-27	38	4	3	0	1890.00	\N	\N	11.00	75.00	630.00	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
d69486af-0c28-46a6-98c0-9f77ef209364	686420b8-63de-4638-abaf-cc89e4604bd2	555528192	альбом_военный_ зеленый_2цвета	2025-10-27	38	4	0	0	0.00	\N	\N	11.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
d18f8d2c-3a6c-4211-a46d-9f64ab5b5e41	855090a0-7e1a-45c1-8403-fe70a7c64942	473520920	penal_braun_plush	2025-10-27	27	3	1	1	494.00	\N	\N	11.00	33.00	494.00	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
3042c504-ffba-45e6-a2d4-fd2d48bfc90b	a89f8530-97cc-49d5-89b3-dd37c5370f64	467102887	korzina_mini	2025-10-27	25	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
0c14261d-8966-4219-9639-3dd1711ac035	4997211b-836f-4450-be30-b1c5f0449ac3	467207051	korzina_grey	2025-10-27	21	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
73108cf6-fe3d-4b28-8ea4-7775d88ce19d	829b0392-47e7-44d7-a671-cd419b25d2b1	467207055	korzina_pink	2025-10-27	21	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
131b9a78-36bf-477a-93e7-07318abd451e	61d3359d-3112-42fb-9685-73c39b74dc92	556953442	альбом_военный _зеленый_4цвета	2025-10-27	19	1	0	0	0.00	\N	\N	5.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
dfc15330-2dde-4a01-a3c7-e3023440db6f	f2779a56-27de-4904-84db-ff5f25f3110d	473520918	penal_bezh_plush	2025-10-27	14	2	0	0	0.00	\N	\N	14.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
130c5ddd-c7d6-4c53-bdf2-d76a984f6e48	2dae1d26-2788-4a1f-b439-18510a6d45de	555528179	album_glyanec_grey	2025-10-27	9	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
08e3605d-1132-409d-ba53-0c4e3d733445	2ca071cb-1e7f-4a50-aebf-307d36c4e3c3	556902796	альбом_вырезанное_ сердце_ голубой	2025-10-27	9	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
6d7753ec-7cea-4220-b914-4ed56ab15917	98319b3c-67ff-44e7-b4fc-deb47179c6d7	558118822	album_big_hearts	2025-10-27	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
dd50117f-c027-4733-9390-941c00a93ec4	3a6ebe20-a8d6-4341-9c61-870c698c7636	556311215	album_paporotnik	2025-10-27	7	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
1b4208a0-b418-4740-b5fc-b797d8648fb4	90621643-12da-46d9-b967-8abc695e5652	473520916	penal_pink_cat	2025-10-27	6	1	0	0	0.00	\N	\N	17.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
1c1ab4b8-fcad-4477-8bf7-e321bfffe0d7	ca604ed0-475c-4d42-90b5-6b62234e0e17	555528188	album_leaves	2025-10-27	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
09205cbe-4dda-4098-9257-b5804774f163	1e009158-8d26-4300-8f5a-cd5c53825801	558118815	альбом _военный_краснопесчаный	2025-10-27	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
886bef88-bb71-4511-bd6d-57fdf01eca82	a6b9bc34-aa25-4f84-aa91-f1d303c8643b	473520914	penal_black_cat	2025-10-27	5	1	0	0	0.00	\N	\N	20.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
b35bc032-befc-4782-8abf-286bd1390417	8dda1b27-c73e-4a12-a3ea-e36ffae2c0b8	473520919	penal_black_plush	2025-10-27	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
2b7ca2bd-ef26-4623-a412-6a92e0f1b183	1e4d4fe5-14df-45a2-8d2e-bf07c078cbab	555528178	album_vetochki	2025-10-27	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
6b7718ea-f5b0-4f57-bf5e-086692ee7f55	ba729003-0857-4378-91eb-7712858964b6	556311214	альбом_вырезанное_сердце_розовый	2025-10-27	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
9167402d-8621-40cf-b459-274913376f3f	d3ed53c9-4b5d-4dd1-9647-f5369b4105de	556311217	альбом_папоротник_Биг	2025-10-27	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
57769378-9360-4c27-8c86-3a0da93ac0af	510f2e6e-1486-4082-91a2-154eddcd30a5	556902797	альбом_вырезанное_сердце_желтый	2025-10-27	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
6be88c7c-515f-4f76-a95e-4f7790c9304c	d28d47c9-76eb-406d-af4a-17fbb8066281	473520915	penal_blue_cat	2025-10-27	4	1	0	0	0.00	\N	\N	25.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
48b713c8-9c7d-44cd-aab2-1d8afdfd7bbc	2585e4a1-16b5-4ff0-a298-c64655faa229	556311218	альбом_сердце_Биг	2025-10-27	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
fd88b83f-4991-4885-92ff-59da6f91f54e	31f0df7a-98a3-4c01-a5a5-f73fc359bd28	558118819	альбом_веточки_Биг	2025-10-27	4	1	0	0	0.00	\N	\N	25.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
c853208e-6b27-4af1-85ef-911306be2b52	b0361398-d919-47ba-8d2c-c8ba8dc852c8	555528182	album_wave_old	2025-10-27	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
124f63dd-ed2a-4aa2-a73a-179ffd455ad9	2e67e4ac-1929-40a1-9f86-2c6dc73d548d	558118816	альбом_военный _коричневый	2025-10-27	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
402a6104-839a-4426-beab-5d80bc552652	707722f9-8229-4dc2-ba2a-82dac805e6a4	558118821	album_glyanec_pink	2025-10-27	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
33d7dab1-27ac-490a-83e1-005da09d69db	8db391e6-9ed3-489d-9cf9-ec1cbd0be071	555528177	album_big_hearts_Big	2025-10-27	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
59dd7daf-1600-46e7-93c3-949b45afe638	531c3bfe-7a15-4d8f-aefe-6981fe3a179e	555528187	album_flamingo	2025-10-27	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
7e249647-f768-4e51-a4a0-bd7f71f896f0	bbab353e-6266-44fc-b2b5-fe615d67c650	556311216	альбом_лен_голубой	2025-10-27	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
00af3ce2-cef5-46cd-8bbb-9572300bd5f0	6afe8407-24e1-4ae2-b973-859cdb04a549	558118817	album_kaktus	2025-10-27	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
106ad5f3-b8bc-4a0b-bb73-e6ba3b290611	1a6edc21-1692-40ab-9f25-e01dd782082d	558118820	album_grey_heart	2025-10-27	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
ddd0d93e-912a-4fc1-b5ca-22b8d400cf82	622ab6c1-6d24-44c7-90a5-1a0d66d33768	473520917	penal_white_plush	2025-10-27	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.247137+00	2025-11-13 12:42:56.247137+00
f3b02dcf-e7e4-4525-ac14-26173876e659	675a4e11-aafc-4d40-a112-4b4ec05e0411	467207049	korzina_big	2025-10-26	163	12	3	1	5133.00	\N	\N	7.00	25.00	1711.00	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
ecd48350-8bfc-4ba9-af31-a3bb35f1e51b	04d8589f-d352-4c7c-893a-6e2d41c567dd	507758935	korzina_svetlaya	2025-10-26	173	14	2	1	3422.00	\N	\N	8.00	14.00	1711.00	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
f75170a1-68a3-41ba-8c98-0c41569fb4af	68b4da00-36ec-4cea-a8ff-367f1d27604c	467102886	korzina_tiger	2025-10-26	112	4	1	1	1536.00	\N	\N	4.00	25.00	1536.00	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
3accbdb3-dfd9-4375-b8e9-f83c8c917c85	aa9526c1-47a1-4cda-8ba3-1ac82917018a	555528176	album_serdechki	2025-10-26	51	4	3	1	1890.00	\N	\N	8.00	75.00	630.00	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
655c8639-1071-45de-a452-5b4ccb923322	686420b8-63de-4638-abaf-cc89e4604bd2	555528192	альбом_военный_ зеленый_2цвета	2025-10-26	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
36346f0d-6dec-42aa-a913-cd4ad06ad749	855090a0-7e1a-45c1-8403-fe70a7c64942	473520920	penal_braun_plush	2025-10-26	45	3	0	0	0.00	\N	\N	7.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
0d7acf49-a1ca-4dbd-8066-5eebc0e6af0d	a89f8530-97cc-49d5-89b3-dd37c5370f64	467102887	korzina_mini	2025-10-26	64	5	1	0	1159.00	\N	\N	8.00	20.00	1159.00	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
f7bcf5e7-eef9-418a-8bfb-371e98e7294c	4997211b-836f-4450-be30-b1c5f0449ac3	467207051	korzina_grey	2025-10-26	14	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
f0fd33ae-7ce5-48ff-a2e5-5baaf159d2a9	829b0392-47e7-44d7-a671-cd419b25d2b1	467207055	korzina_pink	2025-10-26	13	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
93dd492e-7137-412b-9c0b-e665c5295bae	61d3359d-3112-42fb-9685-73c39b74dc92	556953442	альбом_военный _зеленый_4цвета	2025-10-26	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
923ddc35-9a30-4d5a-98bb-2586209a08dd	f2779a56-27de-4904-84db-ff5f25f3110d	473520918	penal_bezh_plush	2025-10-26	28	4	0	0	0.00	\N	\N	14.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
c7ee5560-9b06-4e71-bff1-aa574f28c905	2dae1d26-2788-4a1f-b439-18510a6d45de	555528179	album_glyanec_grey	2025-10-26	7	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
7009e60d-5725-4722-aa77-303ba0abf283	2ca071cb-1e7f-4a50-aebf-307d36c4e3c3	556902796	альбом_вырезанное_ сердце_ голубой	2025-10-26	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
d127e4c0-5596-4623-827d-b4ff9fb9ac7a	98319b3c-67ff-44e7-b4fc-deb47179c6d7	558118822	album_big_hearts	2025-10-26	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
756cb2e3-7964-4c0e-8286-e4de9f2b50b6	3a6ebe20-a8d6-4341-9c61-870c698c7636	556311215	album_paporotnik	2025-10-26	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
ca5e57d0-80c0-42de-a9bd-9f571d202f12	90621643-12da-46d9-b967-8abc695e5652	473520916	penal_pink_cat	2025-10-26	10	1	0	0	0.00	\N	\N	10.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
b209e9b3-104f-4c38-9c66-39373e916da8	ca604ed0-475c-4d42-90b5-6b62234e0e17	555528188	album_leaves	2025-10-26	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
d7fd5c82-8024-455f-b0fd-dd5d9a73b189	1e009158-8d26-4300-8f5a-cd5c53825801	558118815	альбом _военный_краснопесчаный	2025-10-26	0	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
d3e72685-af21-4eb2-8cfd-8b2b0a90ea76	a6b9bc34-aa25-4f84-aa91-f1d303c8643b	473520914	penal_black_cat	2025-10-26	14	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
18200c0d-2089-4217-b867-8ed1465e4339	8dda1b27-c73e-4a12-a3ea-e36ffae2c0b8	473520919	penal_black_plush	2025-10-26	9	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
5dde6700-e191-4a48-b89b-38e71b7d7ab6	1e4d4fe5-14df-45a2-8d2e-bf07c078cbab	555528178	album_vetochki	2025-10-26	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
a5468c19-5c69-41b3-bdac-08ddf1782b8a	ba729003-0857-4378-91eb-7712858964b6	556311214	альбом_вырезанное_сердце_розовый	2025-10-26	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
e6655e47-7daa-46d1-808d-601d5c2af3e7	d3ed53c9-4b5d-4dd1-9647-f5369b4105de	556311217	альбом_папоротник_Биг	2025-10-26	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
5a6a74a6-7fb9-48be-9cf7-16771a589bdd	510f2e6e-1486-4082-91a2-154eddcd30a5	556902797	альбом_вырезанное_сердце_желтый	2025-10-26	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
c752db66-92d3-4f2b-8d38-42e35ba6bd2e	d28d47c9-76eb-406d-af4a-17fbb8066281	473520915	penal_blue_cat	2025-10-26	10	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
c66849f5-0f6f-4028-bd53-96baeda555b3	2585e4a1-16b5-4ff0-a298-c64655faa229	556311218	альбом_сердце_Биг	2025-10-26	4	1	0	0	0.00	\N	\N	25.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
9e7f096f-7a19-4a34-a763-6c591f09f064	31f0df7a-98a3-4c01-a5a5-f73fc359bd28	558118819	альбом_веточки_Биг	2025-10-26	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
49ca2f00-ccb9-4153-b6aa-f09108f6256d	b0361398-d919-47ba-8d2c-c8ba8dc852c8	555528182	album_wave_old	2025-10-26	5	1	0	0	0.00	\N	\N	20.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
ced3ccb9-cff3-4ff8-a884-97ec3844777f	2e67e4ac-1929-40a1-9f86-2c6dc73d548d	558118816	альбом_военный _коричневый	2025-10-26	4	2	1	0	900.00	\N	\N	50.00	50.00	900.00	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
c2818b97-1d06-4e18-b90a-446227a9b3d1	707722f9-8229-4dc2-ba2a-82dac805e6a4	558118821	album_glyanec_pink	2025-10-26	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
5c84536b-ad8d-40d1-9626-ae1259661d13	8db391e6-9ed3-489d-9cf9-ec1cbd0be071	555528177	album_big_hearts_Big	2025-10-26	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
d551be76-f07c-4788-b748-5826a9c3e483	531c3bfe-7a15-4d8f-aefe-6981fe3a179e	555528187	album_flamingo	2025-10-26	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
11cc1f2b-635e-4ce4-b9ee-e3df58dd3b20	bbab353e-6266-44fc-b2b5-fe615d67c650	556311216	альбом_лен_голубой	2025-10-26	0	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
5fe6fe65-4f26-4a3c-86c4-752c761a52ef	6afe8407-24e1-4ae2-b973-859cdb04a549	558118817	album_kaktus	2025-10-26	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
31ed4dac-b598-4542-9a24-a5bb35f0898c	1a6edc21-1692-40ab-9f25-e01dd782082d	558118820	album_grey_heart	2025-10-26	8	1	0	0	0.00	\N	\N	13.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
2608340f-1b10-442b-95b6-4c6afef56585	622ab6c1-6d24-44c7-90a5-1a0d66d33768	473520917	penal_white_plush	2025-10-26	7	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:42:56.257837+00	2025-11-13 12:42:56.257837+00
f3d06d0a-efd4-4932-845a-df76d16290e4	04d8589f-d352-4c7c-893a-6e2d41c567dd	507758935	korzina_svetlaya	2025-10-29	137	10	0	0	0.00	\N	\N	7.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
517d9a3c-2bb4-4e22-bdde-46359cab4ed8	675a4e11-aafc-4d40-a112-4b4ec05e0411	467207049	korzina_big	2025-10-29	136	9	3	0	5133.00	\N	\N	7.00	33.00	1711.00	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
da1ee41b-b0da-4387-831c-6c262c43677c	68b4da00-36ec-4cea-a8ff-367f1d27604c	467102886	korzina_tiger	2025-10-29	120	6	0	0	0.00	\N	\N	5.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
84881d5f-c6cf-4ea1-9cf0-d5c8ecbbf8e1	aa9526c1-47a1-4cda-8ba3-1ac82917018a	555528176	album_serdechki	2025-10-29	60	11	1	0	630.00	\N	\N	18.00	9.00	630.00	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
3af3f273-d8f6-406c-b35d-d649b7e59fce	ba729003-0857-4378-91eb-7712858964b6	556311214	альбом_вырезанное_сердце_розовый	2025-10-29	53	6	0	0	0.00	\N	\N	11.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
f945f8a5-7271-4bec-b7cd-13917bcf4718	686420b8-63de-4638-abaf-cc89e4604bd2	555528192	альбом_военный_ зеленый_2цвета	2025-10-29	32	3	2	0	1800.00	\N	\N	9.00	67.00	900.00	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
b6b66c8d-9eae-45b0-8570-b031664cc34c	a89f8530-97cc-49d5-89b3-dd37c5370f64	467102887	korzina_mini	2025-10-29	28	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
bcd95444-13c8-4a36-bda9-0753df0b2b30	2dae1d26-2788-4a1f-b439-18510a6d45de	555528179	album_glyanec_grey	2025-10-29	21	1	1	0	2910.00	\N	\N	5.00	100.00	2910.00	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
3e12df0d-c342-462a-a201-5818465294bb	855090a0-7e1a-45c1-8403-fe70a7c64942	473520920	penal_braun_plush	2025-10-29	20	3	0	0	0.00	\N	\N	15.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
0ffe2b86-ef73-4f4c-8572-085f33c7af88	61d3359d-3112-42fb-9685-73c39b74dc92	556953442	альбом_военный _зеленый_4цвета	2025-10-29	20	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
e140486f-7112-461d-91f8-5c6e4167da6e	829b0392-47e7-44d7-a671-cd419b25d2b1	467207055	korzina_pink	2025-10-29	18	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
634f609f-06ae-48df-a7d6-5734952fb203	4997211b-836f-4450-be30-b1c5f0449ac3	467207051	korzina_grey	2025-10-29	17	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
53d6004e-794c-4011-ad7a-e81ee78d2952	d3ed53c9-4b5d-4dd1-9647-f5369b4105de	556311217	альбом_папоротник_Биг	2025-10-29	13	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
89ba4982-b12d-4278-b29c-000c7736652f	2ca071cb-1e7f-4a50-aebf-307d36c4e3c3	556902796	альбом_вырезанное_ сердце_ голубой	2025-10-29	12	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
2988f9b8-9fc2-4880-8c0b-7d453a5a512d	f2779a56-27de-4904-84db-ff5f25f3110d	473520918	penal_bezh_plush	2025-10-29	11	3	1	0	429.00	\N	\N	27.00	33.00	429.00	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
4518c31c-4824-4ec5-8b51-efbc268d7f6e	98319b3c-67ff-44e7-b4fc-deb47179c6d7	558118822	album_big_hearts	2025-10-29	10	1	0	0	0.00	\N	\N	10.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
2d25b3b8-8bb3-4d6b-bd4d-eada11b67185	8db391e6-9ed3-489d-9cf9-ec1cbd0be071	555528177	album_big_hearts_Big	2025-10-29	9	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
8121d1e5-ce7f-4bdc-9506-b61fe80fcd76	2e67e4ac-1929-40a1-9f86-2c6dc73d548d	558118816	альбом_военный _коричневый	2025-10-29	9	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
884b6454-9ffc-4db0-a0d1-5633af49c227	6afe8407-24e1-4ae2-b973-859cdb04a549	558118817	album_kaktus	2025-10-29	9	1	0	0	0.00	\N	\N	11.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
483d13c1-5907-41f3-bb3a-971a5914a74e	1a6edc21-1692-40ab-9f25-e01dd782082d	558118820	album_grey_heart	2025-10-29	9	2	0	0	0.00	\N	\N	22.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
de8b22c9-daf8-43f3-9d9f-a0b6d4ba0c8a	707722f9-8229-4dc2-ba2a-82dac805e6a4	558118821	album_glyanec_pink	2025-10-29	9	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
9890fa49-1109-4e14-99bb-1549baf94151	3a6ebe20-a8d6-4341-9c61-870c698c7636	556311215	album_paporotnik	2025-10-29	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
7a289116-6457-4be6-bc72-741cecbacea7	2585e4a1-16b5-4ff0-a298-c64655faa229	556311218	альбом_сердце_Биг	2025-10-29	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
bca81c87-8a38-4ac3-9391-10e0fa2b398b	bbab353e-6266-44fc-b2b5-fe615d67c650	556311216	альбом_лен_голубой	2025-10-29	7	1	0	0	0.00	\N	\N	14.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
1d113c06-5b7e-4699-96e5-7c3378fd5519	1e009158-8d26-4300-8f5a-cd5c53825801	558118815	альбом _военный_краснопесчаный	2025-10-29	7	1	0	0	0.00	\N	\N	14.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
c947a5a0-dbd3-4b05-ba28-04d1453ff6ce	a6b9bc34-aa25-4f84-aa91-f1d303c8643b	473520914	penal_black_cat	2025-10-29	6	1	0	0	0.00	\N	\N	17.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
06cfe2ca-040e-4900-bcbe-78627f014591	1e4d4fe5-14df-45a2-8d2e-bf07c078cbab	555528178	album_vetochki	2025-10-29	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
f8f00daa-d589-4ff6-a4ed-3042cc461c05	531c3bfe-7a15-4d8f-aefe-6981fe3a179e	555528187	album_flamingo	2025-10-29	6	1	0	0	0.00	\N	\N	17.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
84d58d0f-535e-4801-b1ca-755286398394	31f0df7a-98a3-4c01-a5a5-f73fc359bd28	558118819	альбом_веточки_Биг	2025-10-29	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
d28048dc-fc85-465f-9053-2506673ed1b6	b0361398-d919-47ba-8d2c-c8ba8dc852c8	555528182	album_wave_old	2025-10-29	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
5a441238-aaab-4ba3-8013-f06df81691b5	ca604ed0-475c-4d42-90b5-6b62234e0e17	555528188	album_leaves	2025-10-29	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
187ddb50-efa3-4edd-8670-7c6eb943d61b	510f2e6e-1486-4082-91a2-154eddcd30a5	556902797	альбом_вырезанное_сердце_желтый	2025-10-29	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
829d4b43-daa5-4248-92a1-f22ca2372ce0	d28d47c9-76eb-406d-af4a-17fbb8066281	473520915	penal_blue_cat	2025-10-29	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
b435eb9c-fad0-4b85-a6f6-b410de9ac70d	90621643-12da-46d9-b967-8abc695e5652	473520916	penal_pink_cat	2025-10-29	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
1367d44e-c533-4ca4-b3c0-621a300244ea	2b1ecf5a-ec30-4c27-9ea4-675f09dda3c9	472859758	holder_pink	2025-10-29	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
2da5e907-cbfb-49d5-a552-44b481fb826b	8dda1b27-c73e-4a12-a3ea-e36ffae2c0b8	473520919	penal_black_plush	2025-10-29	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.794616+00	2025-11-13 12:43:02.794616+00
a3480cbf-a8f4-4c61-a1d2-c38973f58e0c	04d8589f-d352-4c7c-893a-6e2d41c567dd	507758935	korzina_svetlaya	2025-10-28	173	18	2	1	3422.00	\N	\N	10.00	11.00	1711.00	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
69b0526e-82fd-4c6e-8c0b-279748b78d44	675a4e11-aafc-4d40-a112-4b4ec05e0411	467207049	korzina_big	2025-10-28	156	18	3	1	5133.00	\N	\N	12.00	17.00	1711.00	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
7f3f28f2-336e-4f06-beac-68ad44d331b2	68b4da00-36ec-4cea-a8ff-367f1d27604c	467102886	korzina_tiger	2025-10-28	140	12	4	2	6144.00	\N	\N	9.00	33.00	1536.00	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
6b0943c4-76de-4615-b35d-0e8880541539	aa9526c1-47a1-4cda-8ba3-1ac82917018a	555528176	album_serdechki	2025-10-28	42	10	4	0	2520.00	\N	\N	24.00	40.00	630.00	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
c936bcbb-9505-4942-ba4d-4648447c67da	ba729003-0857-4378-91eb-7712858964b6	556311214	альбом_вырезанное_сердце_розовый	2025-10-28	29	6	1	0	900.00	\N	\N	21.00	17.00	900.00	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
639a7545-dd58-4ae4-8c46-07623d2aecf9	686420b8-63de-4638-abaf-cc89e4604bd2	555528192	альбом_военный_ зеленый_2цвета	2025-10-28	32	5	0	0	0.00	\N	\N	16.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
dcb007e5-0aeb-46d0-a67a-21aa57a9e65a	a89f8530-97cc-49d5-89b3-dd37c5370f64	467102887	korzina_mini	2025-10-28	22	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
8fea8c30-164c-447e-b117-edc61ce20a1e	2dae1d26-2788-4a1f-b439-18510a6d45de	555528179	album_glyanec_grey	2025-10-28	12	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
cc7ae01b-9757-4162-aa69-5b7d5b97b06a	855090a0-7e1a-45c1-8403-fe70a7c64942	473520920	penal_braun_plush	2025-10-28	37	15	0	0	0.00	\N	\N	41.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
1eba4d09-a2b7-4ccd-92bb-acefb9862b31	61d3359d-3112-42fb-9685-73c39b74dc92	556953442	альбом_военный _зеленый_4цвета	2025-10-28	17	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
fef63b84-d30e-4ce2-b0c6-272fcc482a5d	829b0392-47e7-44d7-a671-cd419b25d2b1	467207055	korzina_pink	2025-10-28	20	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
d9febb73-2dca-4152-bdbb-ab811b332873	4997211b-836f-4450-be30-b1c5f0449ac3	467207051	korzina_grey	2025-10-28	23	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
71b95966-89c4-4ed5-9a93-9077ad8b3db5	d3ed53c9-4b5d-4dd1-9647-f5369b4105de	556311217	альбом_папоротник_Биг	2025-10-28	9	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
da7afc68-6d0e-45c5-b5e3-d55b83ffd2ac	2ca071cb-1e7f-4a50-aebf-307d36c4e3c3	556902796	альбом_вырезанное_ сердце_ голубой	2025-10-28	7	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
c6fc9fef-7d2e-4d14-b821-0dd5b82411a9	f2779a56-27de-4904-84db-ff5f25f3110d	473520918	penal_bezh_plush	2025-10-28	42	4	1	0	429.00	\N	\N	10.00	25.00	429.00	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
ab57e632-23b1-43a9-94d3-b9703f0832a9	98319b3c-67ff-44e7-b4fc-deb47179c6d7	558118822	album_big_hearts	2025-10-28	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
709beaec-c048-47dc-aeec-41aa6794d5ea	8db391e6-9ed3-489d-9cf9-ec1cbd0be071	555528177	album_big_hearts_Big	2025-10-28	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
0c5d5420-187a-4eea-8d5b-85180002191c	2e67e4ac-1929-40a1-9f86-2c6dc73d548d	558118816	альбом_военный _коричневый	2025-10-28	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
9ae9b8d3-1ecd-4ec9-b3c9-e4f42ad097c1	6afe8407-24e1-4ae2-b973-859cdb04a549	558118817	album_kaktus	2025-10-28	4	1	0	0	0.00	\N	\N	25.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
5a571e6b-782c-4d38-89a3-2c92aeea2e7a	1a6edc21-1692-40ab-9f25-e01dd782082d	558118820	album_grey_heart	2025-10-28	3	1	0	0	0.00	\N	\N	33.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
da34df27-d44d-40da-a016-70cd4cd88e27	707722f9-8229-4dc2-ba2a-82dac805e6a4	558118821	album_glyanec_pink	2025-10-28	7	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
88b75ccc-f59c-4b6c-9da8-b5ed07971c34	3a6ebe20-a8d6-4341-9c61-870c698c7636	556311215	album_paporotnik	2025-10-28	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
aa5d1d47-6be6-437f-ace2-63e130d639e9	2585e4a1-16b5-4ff0-a298-c64655faa229	556311218	альбом_сердце_Биг	2025-10-28	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
4df74910-3760-423e-805d-ad267752922c	bbab353e-6266-44fc-b2b5-fe615d67c650	556311216	альбом_лен_голубой	2025-10-28	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
5eb7e08b-6597-48f7-bcb0-18a7e4ec5409	1e009158-8d26-4300-8f5a-cd5c53825801	558118815	альбом _военный_краснопесчаный	2025-10-28	4	1	0	0	0.00	\N	\N	25.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
3c3b101a-f2c7-4040-9079-37a5b57a521e	a6b9bc34-aa25-4f84-aa91-f1d303c8643b	473520914	penal_black_cat	2025-10-28	9	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
519594ba-9cc7-4d9a-b6c5-8f69c0b6cea3	1e4d4fe5-14df-45a2-8d2e-bf07c078cbab	555528178	album_vetochki	2025-10-28	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
b32c1117-d248-4bc8-8025-cbe23f9be432	531c3bfe-7a15-4d8f-aefe-6981fe3a179e	555528187	album_flamingo	2025-10-28	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
bfa10abd-7828-4e9d-b698-1706925bee86	31f0df7a-98a3-4c01-a5a5-f73fc359bd28	558118819	альбом_веточки_Биг	2025-10-28	0	1	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
725ace17-3ecf-4beb-b244-01d2208bc292	b0361398-d919-47ba-8d2c-c8ba8dc852c8	555528182	album_wave_old	2025-10-28	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
6b9ccc8f-ba00-4c2e-ace6-773e83527657	ca604ed0-475c-4d42-90b5-6b62234e0e17	555528188	album_leaves	2025-10-28	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
759501e0-e458-4d1c-b17c-6e69af9f4317	510f2e6e-1486-4082-91a2-154eddcd30a5	556902797	альбом_вырезанное_сердце_желтый	2025-10-28	5	1	0	0	0.00	\N	\N	20.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
afab1c80-313a-4139-80b4-8e19635c473c	d28d47c9-76eb-406d-af4a-17fbb8066281	473520915	penal_blue_cat	2025-10-28	9	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
a9b1db0b-9cff-40af-a5a4-154ceab53cc5	90621643-12da-46d9-b967-8abc695e5652	473520916	penal_pink_cat	2025-10-28	10	1	0	0	0.00	\N	\N	10.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
82ded2f3-39b5-46c6-bb7d-287a851a17ed	2b1ecf5a-ec30-4c27-9ea4-675f09dda3c9	472859758	holder_pink	2025-10-28	0	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
df21a5bd-4e62-40de-b6cd-1542701b79f0	8dda1b27-c73e-4a12-a3ea-e36ffae2c0b8	473520919	penal_black_plush	2025-10-28	13	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:02.80559+00	2025-11-13 12:43:02.80559+00
004779d1-5413-42fe-91e6-59685f9674c0	68b4da00-36ec-4cea-a8ff-367f1d27604c	467102886	korzina_tiger	2025-10-31	105	6	2	0	3072.00	\N	\N	6.00	33.00	1536.00	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
429c7870-2d1c-42cc-832a-7ee7e1dfab3a	675a4e11-aafc-4d40-a112-4b4ec05e0411	467207049	korzina_big	2025-10-31	100	6	2	0	3422.00	\N	\N	6.00	33.00	1711.00	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
6a826af1-9af0-4776-9b82-317a27958397	04d8589f-d352-4c7c-893a-6e2d41c567dd	507758935	korzina_svetlaya	2025-10-31	94	9	2	0	3422.00	\N	\N	10.00	22.00	1711.00	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
8701e2d0-1811-461f-9c22-919771bf751a	ba729003-0857-4378-91eb-7712858964b6	556311214	альбом_вырезанное_сердце_розовый	2025-10-31	51	5	4	0	3375.00	\N	\N	10.00	80.00	843.75	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
e8375951-c045-466d-b56e-01bbe85da233	aa9526c1-47a1-4cda-8ba3-1ac82917018a	555528176	album_serdechki	2025-10-31	49	6	3	0	1890.00	\N	\N	12.00	50.00	630.00	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
53af74f1-e638-40f5-bbd5-5101aa14d8c6	686420b8-63de-4638-abaf-cc89e4604bd2	555528192	альбом_военный_ зеленый_2цвета	2025-10-31	46	4	1	0	900.00	\N	\N	9.00	25.00	900.00	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
dc88baf3-9263-4e46-90f6-3b12a9f664aa	855090a0-7e1a-45c1-8403-fe70a7c64942	473520920	penal_braun_plush	2025-10-31	42	5	2	1	988.00	\N	\N	12.00	40.00	494.00	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
7e3b67be-ad6a-48c9-98f5-749d86fe48f8	f2779a56-27de-4904-84db-ff5f25f3110d	473520918	penal_bezh_plush	2025-10-31	32	4	2	0	858.00	\N	\N	13.00	50.00	429.00	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
a304debf-f101-4119-88d1-6972bee3ce29	61d3359d-3112-42fb-9685-73c39b74dc92	556953442	альбом_военный _зеленый_4цвета	2025-10-31	16	3	0	0	0.00	\N	\N	19.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
4e507aae-d55a-49c9-a44b-0bac2726a1de	90621643-12da-46d9-b967-8abc695e5652	473520916	penal_pink_cat	2025-10-31	14	2	0	0	0.00	\N	\N	14.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
87c7d5f3-424a-4bf0-a7ff-c85fd03e4529	8dda1b27-c73e-4a12-a3ea-e36ffae2c0b8	473520919	penal_black_plush	2025-10-31	12	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
3d65dd40-97fa-4e03-a656-d3b0c6a282df	a6b9bc34-aa25-4f84-aa91-f1d303c8643b	473520914	penal_black_cat	2025-10-31	11	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
9f65fc81-16db-4729-8777-4e760ae11e52	a89f8530-97cc-49d5-89b3-dd37c5370f64	467102887	korzina_mini	2025-10-31	10	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
8c5b8358-933d-4518-b53d-c70eca80e74d	829b0392-47e7-44d7-a671-cd419b25d2b1	467207055	korzina_pink	2025-10-31	9	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
8daca705-0442-44ab-8036-d393e37fb975	2ca071cb-1e7f-4a50-aebf-307d36c4e3c3	556902796	альбом_вырезанное_ сердце_ голубой	2025-10-31	9	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
1bd4c2b1-9bf7-4b69-93c6-c4bb2e1125de	2dae1d26-2788-4a1f-b439-18510a6d45de	555528179	album_glyanec_grey	2025-10-31	8	1	0	0	0.00	\N	\N	13.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
a61a344d-15f1-45b0-b448-e0c51a7b785d	2e67e4ac-1929-40a1-9f86-2c6dc73d548d	558118816	альбом_военный _коричневый	2025-10-31	8	1	0	0	0.00	\N	\N	13.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
a88b8382-0f65-4f0f-b6b1-a4a6fbc03d9e	1a6edc21-1692-40ab-9f25-e01dd782082d	558118820	album_grey_heart	2025-10-31	8	1	0	0	0.00	\N	\N	13.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
422c3c7b-30cd-4868-8103-84c4cf91d45a	8db391e6-9ed3-489d-9cf9-ec1cbd0be071	555528177	album_big_hearts_Big	2025-10-31	7	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
ae22aadf-48b6-418d-bf31-6946a268585b	d3ed53c9-4b5d-4dd1-9647-f5369b4105de	556311217	альбом_папоротник_Биг	2025-10-31	7	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
f58570b4-a322-4d1e-881d-e295a28f1815	2585e4a1-16b5-4ff0-a298-c64655faa229	556311218	альбом_сердце_Биг	2025-10-31	7	1	0	0	0.00	\N	\N	14.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
2a6f35d7-c172-4aff-b60b-e3892c4868f8	707722f9-8229-4dc2-ba2a-82dac805e6a4	558118821	album_glyanec_pink	2025-10-31	7	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
bc64e3b2-5a1e-4c7b-99ae-3970bc02aa86	98319b3c-67ff-44e7-b4fc-deb47179c6d7	558118822	album_big_hearts	2025-10-31	7	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
983bf4a7-3bb2-4b0a-bf3a-59d156ecbe6a	bbab353e-6266-44fc-b2b5-fe615d67c650	556311216	альбом_лен_голубой	2025-10-31	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
514877e9-6942-4e6d-94b3-9235a5ba043a	d28d47c9-76eb-406d-af4a-17fbb8066281	473520915	penal_blue_cat	2025-10-31	5	1	0	0	0.00	\N	\N	20.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
8fc486f1-5d28-4845-be37-acd9ba9faf68	622ab6c1-6d24-44c7-90a5-1a0d66d33768	473520917	penal_white_plush	2025-10-31	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
79c686e0-827d-4a3f-87da-2a02301713aa	b0361398-d919-47ba-8d2c-c8ba8dc852c8	555528182	album_wave_old	2025-10-31	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
96b50015-03a2-4971-a5da-41f8cdc30e9c	1e009158-8d26-4300-8f5a-cd5c53825801	558118815	альбом _военный_краснопесчаный	2025-10-31	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
20403c5c-63de-4b1a-9977-4da451d5058f	31f0df7a-98a3-4c01-a5a5-f73fc359bd28	558118819	альбом_веточки_Биг	2025-10-31	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
6defc31a-4acc-49cf-85d9-a8a6449d764f	4997211b-836f-4450-be30-b1c5f0449ac3	467207051	korzina_grey	2025-10-31	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
81bc6d3a-adea-41d9-950d-b30c95f8adb3	531c3bfe-7a15-4d8f-aefe-6981fe3a179e	555528187	album_flamingo	2025-10-31	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
8b32891c-f2d7-4f58-97c5-dcc6ecbce219	510f2e6e-1486-4082-91a2-154eddcd30a5	556902797	альбом_вырезанное_сердце_желтый	2025-10-31	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
c77f218d-a1f2-409b-80ef-48dc63f41b2f	6afe8407-24e1-4ae2-b973-859cdb04a549	558118817	album_kaktus	2025-10-31	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
825ff114-b8cc-491d-9b85-e1dbd0d61065	1e4d4fe5-14df-45a2-8d2e-bf07c078cbab	555528178	album_vetochki	2025-10-31	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
37bc772c-08a1-4485-91d8-b083659538d8	3a6ebe20-a8d6-4341-9c61-870c698c7636	556311215	album_paporotnik	2025-10-31	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
6232ac8d-4348-4342-b0e9-ab4c1c4ff42f	ca604ed0-475c-4d42-90b5-6b62234e0e17	555528188	album_leaves	2025-10-31	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.704202+00	2025-11-13 12:43:12.704202+00
bccb2efd-270b-428d-98f0-099ac22432fe	68b4da00-36ec-4cea-a8ff-367f1d27604c	467102886	korzina_tiger	2025-10-30	119	4	2	1	3072.00	\N	\N	3.00	50.00	1536.00	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
27ffb421-cb23-4c40-8642-17e0361ae61d	675a4e11-aafc-4d40-a112-4b4ec05e0411	467207049	korzina_big	2025-10-30	147	10	1	0	1711.00	\N	\N	7.00	10.00	1711.00	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
2f67b5a5-f866-4c36-9723-287c8580804e	04d8589f-d352-4c7c-893a-6e2d41c567dd	507758935	korzina_svetlaya	2025-10-30	152	9	1	1	1711.00	\N	\N	6.00	11.00	1711.00	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
759c282f-5a81-477c-a688-8adc15c45493	ba729003-0857-4378-91eb-7712858964b6	556311214	альбом_вырезанное_сердце_розовый	2025-10-30	43	5	0	0	0.00	\N	\N	12.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
c0c10251-a22b-4c07-b5d8-e2222dac4f6d	aa9526c1-47a1-4cda-8ba3-1ac82917018a	555528176	album_serdechki	2025-10-30	38	3	2	0	1260.00	\N	\N	8.00	67.00	630.00	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
6378ef9e-9e0b-4957-9d0a-411023baa436	686420b8-63de-4638-abaf-cc89e4604bd2	555528192	альбом_военный_ зеленый_2цвета	2025-10-30	39	2	0	0	0.00	\N	\N	5.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
b523765b-3527-4603-a3a6-c40820285d42	855090a0-7e1a-45c1-8403-fe70a7c64942	473520920	penal_braun_plush	2025-10-30	32	6	2	1	988.00	\N	\N	19.00	33.00	494.00	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
0ea04bfa-e854-4584-a3b0-bbb3252c9a93	f2779a56-27de-4904-84db-ff5f25f3110d	473520918	penal_bezh_plush	2025-10-30	13	3	1	1	429.00	\N	\N	23.00	33.00	429.00	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
1a236fbf-b1bc-4ca5-abd9-5fba845ae5d5	61d3359d-3112-42fb-9685-73c39b74dc92	556953442	альбом_военный _зеленый_4цвета	2025-10-30	21	2	0	0	0.00	\N	\N	10.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
df117259-35af-4d33-9178-10f6edd032f9	90621643-12da-46d9-b967-8abc695e5652	473520916	penal_pink_cat	2025-10-30	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
04ea6d1b-73da-4bc7-a151-91a936a9f4b4	8dda1b27-c73e-4a12-a3ea-e36ffae2c0b8	473520919	penal_black_plush	2025-10-30	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
0f4c77d0-dc95-442f-91c2-f381b775ad21	a6b9bc34-aa25-4f84-aa91-f1d303c8643b	473520914	penal_black_cat	2025-10-30	6	1	0	0	0.00	\N	\N	17.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
c955888f-a267-4737-8a94-b23047660b49	a89f8530-97cc-49d5-89b3-dd37c5370f64	467102887	korzina_mini	2025-10-30	22	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
a379f1ae-b18f-48d9-9e06-269d8bec42df	829b0392-47e7-44d7-a671-cd419b25d2b1	467207055	korzina_pink	2025-10-30	14	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
c85e5ef5-6c06-4507-9fb5-27f209bbfebf	2ca071cb-1e7f-4a50-aebf-307d36c4e3c3	556902796	альбом_вырезанное_ сердце_ голубой	2025-10-30	9	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
8518d634-8f27-42c8-8d35-361f4abe4b74	2dae1d26-2788-4a1f-b439-18510a6d45de	555528179	album_glyanec_grey	2025-10-30	10	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
71f24155-0ab8-4e08-9d51-223d89906ccf	2e67e4ac-1929-40a1-9f86-2c6dc73d548d	558118816	альбом_военный _коричневый	2025-10-30	8	1	0	0	0.00	\N	\N	13.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
bc6ca06b-05c6-4c9f-869e-e10dd0a8bdfc	1a6edc21-1692-40ab-9f25-e01dd782082d	558118820	album_grey_heart	2025-10-30	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
999080a3-dd59-4bd2-8731-c3a5070d8361	8db391e6-9ed3-489d-9cf9-ec1cbd0be071	555528177	album_big_hearts_Big	2025-10-30	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
092689c9-16a6-4db0-8afb-767a9e45e720	d3ed53c9-4b5d-4dd1-9647-f5369b4105de	556311217	альбом_папоротник_Биг	2025-10-30	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
49e155fd-b903-49d1-9426-f6cbf5712aec	2585e4a1-16b5-4ff0-a298-c64655faa229	556311218	альбом_сердце_Биг	2025-10-30	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
6ca3c8c0-59e1-451e-823d-79f60aa88b60	707722f9-8229-4dc2-ba2a-82dac805e6a4	558118821	album_glyanec_pink	2025-10-30	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
a86c7250-7caf-41d5-8f45-caabc3fa96e8	98319b3c-67ff-44e7-b4fc-deb47179c6d7	558118822	album_big_hearts	2025-10-30	10	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
370e3423-1640-4165-852d-f3bb81cd9748	bbab353e-6266-44fc-b2b5-fe615d67c650	556311216	альбом_лен_голубой	2025-10-30	9	1	0	0	0.00	\N	\N	11.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
86e3e76c-5a55-4abd-b3ca-c5e2aa6e765b	d28d47c9-76eb-406d-af4a-17fbb8066281	473520915	penal_blue_cat	2025-10-30	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
cd3b7e2d-5b49-4544-b2cf-d0456417f191	622ab6c1-6d24-44c7-90a5-1a0d66d33768	473520917	penal_white_plush	2025-10-30	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
99928d62-b4b5-4a61-84e5-b1c7e72fa631	b0361398-d919-47ba-8d2c-c8ba8dc852c8	555528182	album_wave_old	2025-10-30	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
0692ce13-8d6f-428c-8afe-612ec5ecd3c8	1e009158-8d26-4300-8f5a-cd5c53825801	558118815	альбом _военный_краснопесчаный	2025-10-30	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
20665437-6eef-45d9-bc6c-c4102a515b35	31f0df7a-98a3-4c01-a5a5-f73fc359bd28	558118819	альбом_веточки_Биг	2025-10-30	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
b2667293-6fcb-451a-b733-16be1b3b684c	4997211b-836f-4450-be30-b1c5f0449ac3	467207051	korzina_grey	2025-10-30	21	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
3e3290c8-062a-4ba4-9393-167c637fbf55	531c3bfe-7a15-4d8f-aefe-6981fe3a179e	555528187	album_flamingo	2025-10-30	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
06410a85-aa6a-4312-a6f8-7be35109a7e5	510f2e6e-1486-4082-91a2-154eddcd30a5	556902797	альбом_вырезанное_сердце_желтый	2025-10-30	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
79df98cc-c03d-4392-ab21-54a8f6d84ee7	6afe8407-24e1-4ae2-b973-859cdb04a549	558118817	album_kaktus	2025-10-30	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
f6993c2b-0c24-4838-9d37-686600eb5fb4	1e4d4fe5-14df-45a2-8d2e-bf07c078cbab	555528178	album_vetochki	2025-10-30	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
5c4b4e56-c6a3-4c89-9ca0-38b42fe3e07a	3a6ebe20-a8d6-4341-9c61-870c698c7636	556311215	album_paporotnik	2025-10-30	8	1	0	0	0.00	\N	\N	13.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
f10bba6b-c224-4bce-b7cc-0247282d9e2d	ca604ed0-475c-4d42-90b5-6b62234e0e17	555528188	album_leaves	2025-10-30	5	1	0	0	0.00	\N	\N	20.00	0.00	\N	2025-11-13 12:43:12.72936+00	2025-11-13 12:43:12.72936+00
d3f006ba-c56c-447a-8b62-10de749c1e6f	675a4e11-aafc-4d40-a112-4b4ec05e0411	467207049	korzina_big	2025-11-02	152	11	1	1	1711.00	\N	\N	7.00	9.00	1711.00	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
8b4d3434-5585-4106-ab87-65705a2e96f3	04d8589f-d352-4c7c-893a-6e2d41c567dd	507758935	korzina_svetlaya	2025-11-02	149	16	1	0	1711.00	\N	\N	11.00	6.00	1711.00	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
d7fa5d86-6d82-43da-8296-449b99d20b5e	68b4da00-36ec-4cea-a8ff-367f1d27604c	467102886	korzina_tiger	2025-11-02	118	9	2	0	3072.00	\N	\N	8.00	22.00	1536.00	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
89e0f03e-a1f9-4bf6-99de-320db9c49182	aa9526c1-47a1-4cda-8ba3-1ac82917018a	555528176	album_serdechki	2025-11-02	83	17	5	1	3150.00	\N	\N	20.00	29.00	630.00	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
0b7b50b9-6331-41a9-af1b-abcdab1c02da	686420b8-63de-4638-abaf-cc89e4604bd2	555528192	альбом_военный_ зеленый_2цвета	2025-11-02	62	7	3	0	2700.00	\N	\N	11.00	43.00	900.00	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
bf0838b6-8755-48be-80d0-7ef7f0afee0c	f2779a56-27de-4904-84db-ff5f25f3110d	473520918	penal_bezh_plush	2025-11-02	47	9	0	0	0.00	\N	\N	19.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
3439ec9f-c5ec-4b61-ab78-dda9bde7cba8	ba729003-0857-4378-91eb-7712858964b6	556311214	альбом_вырезанное_сердце_розовый	2025-11-02	28	4	0	0	0.00	\N	\N	14.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
2ed3fbb0-d129-4b82-85ed-940af6ca3828	a89f8530-97cc-49d5-89b3-dd37c5370f64	467102887	korzina_mini	2025-11-02	23	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
fdabe338-d12d-4a93-a0d6-451d500de909	855090a0-7e1a-45c1-8403-fe70a7c64942	473520920	penal_braun_plush	2025-11-02	20	3	1	0	494.00	\N	\N	15.00	33.00	494.00	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
896b0bed-1ac9-4c46-b867-fa110b9d59ed	61d3359d-3112-42fb-9685-73c39b74dc92	556953442	альбом_военный _зеленый_4цвета	2025-11-02	17	2	0	0	0.00	\N	\N	12.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
005eafa1-00d3-4e8c-9d78-051dce01fd6f	829b0392-47e7-44d7-a671-cd419b25d2b1	467207055	korzina_pink	2025-11-02	14	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
a5dae96d-4a11-4a8d-a2fb-671866b58ea3	2ca071cb-1e7f-4a50-aebf-307d36c4e3c3	556902796	альбом_вырезанное_ сердце_ голубой	2025-11-02	13	1	0	0	0.00	\N	\N	8.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
60522d9c-6674-4d13-b3d4-942d1018a51a	4997211b-836f-4450-be30-b1c5f0449ac3	467207051	korzina_grey	2025-11-02	12	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
ae3c3c6b-0f67-4ec6-8264-91078af7818b	2e67e4ac-1929-40a1-9f86-2c6dc73d548d	558118816	альбом_военный _коричневый	2025-11-02	12	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
87b4ddc9-8211-46af-80cf-a6cbee4e0ed0	2dae1d26-2788-4a1f-b439-18510a6d45de	555528179	album_glyanec_grey	2025-11-02	11	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
974aa7ee-ee2d-4afd-8595-0d22f5aef036	98319b3c-67ff-44e7-b4fc-deb47179c6d7	558118822	album_big_hearts	2025-11-02	10	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
d2304c4f-8c0a-4445-baed-ded19eeaef93	90621643-12da-46d9-b967-8abc695e5652	473520916	penal_pink_cat	2025-11-02	9	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
66fd8f6e-7167-446b-9448-f6b6fd4c616d	d3ed53c9-4b5d-4dd1-9647-f5369b4105de	556311217	альбом_папоротник_Биг	2025-11-02	9	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
e774bd6d-a4d9-4e3a-8ce1-91c8b7c6a210	a6b9bc34-aa25-4f84-aa91-f1d303c8643b	473520914	penal_black_cat	2025-11-02	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
6e15e6c9-4a0c-4448-8b03-5e1bbc60f985	3a6ebe20-a8d6-4341-9c61-870c698c7636	556311215	album_paporotnik	2025-11-02	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
4a2192de-295f-4472-a3dc-ee98553547ff	1a6edc21-1692-40ab-9f25-e01dd782082d	558118820	album_grey_heart	2025-11-02	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
d9c716de-ddb9-426c-8770-780eb63ea48f	d28d47c9-76eb-406d-af4a-17fbb8066281	473520915	penal_blue_cat	2025-11-02	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
f7f886ef-7b44-4d08-b27f-1f5570b98d1d	8dda1b27-c73e-4a12-a3ea-e36ffae2c0b8	473520919	penal_black_plush	2025-11-02	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
68643b7b-4ce1-4c0f-91ee-2cb0af87bdc8	8db391e6-9ed3-489d-9cf9-ec1cbd0be071	555528177	album_big_hearts_Big	2025-11-02	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
d648a945-b025-4569-a9c7-e2bd1a55e1ee	2585e4a1-16b5-4ff0-a298-c64655faa229	556311218	альбом_сердце_Биг	2025-11-02	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
499b336d-994d-4fad-8807-7cebabe3fe5e	707722f9-8229-4dc2-ba2a-82dac805e6a4	558118821	album_glyanec_pink	2025-11-02	5	1	0	0	0.00	\N	\N	20.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
407a80b8-ed2f-4129-8240-73981b2740c7	b0361398-d919-47ba-8d2c-c8ba8dc852c8	555528182	album_wave_old	2025-11-02	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
bfc1d00e-560e-4125-a9d2-39590150fb48	bbab353e-6266-44fc-b2b5-fe615d67c650	556311216	альбом_лен_голубой	2025-11-02	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
468b9376-2049-4e6e-9f84-a0fef6fd8781	1e009158-8d26-4300-8f5a-cd5c53825801	558118815	альбом _военный_краснопесчаный	2025-11-02	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
58c3a26a-7e41-4f61-85c0-2b189216bbb7	31f0df7a-98a3-4c01-a5a5-f73fc359bd28	558118819	альбом_веточки_Биг	2025-11-02	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
2cc4dbc2-29f5-46ee-b946-734710218a2a	622ab6c1-6d24-44c7-90a5-1a0d66d33768	473520917	penal_white_plush	2025-11-02	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
186aa90e-9d7d-4387-a58f-96f4b5526be3	1e4d4fe5-14df-45a2-8d2e-bf07c078cbab	555528178	album_vetochki	2025-11-02	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
99fdb097-81e4-4224-a06e-393d69c348a5	ca604ed0-475c-4d42-90b5-6b62234e0e17	555528188	album_leaves	2025-11-02	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
21184b8a-91e2-4a9a-be39-1d9a033f70d0	510f2e6e-1486-4082-91a2-154eddcd30a5	556902797	альбом_вырезанное_сердце_желтый	2025-11-02	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
0f65c61c-67da-43a5-a3d4-bdbb8c6346af	6afe8407-24e1-4ae2-b973-859cdb04a549	558118817	album_kaktus	2025-11-02	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
68f19f67-10cf-4288-9d46-283fd7e00711	2b1ecf5a-ec30-4c27-9ea4-675f09dda3c9	472859758	holder_pink	2025-11-02	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
56dff5a4-e207-406a-9e33-d1d4453c6ea1	531c3bfe-7a15-4d8f-aefe-6981fe3a179e	555528187	album_flamingo	2025-11-02	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.813203+00	2025-11-13 12:43:32.813203+00
e853e44c-95cb-4712-a815-ba87e9a306a7	675a4e11-aafc-4d40-a112-4b4ec05e0411	467207049	korzina_big	2025-11-01	153	15	2	0	3422.00	\N	\N	10.00	13.00	1711.00	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
5e89e4bb-d726-4790-a5af-a35656c856e5	04d8589f-d352-4c7c-893a-6e2d41c567dd	507758935	korzina_svetlaya	2025-11-01	136	12	1	1	1711.00	\N	\N	9.00	8.00	1711.00	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
07e974de-03e6-4ec4-80f2-62c54244c0e7	68b4da00-36ec-4cea-a8ff-367f1d27604c	467102886	korzina_tiger	2025-11-01	132	5	1	0	1536.00	\N	\N	4.00	20.00	1536.00	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
905fb230-a16d-4d7b-b060-e03b0f818da6	aa9526c1-47a1-4cda-8ba3-1ac82917018a	555528176	album_serdechki	2025-11-01	41	3	0	0	0.00	\N	\N	7.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
97bed952-1a64-4572-8906-9b2f1930445f	686420b8-63de-4638-abaf-cc89e4604bd2	555528192	альбом_военный_ зеленый_2цвета	2025-11-01	50	8	2	0	1800.00	\N	\N	16.00	25.00	900.00	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
0ce1c358-23cc-43ec-95fe-848f3d4d41e1	f2779a56-27de-4904-84db-ff5f25f3110d	473520918	penal_bezh_plush	2025-11-01	13	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
6889b4a4-1997-4d0e-95b4-7a3e1da6ac41	ba729003-0857-4378-91eb-7712858964b6	556311214	альбом_вырезанное_сердце_розовый	2025-11-01	43	3	0	0	0.00	\N	\N	7.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
3ed82ef9-f07f-463b-add8-4a05cb73dabd	a89f8530-97cc-49d5-89b3-dd37c5370f64	467102887	korzina_mini	2025-11-01	24	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
8942ec4d-44e3-41f3-94e8-ee3d13d2b3b4	855090a0-7e1a-45c1-8403-fe70a7c64942	473520920	penal_braun_plush	2025-11-01	17	5	0	0	0.00	\N	\N	29.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
5dc41c91-acc9-42f1-854f-fe1e66a7aed6	61d3359d-3112-42fb-9685-73c39b74dc92	556953442	альбом_военный _зеленый_4цвета	2025-11-01	20	1	0	0	0.00	\N	\N	5.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
a88baa16-cc41-4378-b37a-54897db25138	829b0392-47e7-44d7-a671-cd419b25d2b1	467207055	korzina_pink	2025-11-01	19	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
80bbf8ce-39e0-4194-970e-f8b5bc9ca111	2ca071cb-1e7f-4a50-aebf-307d36c4e3c3	556902796	альбом_вырезанное_ сердце_ голубой	2025-11-01	6	1	0	0	0.00	\N	\N	17.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
09fdea7d-2ed7-4c79-ac2c-69ff73741441	4997211b-836f-4450-be30-b1c5f0449ac3	467207051	korzina_grey	2025-11-01	23	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
c89c85d6-7bda-4e26-a112-aad969558f6b	2e67e4ac-1929-40a1-9f86-2c6dc73d548d	558118816	альбом_военный _коричневый	2025-11-01	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
642b023a-1e71-4183-b595-8510be8dd217	2dae1d26-2788-4a1f-b439-18510a6d45de	555528179	album_glyanec_grey	2025-11-01	11	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
683c320e-413e-463e-9bef-721c375d13dd	98319b3c-67ff-44e7-b4fc-deb47179c6d7	558118822	album_big_hearts	2025-11-01	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
6428a291-6ceb-4385-958b-cacaf2a11d5b	90621643-12da-46d9-b967-8abc695e5652	473520916	penal_pink_cat	2025-11-01	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
a61f8ab3-e6e5-42ea-a7c5-b49afca09fcf	d3ed53c9-4b5d-4dd1-9647-f5369b4105de	556311217	альбом_папоротник_Биг	2025-11-01	9	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
841f0faf-837c-4a2b-b5cb-45d84e4196ae	a6b9bc34-aa25-4f84-aa91-f1d303c8643b	473520914	penal_black_cat	2025-11-01	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
cbac0b2d-2ff4-4aad-8d1e-78e496feb11d	3a6ebe20-a8d6-4341-9c61-870c698c7636	556311215	album_paporotnik	2025-11-01	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
7a5fd088-fa7d-4af3-86c0-3836c2e36ab3	1a6edc21-1692-40ab-9f25-e01dd782082d	558118820	album_grey_heart	2025-11-01	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
04e6e080-c6d3-46ac-8bdb-80fcab477446	d28d47c9-76eb-406d-af4a-17fbb8066281	473520915	penal_blue_cat	2025-11-01	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
90b85bc9-26a6-4cfd-b3ed-ebb7955c2b63	8dda1b27-c73e-4a12-a3ea-e36ffae2c0b8	473520919	penal_black_plush	2025-11-01	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
998c4f8b-d619-40f1-be23-c40dda247011	8db391e6-9ed3-489d-9cf9-ec1cbd0be071	555528177	album_big_hearts_Big	2025-11-01	7	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
757f8a47-3bc0-4306-bded-de8b90e4c40f	2585e4a1-16b5-4ff0-a298-c64655faa229	556311218	альбом_сердце_Биг	2025-11-01	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
61d3ea5c-c85b-4056-8f2a-91ae7b26ba4a	707722f9-8229-4dc2-ba2a-82dac805e6a4	558118821	album_glyanec_pink	2025-11-01	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
73b1a50d-2293-4480-8399-7374ec41ecf4	b0361398-d919-47ba-8d2c-c8ba8dc852c8	555528182	album_wave_old	2025-11-01	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
568b61c1-f72b-4a70-8c26-68b3e0258f38	bbab353e-6266-44fc-b2b5-fe615d67c650	556311216	альбом_лен_голубой	2025-11-01	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
c827adb2-e7d9-4914-9f39-6f3f59bbf42c	1e009158-8d26-4300-8f5a-cd5c53825801	558118815	альбом _военный_краснопесчаный	2025-11-01	8	1	1	0	900.00	\N	\N	13.00	100.00	900.00	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
821e9730-d0e3-4b34-9dea-c892a9a246e4	31f0df7a-98a3-4c01-a5a5-f73fc359bd28	558118819	альбом_веточки_Биг	2025-11-01	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
31ac94cd-0db1-4a74-9def-30c1200afcad	622ab6c1-6d24-44c7-90a5-1a0d66d33768	473520917	penal_white_plush	2025-11-01	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
1e8f19cd-b11e-441a-ac49-102a31c114fd	1e4d4fe5-14df-45a2-8d2e-bf07c078cbab	555528178	album_vetochki	2025-11-01	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
36c54d1d-9421-471d-b9d3-4658a9e69226	ca604ed0-475c-4d42-90b5-6b62234e0e17	555528188	album_leaves	2025-11-01	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
6e4575eb-17c2-41f6-aac5-9e9dca8e5fc7	510f2e6e-1486-4082-91a2-154eddcd30a5	556902797	альбом_вырезанное_сердце_желтый	2025-11-01	4	1	0	0	0.00	\N	\N	25.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
7484fb6a-2968-4132-ac11-60b4091c0ad1	6afe8407-24e1-4ae2-b973-859cdb04a549	558118817	album_kaktus	2025-11-01	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
44460a13-dddf-43f5-ab37-dc7a9197d72f	2b1ecf5a-ec30-4c27-9ea4-675f09dda3c9	472859758	holder_pink	2025-11-01	0	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
82c712b5-3569-48c4-81a3-9956a04e9cb4	531c3bfe-7a15-4d8f-aefe-6981fe3a179e	555528187	album_flamingo	2025-11-01	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:32.825035+00	2025-11-13 12:43:32.825035+00
af9e442b-f2cb-4e10-858d-fdcace907d62	675a4e11-aafc-4d40-a112-4b4ec05e0411	467207049	korzina_big	2025-11-04	193	18	4	1	6844.00	\N	\N	9.00	22.00	1711.00	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
557dd0bc-6aa5-4f5b-8c6c-70624c9d92d3	04d8589f-d352-4c7c-893a-6e2d41c567dd	507758935	korzina_svetlaya	2025-11-04	173	11	2	1	3423.00	\N	\N	6.00	18.00	1711.50	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
70f18b80-9ed7-4ee6-9fb8-10cc70f97120	68b4da00-36ec-4cea-a8ff-367f1d27604c	467102886	korzina_tiger	2025-11-04	143	8	4	0	6144.00	\N	\N	6.00	50.00	1536.00	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
03b33285-739d-4473-91b6-07ac4d20c9a7	855090a0-7e1a-45c1-8403-fe70a7c64942	473520920	penal_braun_plush	2025-11-04	52	3	1	0	494.00	\N	\N	6.00	33.00	494.00	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
31bcab0e-eb5d-4917-a756-30ec889a579a	f2779a56-27de-4904-84db-ff5f25f3110d	473520918	penal_bezh_plush	2025-11-04	51	3	0	0	0.00	\N	\N	6.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
474866eb-ebc4-46a6-a058-427e2537d926	686420b8-63de-4638-abaf-cc89e4604bd2	555528192	альбом_военный_ зеленый_2цвета	2025-11-04	41	2	0	0	0.00	\N	\N	5.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
523f8f4a-1eb4-46ae-98d7-255d7f8a6c04	ba729003-0857-4378-91eb-7712858964b6	556311214	альбом_вырезанное_сердце_розовый	2025-11-04	22	2	0	0	0.00	\N	\N	9.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
9e54cd89-3c26-4f1a-ae13-ec382fd5c238	a89f8530-97cc-49d5-89b3-dd37c5370f64	467102887	korzina_mini	2025-11-04	20	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
7239e30d-8a08-45ca-a02c-c9bb5cdd5657	90621643-12da-46d9-b967-8abc695e5652	473520916	penal_pink_cat	2025-11-04	19	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
5c6a5e55-6338-40d6-95c3-ae239c08eed1	d28d47c9-76eb-406d-af4a-17fbb8066281	473520915	penal_blue_cat	2025-11-04	18	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
6fb48eb1-20fc-46bf-890d-0c64f9357782	8dda1b27-c73e-4a12-a3ea-e36ffae2c0b8	473520919	penal_black_plush	2025-11-04	18	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
39eb2761-b08c-4083-9246-6100a5626a50	61d3359d-3112-42fb-9685-73c39b74dc92	556953442	альбом_военный _зеленый_4цвета	2025-11-04	18	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
2cdb5dc0-4780-4d27-b704-52b6e3bfeae7	a6b9bc34-aa25-4f84-aa91-f1d303c8643b	473520914	penal_black_cat	2025-11-04	17	1	1	0	429.00	\N	\N	6.00	100.00	429.00	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
cca98925-ca54-4a73-8c8b-a5e747b44431	829b0392-47e7-44d7-a671-cd419b25d2b1	467207055	korzina_pink	2025-11-04	16	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
3f7f77d3-bbfe-45ca-bbc9-96d0aacda418	4997211b-836f-4450-be30-b1c5f0449ac3	467207051	korzina_grey	2025-11-04	15	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
7d5e3bb4-10a3-46ac-b465-0c19b7dfaaa9	2dae1d26-2788-4a1f-b439-18510a6d45de	555528179	album_glyanec_grey	2025-11-04	13	3	0	0	0.00	\N	\N	23.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
ebaaec5f-441e-4abf-a232-0e32c0e73138	622ab6c1-6d24-44c7-90a5-1a0d66d33768	473520917	penal_white_plush	2025-11-04	12	1	0	0	0.00	\N	\N	8.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
0c897d62-9625-4b81-8372-6928a5db24c8	2e67e4ac-1929-40a1-9f86-2c6dc73d548d	558118816	альбом_военный _коричневый	2025-11-04	12	1	0	0	0.00	\N	\N	8.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
2008e49f-14f4-490d-acea-2bd11b043927	aa9526c1-47a1-4cda-8ba3-1ac82917018a	555528176	album_serdechki	2025-11-04	11	1	1	0	630.00	\N	\N	9.00	100.00	630.00	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
07497733-615c-409b-bc78-680723d2e38d	d3ed53c9-4b5d-4dd1-9647-f5369b4105de	556311217	альбом_папоротник_Биг	2025-11-04	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
74e90bad-28cc-41fa-84bc-862085cd4ec5	1e009158-8d26-4300-8f5a-cd5c53825801	558118815	альбом _военный_краснопесчаный	2025-11-04	7	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
bd1db833-bc7d-478c-ac72-f15aae35b534	2ca071cb-1e7f-4a50-aebf-307d36c4e3c3	556902796	альбом_вырезанное_ сердце_ голубой	2025-11-04	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
b113b66b-7f43-4853-beb1-6e67672fc480	3a6ebe20-a8d6-4341-9c61-870c698c7636	556311215	album_paporotnik	2025-11-04	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
7dfeb648-a59d-4057-8b73-d62ea4c733b0	6afe8407-24e1-4ae2-b973-859cdb04a549	558118817	album_kaktus	2025-11-04	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
7a45d5df-b5fc-4173-8d69-a23cde118986	98319b3c-67ff-44e7-b4fc-deb47179c6d7	558118822	album_big_hearts	2025-11-04	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
97257ebf-497a-4281-b826-a9896d0b1e92	b0361398-d919-47ba-8d2c-c8ba8dc852c8	555528182	album_wave_old	2025-11-04	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
445fe0d5-3f7e-4a60-a8af-82b544439e91	ca604ed0-475c-4d42-90b5-6b62234e0e17	555528188	album_leaves	2025-11-04	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
9ed91267-761b-466e-a357-b2b847cc8228	510f2e6e-1486-4082-91a2-154eddcd30a5	556902797	альбом_вырезанное_сердце_желтый	2025-11-04	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
af4c8385-5c62-47db-a435-9527464df73f	8db391e6-9ed3-489d-9cf9-ec1cbd0be071	555528177	album_big_hearts_Big	2025-11-04	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
de52f0a8-2a65-4d9a-9ca0-59417b9fceec	2585e4a1-16b5-4ff0-a298-c64655faa229	556311218	альбом_сердце_Биг	2025-11-04	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
bf1fb858-aa54-4a13-8cbb-8815d3e51be7	1e4d4fe5-14df-45a2-8d2e-bf07c078cbab	555528178	album_vetochki	2025-11-04	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
9d3dcb25-9d87-480f-ba7a-0228f08c3ee7	bbab353e-6266-44fc-b2b5-fe615d67c650	556311216	альбом_лен_голубой	2025-11-04	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
8f4f8154-f0f0-429f-a398-4db1b221d291	31f0df7a-98a3-4c01-a5a5-f73fc359bd28	558118819	альбом_веточки_Биг	2025-11-04	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
c16c0e67-1949-4913-a562-eacaa6410b00	1a6edc21-1692-40ab-9f25-e01dd782082d	558118820	album_grey_heart	2025-11-04	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
dd2409ff-9e95-4679-b12d-e7ec43bf8c8a	707722f9-8229-4dc2-ba2a-82dac805e6a4	558118821	album_glyanec_pink	2025-11-04	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.264798+00	2025-11-13 12:43:52.264798+00
7d58081d-23c3-4422-925c-93678da9f27d	675a4e11-aafc-4d40-a112-4b4ec05e0411	467207049	korzina_big	2025-11-03	169	14	3	0	5133.00	\N	\N	8.00	21.00	1711.00	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
0d68e826-c5a1-4a5a-97fc-5e73169d07f6	04d8589f-d352-4c7c-893a-6e2d41c567dd	507758935	korzina_svetlaya	2025-11-03	142	12	2	1	3422.00	\N	\N	8.00	17.00	1711.00	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
82d4a9ab-751c-4cae-aa74-827977ec81f3	68b4da00-36ec-4cea-a8ff-367f1d27604c	467102886	korzina_tiger	2025-11-03	98	6	4	0	6144.00	\N	\N	6.00	67.00	1536.00	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
86bd64ec-2cf6-49e2-b101-8169cc2ca10e	855090a0-7e1a-45c1-8403-fe70a7c64942	473520920	penal_braun_plush	2025-11-03	30	3	1	0	494.00	\N	\N	10.00	33.00	494.00	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
9ee915e8-2d98-4fc3-9397-a3908d00a3a4	f2779a56-27de-4904-84db-ff5f25f3110d	473520918	penal_bezh_plush	2025-11-03	25	2	0	0	0.00	\N	\N	8.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
362039ba-8952-4382-ba5b-903768339c62	686420b8-63de-4638-abaf-cc89e4604bd2	555528192	альбом_военный_ зеленый_2цвета	2025-11-03	59	3	1	0	900.00	\N	\N	5.00	33.00	900.00	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
4dcc3e1e-4e7d-4af5-9b6c-b02049d596b5	ba729003-0857-4378-91eb-7712858964b6	556311214	альбом_вырезанное_сердце_розовый	2025-11-03	12	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
343c3170-551d-4cf1-b0a3-62a6f999387f	a89f8530-97cc-49d5-89b3-dd37c5370f64	467102887	korzina_mini	2025-11-03	11	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
3a051d83-bbb8-40c9-99fc-5176366297f4	90621643-12da-46d9-b967-8abc695e5652	473520916	penal_pink_cat	2025-11-03	7	1	0	0	0.00	\N	\N	14.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
2ce38023-ec72-42d5-8e9a-f516ae0720d7	d28d47c9-76eb-406d-af4a-17fbb8066281	473520915	penal_blue_cat	2025-11-03	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
c5c5f921-4afb-49a0-8641-f52defa1ad06	8dda1b27-c73e-4a12-a3ea-e36ffae2c0b8	473520919	penal_black_plush	2025-11-03	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
a61bb0eb-a073-4f78-9c37-7e427d148aae	61d3359d-3112-42fb-9685-73c39b74dc92	556953442	альбом_военный _зеленый_4цвета	2025-11-03	16	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
5bdc639e-f405-4d75-b5d7-687f08c76f43	a6b9bc34-aa25-4f84-aa91-f1d303c8643b	473520914	penal_black_cat	2025-11-03	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
27cba435-ce5c-48e7-b392-5d6d5614669b	829b0392-47e7-44d7-a671-cd419b25d2b1	467207055	korzina_pink	2025-11-03	15	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
c7796938-cb22-4422-b1e4-74410bcd31b8	4997211b-836f-4450-be30-b1c5f0449ac3	467207051	korzina_grey	2025-11-03	11	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
87cc2998-ea7d-434d-b50e-7068280d6b67	2dae1d26-2788-4a1f-b439-18510a6d45de	555528179	album_glyanec_grey	2025-11-03	23	3	1	0	2910.00	\N	\N	13.00	33.00	2910.00	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
0b83252e-8a4d-4daf-8f6f-3df90fc5b2fe	622ab6c1-6d24-44c7-90a5-1a0d66d33768	473520917	penal_white_plush	2025-11-03	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
107cba83-3291-46b3-922a-b138a93229ac	2e67e4ac-1929-40a1-9f86-2c6dc73d548d	558118816	альбом_военный _коричневый	2025-11-03	12	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
7bee7cec-757a-4120-ab4d-6eaef8e29689	aa9526c1-47a1-4cda-8ba3-1ac82917018a	555528176	album_serdechki	2025-11-03	65	10	1	0	630.00	\N	\N	15.00	10.00	630.00	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
2725096e-11c3-4539-992f-ea11a306e5d1	d3ed53c9-4b5d-4dd1-9647-f5369b4105de	556311217	альбом_папоротник_Биг	2025-11-03	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
9c997076-b0a7-42f8-93a3-39d3635002ef	1e009158-8d26-4300-8f5a-cd5c53825801	558118815	альбом _военный_краснопесчаный	2025-11-03	7	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
6d730123-c325-4fe9-b237-951a3935ba84	2ca071cb-1e7f-4a50-aebf-307d36c4e3c3	556902796	альбом_вырезанное_ сердце_ голубой	2025-11-03	7	1	0	0	0.00	\N	\N	14.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
c9a34450-226b-455b-8c33-e98cf9e3612a	3a6ebe20-a8d6-4341-9c61-870c698c7636	556311215	album_paporotnik	2025-11-03	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
62c7ec15-c154-41f5-aa80-03583e7958f8	6afe8407-24e1-4ae2-b973-859cdb04a549	558118817	album_kaktus	2025-11-03	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
95948bc0-8462-499a-91b9-f564dbbab517	98319b3c-67ff-44e7-b4fc-deb47179c6d7	558118822	album_big_hearts	2025-11-03	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
db2d8026-6803-457f-bd77-e8e4fea98b2c	b0361398-d919-47ba-8d2c-c8ba8dc852c8	555528182	album_wave_old	2025-11-03	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
1e2a827d-0728-40e8-9e8a-3e3b0a43b835	ca604ed0-475c-4d42-90b5-6b62234e0e17	555528188	album_leaves	2025-11-03	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
34ce513d-95fe-4cd3-9bd5-625e6feaf838	510f2e6e-1486-4082-91a2-154eddcd30a5	556902797	альбом_вырезанное_сердце_желтый	2025-11-03	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
c899675f-cbd7-486f-ac75-c9ba6ab63dbe	8db391e6-9ed3-489d-9cf9-ec1cbd0be071	555528177	album_big_hearts_Big	2025-11-03	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
e1116df3-75d4-48b9-80f7-44a9a780fcf8	2585e4a1-16b5-4ff0-a298-c64655faa229	556311218	альбом_сердце_Биг	2025-11-03	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
65f39246-8af4-4b16-b8de-9947869ffcb4	1e4d4fe5-14df-45a2-8d2e-bf07c078cbab	555528178	album_vetochki	2025-11-03	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
72ff2b91-20dc-418f-9907-c1c6c3ff0bb9	bbab353e-6266-44fc-b2b5-fe615d67c650	556311216	альбом_лен_голубой	2025-11-03	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
be8d9550-500d-4b24-88b0-8444ca9d75ec	31f0df7a-98a3-4c01-a5a5-f73fc359bd28	558118819	альбом_веточки_Биг	2025-11-03	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
57daeb74-50ae-4f69-8e29-c485da22249a	1a6edc21-1692-40ab-9f25-e01dd782082d	558118820	album_grey_heart	2025-11-03	5	1	0	0	0.00	\N	\N	20.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
7e2df198-f719-4819-8df2-7a5bb19682f1	707722f9-8229-4dc2-ba2a-82dac805e6a4	558118821	album_glyanec_pink	2025-11-03	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:43:52.278924+00	2025-11-13 12:43:52.278924+00
5d1ea618-0571-4906-ae9d-f8a170f2d75e	aa9526c1-47a1-4cda-8ba3-1ac82917018a	555528176	album_serdechki	2025-11-06	326	63	15	1	8096.00	\N	\N	19.00	24.00	539.73	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
c7d25ca1-a4d4-4d44-b413-a891f71fca7f	675a4e11-aafc-4d40-a112-4b4ec05e0411	467207049	korzina_big	2025-11-06	166	15	2	0	3422.00	\N	\N	9.00	13.00	1711.00	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
c54af411-6777-46e1-82cf-e3a3fac3fbdd	04d8589f-d352-4c7c-893a-6e2d41c567dd	507758935	korzina_svetlaya	2025-11-06	144	8	1	1	1711.00	\N	\N	6.00	13.00	1711.00	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
e632850f-40ff-4af2-9fea-86dda4021603	68b4da00-36ec-4cea-a8ff-367f1d27604c	467102886	korzina_tiger	2025-11-06	114	2	2	0	3072.00	\N	\N	2.00	100.00	1536.00	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
4cee5732-1bbf-4d0e-880e-af208691f692	686420b8-63de-4638-abaf-cc89e4604bd2	555528192	альбом_военный_ зеленый_2цвета	2025-11-06	58	4	1	0	900.00	\N	\N	7.00	25.00	900.00	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
20c63698-f8ab-4e19-9b5b-e7fc5e47b627	855090a0-7e1a-45c1-8403-fe70a7c64942	473520920	penal_braun_plush	2025-11-06	47	7	2	0	840.00	\N	\N	15.00	29.00	420.00	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
f0b4e5c3-d09d-4fee-80a0-bf7e88e7dbce	f2779a56-27de-4904-84db-ff5f25f3110d	473520918	penal_bezh_plush	2025-11-06	33	1	1	0	429.00	\N	\N	3.00	100.00	429.00	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
1c67d612-8f8a-4e38-9c58-b1218b01fade	61d3359d-3112-42fb-9685-73c39b74dc92	556953442	альбом_военный _зеленый_4цвета	2025-11-06	30	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
7d08a73d-a95f-4c38-9924-c16b616993b9	98319b3c-67ff-44e7-b4fc-deb47179c6d7	558118822	album_big_hearts	2025-11-06	27	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
7ba41f8f-e7a7-4339-b791-465a918f04f9	a89f8530-97cc-49d5-89b3-dd37c5370f64	467102887	korzina_mini	2025-11-06	26	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
bb571b51-daa6-4660-ba6b-5979c62e6493	2ca071cb-1e7f-4a50-aebf-307d36c4e3c3	556902796	альбом_вырезанное_ сердце_ голубой	2025-11-06	22	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
0c62f7b8-a1b3-4509-8de6-0c5c57f5dda3	2dae1d26-2788-4a1f-b439-18510a6d45de	555528179	album_glyanec_grey	2025-11-06	20	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
1be64e3c-724d-425e-8200-de047a43d0fc	2585e4a1-16b5-4ff0-a298-c64655faa229	556311218	альбом_сердце_Биг	2025-11-06	20	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
0bdd626e-ac5f-4f35-a52b-c012720403c7	d3ed53c9-4b5d-4dd1-9647-f5369b4105de	556311217	альбом_папоротник_Биг	2025-11-06	19	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
eceb256b-4cb6-4e6d-a75a-b9a324119e04	707722f9-8229-4dc2-ba2a-82dac805e6a4	558118821	album_glyanec_pink	2025-11-06	18	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
042a5292-f7c1-4f09-9d69-80e1f572718f	829b0392-47e7-44d7-a671-cd419b25d2b1	467207055	korzina_pink	2025-11-06	17	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
07e44304-dcb7-44fa-ac9e-f41e4f896c78	ba729003-0857-4378-91eb-7712858964b6	556311214	альбом_вырезанное_сердце_розовый	2025-11-06	17	2	1	0	900.00	\N	\N	12.00	50.00	900.00	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
20b9b29c-473f-4c7a-8860-76e610f29137	1a6edc21-1692-40ab-9f25-e01dd782082d	558118820	album_grey_heart	2025-11-06	17	3	0	0	0.00	\N	\N	18.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
282b98e9-597c-440b-9233-300eb5805102	31f0df7a-98a3-4c01-a5a5-f73fc359bd28	558118819	альбом_веточки_Биг	2025-11-06	16	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
95b10ae9-fa94-4f5b-8775-840296a6a277	4997211b-836f-4450-be30-b1c5f0449ac3	467207051	korzina_grey	2025-11-06	15	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
e8047d6d-4487-494e-9803-323bfd4e4f86	ca604ed0-475c-4d42-90b5-6b62234e0e17	555528188	album_leaves	2025-11-06	14	1	0	0	0.00	\N	\N	7.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
083b428e-33e9-434e-b0dd-982eddacfb2c	8db391e6-9ed3-489d-9cf9-ec1cbd0be071	555528177	album_big_hearts_Big	2025-11-06	13	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
6f819432-3d22-48fe-b306-9326de2b7123	bbab353e-6266-44fc-b2b5-fe615d67c650	556311216	альбом_лен_голубой	2025-11-06	12	1	0	0	0.00	\N	\N	8.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
0920974f-214e-4ae8-b2af-a79e2b187c09	90621643-12da-46d9-b967-8abc695e5652	473520916	penal_pink_cat	2025-11-06	11	1	0	0	0.00	\N	\N	9.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
2961c447-d65c-4f2a-8038-34b36686546f	1e009158-8d26-4300-8f5a-cd5c53825801	558118815	альбом _военный_краснопесчаный	2025-11-06	11	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
e2c4da2c-7e71-4fb2-9af9-6aef4d3bd3c6	b0361398-d919-47ba-8d2c-c8ba8dc852c8	555528182	album_wave_old	2025-11-06	10	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
7353ccca-543c-4e8a-a128-8378c5c427b0	a6b9bc34-aa25-4f84-aa91-f1d303c8643b	473520914	penal_black_cat	2025-11-06	9	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
e716a52b-0eea-4f45-aebd-5b3373c21a65	8dda1b27-c73e-4a12-a3ea-e36ffae2c0b8	473520919	penal_black_plush	2025-11-06	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
5e3d49cd-a4cb-4f31-98d0-2252d9111e3f	531c3bfe-7a15-4d8f-aefe-6981fe3a179e	555528187	album_flamingo	2025-11-06	8	1	0	0	0.00	\N	\N	13.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
0b0b6296-e766-4c2f-9031-ec06760ed3d9	3a6ebe20-a8d6-4341-9c61-870c698c7636	556311215	album_paporotnik	2025-11-06	8	1	0	0	0.00	\N	\N	13.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
522f5501-346c-4eae-9fc2-289186c80c13	510f2e6e-1486-4082-91a2-154eddcd30a5	556902797	альбом_вырезанное_сердце_желтый	2025-11-06	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
060f6b5a-aba9-45fc-9a3a-6404b06870b0	6afe8407-24e1-4ae2-b973-859cdb04a549	558118817	album_kaktus	2025-11-06	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
c6f065af-6190-459b-b6a0-136ae7a338ea	d28d47c9-76eb-406d-af4a-17fbb8066281	473520915	penal_blue_cat	2025-11-06	7	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
5abaa31e-eede-4856-bc14-41e69cc17b89	622ab6c1-6d24-44c7-90a5-1a0d66d33768	473520917	penal_white_plush	2025-11-06	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
339a4d25-9826-4fff-a533-c10b65b7c654	1e4d4fe5-14df-45a2-8d2e-bf07c078cbab	555528178	album_vetochki	2025-11-06	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
921f9b67-6c2d-4fec-a38a-b8dc280d2804	2e67e4ac-1929-40a1-9f86-2c6dc73d548d	558118816	альбом_военный _коричневый	2025-11-06	4	1	0	0	0.00	\N	\N	25.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
1c7a65a8-7844-4807-9895-8dc3d57e084a	2b1ecf5a-ec30-4c27-9ea4-675f09dda3c9	472859758	holder_pink	2025-11-06	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.636717+00	2025-11-13 12:44:11.636717+00
d0d9126e-62e6-4c21-b943-ed6a15a776b1	aa9526c1-47a1-4cda-8ba3-1ac82917018a	555528176	album_serdechki	2025-11-05	38	6	1	0	630.00	\N	\N	16.00	17.00	630.00	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
fff92458-44d1-479d-9194-71f37a07f3ed	675a4e11-aafc-4d40-a112-4b4ec05e0411	467207049	korzina_big	2025-11-05	179	18	4	1	6844.00	\N	\N	10.00	22.00	1711.00	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
e780c40a-b717-4faa-8eea-0c10612083ce	04d8589f-d352-4c7c-893a-6e2d41c567dd	507758935	korzina_svetlaya	2025-11-05	140	9	1	0	1711.00	\N	\N	6.00	11.00	1711.00	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
db6d4b4e-0d1e-45ca-aa4e-afedaf5e3eb2	68b4da00-36ec-4cea-a8ff-367f1d27604c	467102886	korzina_tiger	2025-11-05	100	8	2	0	3072.00	\N	\N	8.00	25.00	1536.00	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
9415db67-7476-411c-b931-b292982017ee	686420b8-63de-4638-abaf-cc89e4604bd2	555528192	альбом_военный_ зеленый_2цвета	2025-11-05	38	3	0	0	0.00	\N	\N	8.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
5a0697e3-d479-46fc-bc45-e45b05893fa0	855090a0-7e1a-45c1-8403-fe70a7c64942	473520920	penal_braun_plush	2025-11-05	28	2	1	0	494.00	\N	\N	7.00	50.00	494.00	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
e898d840-21bd-46e6-850d-2e8c4828f358	f2779a56-27de-4904-84db-ff5f25f3110d	473520918	penal_bezh_plush	2025-11-05	54	8	0	0	0.00	\N	\N	15.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
99ffced5-2f8e-4273-a79f-b87caf9af379	61d3359d-3112-42fb-9685-73c39b74dc92	556953442	альбом_военный _зеленый_4цвета	2025-11-05	15	1	0	0	0.00	\N	\N	7.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
50d95fcc-b0a5-4acc-8ff3-826991eb1d68	98319b3c-67ff-44e7-b4fc-deb47179c6d7	558118822	album_big_hearts	2025-11-05	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
0fdd2901-0b47-45f7-a568-99873a02d89e	a89f8530-97cc-49d5-89b3-dd37c5370f64	467102887	korzina_mini	2025-11-05	27	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
ef9cadcc-4457-4dcc-a205-1731b362c9b5	2ca071cb-1e7f-4a50-aebf-307d36c4e3c3	556902796	альбом_вырезанное_ сердце_ голубой	2025-11-05	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
4a2591be-9b5a-4161-a023-734903e8c63f	2dae1d26-2788-4a1f-b439-18510a6d45de	555528179	album_glyanec_grey	2025-11-05	35	2	2	0	5820.00	\N	\N	6.00	100.00	2910.00	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
c617e060-474c-4ef5-b520-43318fc12f14	2585e4a1-16b5-4ff0-a298-c64655faa229	556311218	альбом_сердце_Биг	2025-11-05	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
5912d527-c796-4739-9ab5-e1ad88aa0c89	d3ed53c9-4b5d-4dd1-9647-f5369b4105de	556311217	альбом_папоротник_Биг	2025-11-05	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
1c9cab03-2052-4155-9b89-44de9fb4c0b8	707722f9-8229-4dc2-ba2a-82dac805e6a4	558118821	album_glyanec_pink	2025-11-05	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
c0b237a7-ff17-43ec-b897-717019fd1abd	829b0392-47e7-44d7-a671-cd419b25d2b1	467207055	korzina_pink	2025-11-05	13	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
c4602387-264a-44b7-ab2d-33162fcc1a23	ba729003-0857-4378-91eb-7712858964b6	556311214	альбом_вырезанное_сердце_розовый	2025-11-05	13	1	1	0	900.00	\N	\N	8.00	100.00	900.00	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
a31cb34b-ebc4-4891-90ec-aab98d1268ce	1a6edc21-1692-40ab-9f25-e01dd782082d	558118820	album_grey_heart	2025-11-05	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
23390d65-7964-4258-b0c8-b5c26f7865f6	31f0df7a-98a3-4c01-a5a5-f73fc359bd28	558118819	альбом_веточки_Биг	2025-11-05	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
7afadfce-5eb9-48d7-9824-6a47fc0d008f	4997211b-836f-4450-be30-b1c5f0449ac3	467207051	korzina_grey	2025-11-05	16	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
efe273f5-63fe-4d3d-8c0d-981d545eb61d	ca604ed0-475c-4d42-90b5-6b62234e0e17	555528188	album_leaves	2025-11-05	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
a898476b-d38a-4caf-bb18-8d5196ab5a76	8db391e6-9ed3-489d-9cf9-ec1cbd0be071	555528177	album_big_hearts_Big	2025-11-05	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
a7a421d0-4004-45fb-9409-f70095ecb02f	bbab353e-6266-44fc-b2b5-fe615d67c650	556311216	альбом_лен_голубой	2025-11-05	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
b4442235-1470-48b2-8ed5-26200b74f586	90621643-12da-46d9-b967-8abc695e5652	473520916	penal_pink_cat	2025-11-05	13	1	1	0	416.00	\N	\N	8.00	100.00	416.00	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
4597626b-1415-40fe-9b3a-628372378e91	1e009158-8d26-4300-8f5a-cd5c53825801	558118815	альбом _военный_краснопесчаный	2025-11-05	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
648cefa3-aa79-4df2-9a85-c6a7bb6e56bc	b0361398-d919-47ba-8d2c-c8ba8dc852c8	555528182	album_wave_old	2025-11-05	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
0b100cfa-3ba2-4efb-92be-7c03400a018f	a6b9bc34-aa25-4f84-aa91-f1d303c8643b	473520914	penal_black_cat	2025-11-05	13	1	1	0	429.00	\N	\N	8.00	100.00	429.00	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
da5a1bd1-d7da-4b15-bed8-96c5b9273bd8	8dda1b27-c73e-4a12-a3ea-e36ffae2c0b8	473520919	penal_black_plush	2025-11-05	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
0be8b93f-c6e1-4042-a646-e67c824c8bfe	531c3bfe-7a15-4d8f-aefe-6981fe3a179e	555528187	album_flamingo	2025-11-05	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
31faf52a-6e40-4807-a49e-48f3de997b38	3a6ebe20-a8d6-4341-9c61-870c698c7636	556311215	album_paporotnik	2025-11-05	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
de30ee5a-b12e-4425-aa2a-cb0cb6db024b	510f2e6e-1486-4082-91a2-154eddcd30a5	556902797	альбом_вырезанное_сердце_желтый	2025-11-05	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
f487e216-dedc-4c79-afa9-97bb67767621	6afe8407-24e1-4ae2-b973-859cdb04a549	558118817	album_kaktus	2025-11-05	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
e585a374-1677-4f6f-b0a9-54c60eb17f30	d28d47c9-76eb-406d-af4a-17fbb8066281	473520915	penal_blue_cat	2025-11-05	9	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
ed866634-354d-419a-8810-b1643e56aec4	622ab6c1-6d24-44c7-90a5-1a0d66d33768	473520917	penal_white_plush	2025-11-05	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
b2e13d43-bbc6-4a67-9e3d-34863e902a21	1e4d4fe5-14df-45a2-8d2e-bf07c078cbab	555528178	album_vetochki	2025-11-05	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
71c35a47-c4ce-44be-96cf-554e6982a202	2e67e4ac-1929-40a1-9f86-2c6dc73d548d	558118816	альбом_военный _коричневый	2025-11-05	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
fbf1bdc1-f9dd-48fa-bae1-b0b8e3cb607f	2b1ecf5a-ec30-4c27-9ea4-675f09dda3c9	472859758	holder_pink	2025-11-05	0	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:11.646791+00	2025-11-13 12:44:11.646791+00
4f2a826d-68fb-439f-b3df-3489173c050b	675a4e11-aafc-4d40-a112-4b4ec05e0411	467207049	korzina_big	2025-11-08	249	16	4	1	6902.00	\N	\N	6.00	25.00	1725.50	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
e57dc345-f655-4eac-a0b2-250d65a36be5	aa9526c1-47a1-4cda-8ba3-1ac82917018a	555528176	album_serdechki	2025-11-08	156	34	9	1	5013.00	\N	\N	22.00	26.00	557.00	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
4dbbed65-2345-4f6c-ae81-8fae72000bc8	68b4da00-36ec-4cea-a8ff-367f1d27604c	467102886	korzina_tiger	2025-11-08	118	7	2	0	3072.00	\N	\N	6.00	29.00	1536.00	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
8d3d79da-fc48-47c5-8e0f-11a98785c73d	04d8589f-d352-4c7c-893a-6e2d41c567dd	507758935	korzina_svetlaya	2025-11-08	93	11	2	0	3481.00	\N	\N	12.00	18.00	1740.50	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
19d97679-cd14-4e9d-b51d-175853c44913	855090a0-7e1a-45c1-8403-fe70a7c64942	473520920	penal_braun_plush	2025-11-08	60	13	3	0	1260.00	\N	\N	22.00	23.00	420.00	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
57ffaaf9-b7d6-41be-8823-0dac73e1708c	686420b8-63de-4638-abaf-cc89e4604bd2	555528192	альбом_военный_ зеленый_2цвета	2025-11-08	59	8	3	0	2700.00	\N	\N	14.00	38.00	900.00	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
894fabfc-3481-40bd-9ed5-48c06aa32e29	f2779a56-27de-4904-84db-ff5f25f3110d	473520918	penal_bezh_plush	2025-11-08	29	2	0	0	0.00	\N	\N	7.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
475a06ef-9ca9-4733-b32f-ffe417cfeeea	61d3359d-3112-42fb-9685-73c39b74dc92	556953442	альбом_военный _зеленый_4цвета	2025-11-08	28	1	1	0	900.00	\N	\N	4.00	100.00	900.00	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
38c2b730-a83c-4330-ab9c-b69bc69754ef	a89f8530-97cc-49d5-89b3-dd37c5370f64	467102887	korzina_mini	2025-11-08	22	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
5f32354c-8215-40ce-9d83-66817449da09	4997211b-836f-4450-be30-b1c5f0449ac3	467207051	korzina_grey	2025-11-08	22	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
31604a3d-1012-41ad-82dc-e8d2464c385b	d3ed53c9-4b5d-4dd1-9647-f5369b4105de	556311217	альбом_папоротник_Биг	2025-11-08	22	1	0	0	0.00	\N	\N	5.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
c2ee53b5-b36b-49c6-9573-9f989310afc2	829b0392-47e7-44d7-a671-cd419b25d2b1	467207055	korzina_pink	2025-11-08	19	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
92780a5a-e3cb-41ca-ad8c-50260c5f7ccd	2dae1d26-2788-4a1f-b439-18510a6d45de	555528179	album_glyanec_grey	2025-11-08	18	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
ed320be3-105b-4def-bd6b-bc01e2b43a79	a6b9bc34-aa25-4f84-aa91-f1d303c8643b	473520914	penal_black_cat	2025-11-08	16	2	0	0	0.00	\N	\N	13.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
b4a42f7b-fb47-4f7d-beeb-f3e92e50e80f	d28d47c9-76eb-406d-af4a-17fbb8066281	473520915	penal_blue_cat	2025-11-08	15	2	0	0	0.00	\N	\N	13.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
8fc1ae3e-fbae-4df7-85f0-27d33eb2f25c	90621643-12da-46d9-b967-8abc695e5652	473520916	penal_pink_cat	2025-11-08	14	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
26d5e30b-6783-43b4-96ed-731cd8a07d62	98319b3c-67ff-44e7-b4fc-deb47179c6d7	558118822	album_big_hearts	2025-11-08	13	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
790c920f-1541-4bf5-9ad2-0881c7dd036a	2ca071cb-1e7f-4a50-aebf-307d36c4e3c3	556902796	альбом_вырезанное_ сердце_ голубой	2025-11-08	12	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
8074e391-db01-41c0-8576-76fdba5b1b08	bbab353e-6266-44fc-b2b5-fe615d67c650	556311216	альбом_лен_голубой	2025-11-08	11	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
2231da9e-2350-4148-95c0-d50ca3a48792	1e009158-8d26-4300-8f5a-cd5c53825801	558118815	альбом _военный_краснопесчаный	2025-11-08	11	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
173dd451-b143-4ab1-8228-de6926645032	8db391e6-9ed3-489d-9cf9-ec1cbd0be071	555528177	album_big_hearts_Big	2025-11-08	10	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
fcdfa187-2700-464a-ad81-7863d1c68256	ba729003-0857-4378-91eb-7712858964b6	556311214	альбом_вырезанное_сердце_розовый	2025-11-08	10	2	0	0	0.00	\N	\N	20.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
6784e294-da54-4f4d-ab78-cf5b2f3fd9bb	8dda1b27-c73e-4a12-a3ea-e36ffae2c0b8	473520919	penal_black_plush	2025-11-08	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
c271ea3f-f02a-4623-be83-ec158a2bd550	1a6edc21-1692-40ab-9f25-e01dd782082d	558118820	album_grey_heart	2025-11-08	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
c5756760-4490-48d3-afb8-e38b2734cd42	3a6ebe20-a8d6-4341-9c61-870c698c7636	556311215	album_paporotnik	2025-11-08	7	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
ad7f731d-4c08-4846-809b-517396ef9e61	2e67e4ac-1929-40a1-9f86-2c6dc73d548d	558118816	альбом_военный _коричневый	2025-11-08	7	1	0	0	0.00	\N	\N	14.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
1bc0757b-ab60-4fd2-a0a2-4db244573bdc	1e4d4fe5-14df-45a2-8d2e-bf07c078cbab	555528178	album_vetochki	2025-11-08	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
008e4947-51ac-4804-ab8c-b40a45c86bd1	531c3bfe-7a15-4d8f-aefe-6981fe3a179e	555528187	album_flamingo	2025-11-08	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
fd34b505-f248-4281-a2d6-7058434ea801	6afe8407-24e1-4ae2-b973-859cdb04a549	558118817	album_kaktus	2025-11-08	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
bf96b019-3831-46ef-a083-c9d6c643c97a	707722f9-8229-4dc2-ba2a-82dac805e6a4	558118821	album_glyanec_pink	2025-11-08	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
895330bc-e057-410f-9e63-8bb552a0ed16	622ab6c1-6d24-44c7-90a5-1a0d66d33768	473520917	penal_white_plush	2025-11-08	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
f55553bf-45a7-4350-a0f7-a2705c34da17	b0361398-d919-47ba-8d2c-c8ba8dc852c8	555528182	album_wave_old	2025-11-08	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
361c8b76-8fff-4530-a779-396cceebccc7	ca604ed0-475c-4d42-90b5-6b62234e0e17	555528188	album_leaves	2025-11-08	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
886e7c3a-95fd-45f9-b600-1c74cef65f61	2585e4a1-16b5-4ff0-a298-c64655faa229	556311218	альбом_сердце_Биг	2025-11-08	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
923e8963-a864-497f-a52e-9b3736e4664c	31f0df7a-98a3-4c01-a5a5-f73fc359bd28	558118819	альбом_веточки_Биг	2025-11-08	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
06a5d5e3-ef12-4758-9376-c86589ddabe8	510f2e6e-1486-4082-91a2-154eddcd30a5	556902797	альбом_вырезанное_сердце_желтый	2025-11-08	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.188342+00	2025-11-13 12:44:34.188342+00
fb95dc5f-c426-4920-912f-485b07060e86	675a4e11-aafc-4d40-a112-4b4ec05e0411	467207049	korzina_big	2025-11-07	185	10	5	2	8555.00	\N	\N	5.00	50.00	1711.00	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
59f072b6-0865-4fb6-876e-c986cf49dac2	aa9526c1-47a1-4cda-8ba3-1ac82917018a	555528176	album_serdechki	2025-11-07	135	22	7	1	3885.00	\N	\N	16.00	32.00	555.00	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
9a2e87bf-2e78-4948-ae79-e023225bc0fa	68b4da00-36ec-4cea-a8ff-367f1d27604c	467102886	korzina_tiger	2025-11-07	117	13	3	0	4608.00	\N	\N	11.00	23.00	1536.00	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
adef0233-4744-4316-a44b-7bf5062368b4	04d8589f-d352-4c7c-893a-6e2d41c567dd	507758935	korzina_svetlaya	2025-11-07	145	11	4	0	6844.00	\N	\N	8.00	36.00	1711.00	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
ac35328e-635a-4e81-a7d7-57d1c3caf94b	855090a0-7e1a-45c1-8403-fe70a7c64942	473520920	penal_braun_plush	2025-11-07	30	2	0	0	0.00	\N	\N	7.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
1000f11c-69f2-4c2a-bc26-cc75513c7341	686420b8-63de-4638-abaf-cc89e4604bd2	555528192	альбом_военный_ зеленый_2цвета	2025-11-07	24	2	0	0	0.00	\N	\N	8.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
ea452865-4841-4327-87af-ec87e5f0749d	f2779a56-27de-4904-84db-ff5f25f3110d	473520918	penal_bezh_plush	2025-11-07	32	3	0	0	0.00	\N	\N	9.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
3f05efd5-3d4c-4467-b0ac-17f83fab5a20	61d3359d-3112-42fb-9685-73c39b74dc92	556953442	альбом_военный _зеленый_4цвета	2025-11-07	12	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
6457be58-e945-4314-bf41-9263e0bb161d	a89f8530-97cc-49d5-89b3-dd37c5370f64	467102887	korzina_mini	2025-11-07	19	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
4e0bb42c-8527-4ed9-b6eb-973cc8f1a25b	4997211b-836f-4450-be30-b1c5f0449ac3	467207051	korzina_grey	2025-11-07	14	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
f43a7291-b4d8-4643-8e98-d5b1c2788479	d3ed53c9-4b5d-4dd1-9647-f5369b4105de	556311217	альбом_папоротник_Биг	2025-11-07	14	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
949c33b9-d065-4910-b1db-5ef54ef1ae7f	829b0392-47e7-44d7-a671-cd419b25d2b1	467207055	korzina_pink	2025-11-07	14	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
452a6453-9a92-4574-ac28-5ae424f05d81	2dae1d26-2788-4a1f-b439-18510a6d45de	555528179	album_glyanec_grey	2025-11-07	21	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
dbc2c27e-1e2e-4753-b1e8-59734b03d078	a6b9bc34-aa25-4f84-aa91-f1d303c8643b	473520914	penal_black_cat	2025-11-07	15	2	0	0	0.00	\N	\N	13.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
463a7618-2d9b-42a6-833c-6329b6acdd21	d28d47c9-76eb-406d-af4a-17fbb8066281	473520915	penal_blue_cat	2025-11-07	24	2	0	0	0.00	\N	\N	8.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
cfee7523-a655-4807-a2ad-bae4a4d2642b	90621643-12da-46d9-b967-8abc695e5652	473520916	penal_pink_cat	2025-11-07	19	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
a99dc0c9-d500-431c-96d6-5aa07d18d0c3	98319b3c-67ff-44e7-b4fc-deb47179c6d7	558118822	album_big_hearts	2025-11-07	13	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
2fa9c7b7-50ac-4c6b-bd34-f870ba218039	2ca071cb-1e7f-4a50-aebf-307d36c4e3c3	556902796	альбом_вырезанное_ сердце_ голубой	2025-11-07	8	1	0	0	0.00	\N	\N	13.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
149adb60-acf8-40f6-ba5c-ec5cfcc999cd	bbab353e-6266-44fc-b2b5-fe615d67c650	556311216	альбом_лен_голубой	2025-11-07	6	1	0	0	0.00	\N	\N	17.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
b31f13d4-46c4-4c28-b6df-c399e0e785b8	1e009158-8d26-4300-8f5a-cd5c53825801	558118815	альбом _военный_краснопесчаный	2025-11-07	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
01639814-abb4-42a3-9e31-32970863e691	8db391e6-9ed3-489d-9cf9-ec1cbd0be071	555528177	album_big_hearts_Big	2025-11-07	10	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
cb29901d-09e1-49c7-a23c-d91741d48efc	ba729003-0857-4378-91eb-7712858964b6	556311214	альбом_вырезанное_сердце_розовый	2025-11-07	11	1	0	0	0.00	\N	\N	9.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
7e0e8fee-99b7-40ba-8c6a-498a9fd7ec33	8dda1b27-c73e-4a12-a3ea-e36ffae2c0b8	473520919	penal_black_plush	2025-11-07	7	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
482ac6dc-d397-48a4-8f6d-ae970f3576a3	1a6edc21-1692-40ab-9f25-e01dd782082d	558118820	album_grey_heart	2025-11-07	10	1	0	0	0.00	\N	\N	10.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
68842fe7-78e8-4de5-98b4-acb9269d3888	3a6ebe20-a8d6-4341-9c61-870c698c7636	556311215	album_paporotnik	2025-11-07	3	0	1	0	780.00	\N	\N	0.00	0.00	780.00	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
9b095558-2fb2-4ab7-be8d-5536fc8477a6	2e67e4ac-1929-40a1-9f86-2c6dc73d548d	558118816	альбом_военный _коричневый	2025-11-07	8	1	0	0	0.00	\N	\N	13.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
281081c5-a5c6-451d-80a9-34bbb125b6eb	1e4d4fe5-14df-45a2-8d2e-bf07c078cbab	555528178	album_vetochki	2025-11-07	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
13202f8b-5744-4d6e-9782-11ce759bf790	531c3bfe-7a15-4d8f-aefe-6981fe3a179e	555528187	album_flamingo	2025-11-07	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
016ed17f-cbba-4b13-9a6e-15de77888aee	6afe8407-24e1-4ae2-b973-859cdb04a549	558118817	album_kaktus	2025-11-07	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
fce75b2d-047f-4eb8-9d4e-726769a43699	707722f9-8229-4dc2-ba2a-82dac805e6a4	558118821	album_glyanec_pink	2025-11-07	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
8a62084d-063a-438c-bed6-208dd9fb92d8	622ab6c1-6d24-44c7-90a5-1a0d66d33768	473520917	penal_white_plush	2025-11-07	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
d477a60e-1829-4694-b986-c89a20ef1fb3	b0361398-d919-47ba-8d2c-c8ba8dc852c8	555528182	album_wave_old	2025-11-07	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
4aa90b1a-f752-450a-a40f-05a1f3f86269	ca604ed0-475c-4d42-90b5-6b62234e0e17	555528188	album_leaves	2025-11-07	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
20455ce3-b77f-40ae-b4ee-c9192c6465cf	2585e4a1-16b5-4ff0-a298-c64655faa229	556311218	альбом_сердце_Биг	2025-11-07	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
d1c42451-8ac2-459b-97b6-c2d0b99803f6	31f0df7a-98a3-4c01-a5a5-f73fc359bd28	558118819	альбом_веточки_Биг	2025-11-07	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
0d246815-2f68-483b-b8dc-1e99835771b6	510f2e6e-1486-4082-91a2-154eddcd30a5	556902797	альбом_вырезанное_сердце_желтый	2025-11-07	1	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:34.200859+00	2025-11-13 12:44:34.200859+00
9d42e99e-4715-4b56-8121-f81943c6b7ef	aa9526c1-47a1-4cda-8ba3-1ac82917018a	555528176	album_serdechki	2025-11-10	479	74	16	0	8912.00	\N	\N	15.00	22.00	557.00	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
406614a9-e189-41aa-8afb-020973a3ebc2	675a4e11-aafc-4d40-a112-4b4ec05e0411	467207049	korzina_big	2025-11-10	236	19	3	0	5307.00	\N	\N	8.00	16.00	1769.00	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
ea525c34-4116-4b99-87f1-2cc8b4c4e864	68b4da00-36ec-4cea-a8ff-367f1d27604c	467102886	korzina_tiger	2025-11-10	126	7	2	0	3120.00	\N	\N	6.00	29.00	1560.00	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
0a8c92e8-89c8-4eac-a3c4-9164b21bfe26	686420b8-63de-4638-abaf-cc89e4604bd2	555528192	альбом_военный_ зеленый_2цвета	2025-11-10	85	4	2	0	1800.00	\N	\N	5.00	50.00	900.00	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
01520c75-133b-48f1-a345-318650c387b3	98319b3c-67ff-44e7-b4fc-deb47179c6d7	558118822	album_big_hearts	2025-11-10	52	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
848398e8-f663-42fa-a1ba-0d3f1b26fad5	61d3359d-3112-42fb-9685-73c39b74dc92	556953442	альбом_военный _зеленый_4цвета	2025-11-10	47	2	1	0	900.00	\N	\N	4.00	50.00	900.00	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
d06c71ad-7235-455d-b9bb-6df4491f07ec	855090a0-7e1a-45c1-8403-fe70a7c64942	473520920	penal_braun_plush	2025-11-10	44	5	2	1	840.00	\N	\N	11.00	40.00	420.00	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
0fc10d08-6e84-431a-b18b-156a60031576	ba729003-0857-4378-91eb-7712858964b6	556311214	альбом_вырезанное_сердце_розовый	2025-11-10	41	4	4	0	3600.00	\N	\N	10.00	100.00	900.00	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
727b9a16-68b9-4240-8d72-ad740424353e	2ca071cb-1e7f-4a50-aebf-307d36c4e3c3	556902796	альбом_вырезанное_ сердце_ голубой	2025-11-10	38	2	0	0	0.00	\N	\N	5.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
29b52ad5-0e6d-49f9-b86c-7b8b10344859	8db391e6-9ed3-489d-9cf9-ec1cbd0be071	555528177	album_big_hearts_Big	2025-11-10	37	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
5d00f988-c8eb-40ef-b75a-4de709d49c63	2dae1d26-2788-4a1f-b439-18510a6d45de	555528179	album_glyanec_grey	2025-11-10	33	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
91686331-b948-4177-9c6a-9caa12ec58a9	d3ed53c9-4b5d-4dd1-9647-f5369b4105de	556311217	альбом_папоротник_Биг	2025-11-10	33	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
f6fa1130-1296-4d6d-a5b7-0a066e07b75a	ca604ed0-475c-4d42-90b5-6b62234e0e17	555528188	album_leaves	2025-11-10	28	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
9ae1d9f4-7ab3-4eec-a12f-bb31adf48d4e	3a6ebe20-a8d6-4341-9c61-870c698c7636	556311215	album_paporotnik	2025-11-10	27	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
8fe2778a-7f98-436f-aaf1-26a043b23b57	2585e4a1-16b5-4ff0-a298-c64655faa229	556311218	альбом_сердце_Биг	2025-11-10	27	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
a9270084-71b6-4117-aa35-c2914e75865f	04d8589f-d352-4c7c-893a-6e2d41c567dd	507758935	korzina_svetlaya	2025-11-10	25	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
8df2cf96-0a25-49a3-b2bc-bc2d4c2a1408	1a6edc21-1692-40ab-9f25-e01dd782082d	558118820	album_grey_heart	2025-11-10	25	1	0	0	0.00	\N	\N	4.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
dcc5e528-bef1-4d37-9fff-91b69a824fef	829b0392-47e7-44d7-a671-cd419b25d2b1	467207055	korzina_pink	2025-11-10	21	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
96228013-fcb9-46c4-a678-d19449bca11c	a6b9bc34-aa25-4f84-aa91-f1d303c8643b	473520914	penal_black_cat	2025-11-10	21	1	1	0	429.00	\N	\N	5.00	100.00	429.00	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
8196e0da-26ff-4f70-9efd-ba90a6967ea2	6afe8407-24e1-4ae2-b973-859cdb04a549	558118817	album_kaktus	2025-11-10	21	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
2149f330-585f-4b67-b45e-d24215cb76fc	a89f8530-97cc-49d5-89b3-dd37c5370f64	467102887	korzina_mini	2025-11-10	20	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
ea45ea33-bada-432c-96e9-56432bd949e1	b0361398-d919-47ba-8d2c-c8ba8dc852c8	555528182	album_wave_old	2025-11-10	20	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
f0e9ff09-1391-4835-bc00-e7ccda3f879e	2e67e4ac-1929-40a1-9f86-2c6dc73d548d	558118816	альбом_военный _коричневый	2025-11-10	20	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
82aa6636-0b3f-4008-9f05-a970541bd280	31f0df7a-98a3-4c01-a5a5-f73fc359bd28	558118819	альбом_веточки_Биг	2025-11-10	20	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
aeef33ad-05f8-4df4-94e0-c14fd0689eda	531c3bfe-7a15-4d8f-aefe-6981fe3a179e	555528187	album_flamingo	2025-11-10	19	1	0	0	0.00	\N	\N	5.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
9ce75e75-7394-487a-a6ee-4329a566d46b	707722f9-8229-4dc2-ba2a-82dac805e6a4	558118821	album_glyanec_pink	2025-11-10	19	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
c305b976-d48b-48f5-9a51-5e0afd83fd61	1e4d4fe5-14df-45a2-8d2e-bf07c078cbab	555528178	album_vetochki	2025-11-10	18	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
e591078e-4ad0-4107-a38b-3f41ed6aed55	4997211b-836f-4450-be30-b1c5f0449ac3	467207051	korzina_grey	2025-11-10	17	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
643add61-0091-4527-a79e-5ca7a30611ec	f2779a56-27de-4904-84db-ff5f25f3110d	473520918	penal_bezh_plush	2025-11-10	17	1	1	0	429.00	\N	\N	6.00	100.00	429.00	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
9bda0ce0-6c7f-4d10-bec5-499e184543b3	bbab353e-6266-44fc-b2b5-fe615d67c650	556311216	альбом_лен_голубой	2025-11-10	17	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
c105ed8d-d269-4e5f-b854-4f5c29f493cd	d28d47c9-76eb-406d-af4a-17fbb8066281	473520915	penal_blue_cat	2025-11-10	16	1	0	0	0.00	\N	\N	6.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
47b739fa-5e88-4abc-a495-fe9e42610a0f	1e009158-8d26-4300-8f5a-cd5c53825801	558118815	альбом _военный_краснопесчаный	2025-11-10	16	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
a4baca25-13d9-4199-beba-09981408b088	510f2e6e-1486-4082-91a2-154eddcd30a5	556902797	альбом_вырезанное_сердце_желтый	2025-11-10	13	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
addb7c0b-d2cf-4e69-90de-841e1c94e3bd	90621643-12da-46d9-b967-8abc695e5652	473520916	penal_pink_cat	2025-11-10	10	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
f3c85351-2669-4f44-9eae-07604e639fee	622ab6c1-6d24-44c7-90a5-1a0d66d33768	473520917	penal_white_plush	2025-11-10	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
7907e7aa-94f4-4e1a-a1fa-613591758435	8dda1b27-c73e-4a12-a3ea-e36ffae2c0b8	473520919	penal_black_plush	2025-11-10	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.748025+00	2025-11-13 12:44:55.748025+00
0670370d-4c73-4753-b75d-ae8c4ed12793	aa9526c1-47a1-4cda-8ba3-1ac82917018a	555528176	album_serdechki	2025-11-09	329	68	13	2	7241.00	\N	\N	21.00	19.00	557.00	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
98f2f291-be31-47e0-a706-6ec6cd219c7d	675a4e11-aafc-4d40-a112-4b4ec05e0411	467207049	korzina_big	2025-11-09	245	16	2	0	3538.00	\N	\N	7.00	13.00	1769.00	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
15d8881b-35e0-4d5a-aae9-5e148a38121b	68b4da00-36ec-4cea-a8ff-367f1d27604c	467102886	korzina_tiger	2025-11-09	144	6	2	0	3120.00	\N	\N	4.00	33.00	1560.00	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
1baa23a6-fa92-432c-b771-d6de1966aca6	686420b8-63de-4638-abaf-cc89e4604bd2	555528192	альбом_военный_ зеленый_2цвета	2025-11-09	86	10	2	0	1800.00	\N	\N	12.00	20.00	900.00	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
3e1170d1-76c4-4465-be65-05df92c5ff70	98319b3c-67ff-44e7-b4fc-deb47179c6d7	558118822	album_big_hearts	2025-11-09	33	1	0	0	0.00	\N	\N	3.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
73132986-9d76-4349-90bc-70335e2c4ea7	61d3359d-3112-42fb-9685-73c39b74dc92	556953442	альбом_военный _зеленый_4цвета	2025-11-09	42	1	0	0	0.00	\N	\N	2.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
24731d90-08e1-4695-944f-53c924a7757b	855090a0-7e1a-45c1-8403-fe70a7c64942	473520920	penal_braun_plush	2025-11-09	96	14	3	0	1260.00	\N	\N	15.00	21.00	420.00	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
4dbe008a-ba25-47df-8db4-35fefa4ed178	ba729003-0857-4378-91eb-7712858964b6	556311214	альбом_вырезанное_сердце_розовый	2025-11-09	26	2	0	0	0.00	\N	\N	8.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
204f8c5a-d90c-4c07-94d2-ccedbafbf6dc	2ca071cb-1e7f-4a50-aebf-307d36c4e3c3	556902796	альбом_вырезанное_ сердце_ голубой	2025-11-09	27	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
baec7899-2364-41e2-b38d-6bf65b2ad7e5	8db391e6-9ed3-489d-9cf9-ec1cbd0be071	555528177	album_big_hearts_Big	2025-11-09	24	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
c65d56f9-02d2-46aa-971e-65511ddd835c	2dae1d26-2788-4a1f-b439-18510a6d45de	555528179	album_glyanec_grey	2025-11-09	36	3	0	0	0.00	\N	\N	8.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
8abc84f4-925b-4e33-a45d-2cbd06a8df56	d3ed53c9-4b5d-4dd1-9647-f5369b4105de	556311217	альбом_папоротник_Биг	2025-11-09	32	1	0	0	0.00	\N	\N	3.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
a0581f42-20ae-43a0-902e-03595b17ce29	ca604ed0-475c-4d42-90b5-6b62234e0e17	555528188	album_leaves	2025-11-09	16	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
338325a4-b602-4ee4-afd8-2a0a015efd8b	3a6ebe20-a8d6-4341-9c61-870c698c7636	556311215	album_paporotnik	2025-11-09	22	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
95748775-fac2-4563-aa24-9c7f24f3f8d7	2585e4a1-16b5-4ff0-a298-c64655faa229	556311218	альбом_сердце_Биг	2025-11-09	23	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
6a24f5e5-c273-439e-8bb1-e1d00f08c7ad	04d8589f-d352-4c7c-893a-6e2d41c567dd	507758935	korzina_svetlaya	2025-11-09	49	1	2	0	3654.00	\N	\N	2.00	200.00	1827.00	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
a316ec25-8690-4359-bb05-4737228edba1	1a6edc21-1692-40ab-9f25-e01dd782082d	558118820	album_grey_heart	2025-11-09	24	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
987aac4a-dd17-4108-a625-9673832bc235	829b0392-47e7-44d7-a671-cd419b25d2b1	467207055	korzina_pink	2025-11-09	16	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
b61d7fc1-cbce-4027-a8d3-ff708870de91	a6b9bc34-aa25-4f84-aa91-f1d303c8643b	473520914	penal_black_cat	2025-11-09	10	1	1	0	429.00	\N	\N	10.00	100.00	429.00	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
1236d5e8-d50e-45ca-a411-72db2fb2eefb	6afe8407-24e1-4ae2-b973-859cdb04a549	558118817	album_kaktus	2025-11-09	13	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
0161a4ad-cbc2-4e42-a528-bfaa266b6c19	a89f8530-97cc-49d5-89b3-dd37c5370f64	467102887	korzina_mini	2025-11-09	36	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
bd22a295-ab5a-4967-b339-cf8f2de73cfd	b0361398-d919-47ba-8d2c-c8ba8dc852c8	555528182	album_wave_old	2025-11-09	17	1	1	0	780.00	\N	\N	6.00	100.00	780.00	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
4cfa3cde-5be9-4ff5-9cf0-d326e975c151	2e67e4ac-1929-40a1-9f86-2c6dc73d548d	558118816	альбом_военный _коричневый	2025-11-09	20	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
6ecad44b-20ce-49ce-ae8d-bbf32dba74d2	31f0df7a-98a3-4c01-a5a5-f73fc359bd28	558118819	альбом_веточки_Биг	2025-11-09	19	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
ef45f1e4-40ab-47cc-a69a-839fbc8dd58e	531c3bfe-7a15-4d8f-aefe-6981fe3a179e	555528187	album_flamingo	2025-11-09	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
d32f0930-3b0f-4152-b8c6-102596ad2add	707722f9-8229-4dc2-ba2a-82dac805e6a4	558118821	album_glyanec_pink	2025-11-09	28	1	0	0	0.00	\N	\N	4.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
64810520-4902-4e9e-a558-c265de08fa11	1e4d4fe5-14df-45a2-8d2e-bf07c078cbab	555528178	album_vetochki	2025-11-09	18	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
9b8a6192-b45c-4473-9e5a-9fcaec02c612	4997211b-836f-4450-be30-b1c5f0449ac3	467207051	korzina_grey	2025-11-09	21	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
649f0264-0274-480f-aca7-cb5cf8ad55db	f2779a56-27de-4904-84db-ff5f25f3110d	473520918	penal_bezh_plush	2025-11-09	44	3	0	0	0.00	\N	\N	7.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
d6c85349-73e8-4cf1-bdce-7c5fa27e2113	bbab353e-6266-44fc-b2b5-fe615d67c650	556311216	альбом_лен_голубой	2025-11-09	11	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
2addf017-2a8a-4fb5-865b-48b2e1da0e0a	d28d47c9-76eb-406d-af4a-17fbb8066281	473520915	penal_blue_cat	2025-11-09	16	2	1	0	320.00	\N	\N	13.00	50.00	320.00	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
4f578de3-8840-4acf-944e-4e1728402e8a	1e009158-8d26-4300-8f5a-cd5c53825801	558118815	альбом _военный_краснопесчаный	2025-11-09	22	1	1	0	900.00	\N	\N	5.00	100.00	900.00	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
9619a03b-f7e4-4425-938c-321f8acbdace	510f2e6e-1486-4082-91a2-154eddcd30a5	556902797	альбом_вырезанное_сердце_желтый	2025-11-09	14	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
d7f7cc55-5fdd-4d82-81ff-9305d8c47b21	90621643-12da-46d9-b967-8abc695e5652	473520916	penal_pink_cat	2025-11-09	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
430fe7b7-b7b7-4cc7-8788-0a213df7d00c	622ab6c1-6d24-44c7-90a5-1a0d66d33768	473520917	penal_white_plush	2025-11-09	12	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
de5241b0-12be-4579-88c8-b2b4507e5452	8dda1b27-c73e-4a12-a3ea-e36ffae2c0b8	473520919	penal_black_plush	2025-11-09	13	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:44:55.757939+00	2025-11-13 12:44:55.757939+00
1ba5978d-093f-49f8-8972-582e12fd7cab	675a4e11-aafc-4d40-a112-4b4ec05e0411	467207049	korzina_big	2025-11-12	202	18	2	0	3596.00	\N	\N	9.00	11.00	1798.00	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
f19eb756-bb9a-49de-8ccc-e13f455fac70	622ab6c1-6d24-44c7-90a5-1a0d66d33768	473520917	penal_white_plush	2025-11-12	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:18.31773+00
03fcc08e-2aa1-4dec-87c9-5fda0c190fcf	aa9526c1-47a1-4cda-8ba3-1ac82917018a	555528176	album_serdechki	2025-11-11	328	67	25	1	14005.00	\N	\N	20.00	37.00	560.20	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
625b46a1-4826-43e5-a588-081e5760fca9	675a4e11-aafc-4d40-a112-4b4ec05e0411	467207049	korzina_big	2025-11-11	240	18	3	0	5337.00	\N	\N	8.00	17.00	1779.00	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
09c2f67e-2176-4ab6-ac20-db7df98bc405	68b4da00-36ec-4cea-a8ff-367f1d27604c	467102886	korzina_tiger	2025-11-11	163	14	2	0	3120.00	\N	\N	9.00	14.00	1560.00	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
7cb6603a-bac7-40c3-9c89-322372ddc73b	855090a0-7e1a-45c1-8403-fe70a7c64942	473520920	penal_braun_plush	2025-11-11	75	15	0	0	0.00	\N	\N	20.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
19a652d7-f40f-4aa5-922b-7dcf255699e6	686420b8-63de-4638-abaf-cc89e4604bd2	555528192	альбом_военный_ зеленый_2цвета	2025-11-11	63	8	2	0	1800.00	\N	\N	13.00	25.00	900.00	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
8dace160-3140-49c1-812d-841358eb891e	f2779a56-27de-4904-84db-ff5f25f3110d	473520918	penal_bezh_plush	2025-11-11	29	5	0	0	0.00	\N	\N	17.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
2ab84f4e-f9d8-41d6-b131-c708a2f0ec69	04d8589f-d352-4c7c-893a-6e2d41c567dd	507758935	korzina_svetlaya	2025-11-11	95	7	1	0	1827.00	\N	\N	7.00	14.00	1827.00	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
5acc70fb-b320-490f-9273-7f2fe92b8d7a	61d3359d-3112-42fb-9685-73c39b74dc92	556953442	альбом_военный _зеленый_4цвета	2025-11-11	26	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
d2e0d8e4-d3b0-496c-bd14-348076b537df	98319b3c-67ff-44e7-b4fc-deb47179c6d7	558118822	album_big_hearts	2025-11-11	20	1	0	0	0.00	\N	\N	5.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
0e6aa079-363f-4649-9a2b-5ece2cb9f312	ba729003-0857-4378-91eb-7712858964b6	556311214	альбом_вырезанное_сердце_розовый	2025-11-11	26	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
7aa03068-4ae8-4412-be69-8a71592cc526	2e67e4ac-1929-40a1-9f86-2c6dc73d548d	558118816	альбом_военный _коричневый	2025-11-11	14	1	0	0	0.00	\N	\N	7.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
d785e247-1221-434d-a595-2b9d8f29e249	1a6edc21-1692-40ab-9f25-e01dd782082d	558118820	album_grey_heart	2025-11-11	25	1	0	0	0.00	\N	\N	4.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
8c51d94a-3ea4-40e2-833e-2a7cb743ac8b	829b0392-47e7-44d7-a671-cd419b25d2b1	467207055	korzina_pink	2025-11-11	21	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
e4bd8827-0df1-4b2e-a727-904513e5a346	2dae1d26-2788-4a1f-b439-18510a6d45de	555528179	album_glyanec_grey	2025-11-11	23	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
5cb861f8-e901-45c8-b166-07445855487a	d3ed53c9-4b5d-4dd1-9647-f5369b4105de	556311217	альбом_папоротник_Биг	2025-11-11	20	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
bab8fbf8-49b6-4fb5-978b-784a0d20437b	707722f9-8229-4dc2-ba2a-82dac805e6a4	558118821	album_glyanec_pink	2025-11-11	18	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
d3da8461-23b5-4412-a6ee-7683ae819339	a89f8530-97cc-49d5-89b3-dd37c5370f64	467102887	korzina_mini	2025-11-11	33	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
0f65a4b8-fa4d-4822-9dd4-e93b5208ec41	a6b9bc34-aa25-4f84-aa91-f1d303c8643b	473520914	penal_black_cat	2025-11-11	10	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
3412ecb5-2c8a-4242-b6be-eae03b314c77	2585e4a1-16b5-4ff0-a298-c64655faa229	556311218	альбом_сердце_Биг	2025-11-11	27	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
4d397455-8a65-4c1f-99db-ae003510e8d2	8db391e6-9ed3-489d-9cf9-ec1cbd0be071	555528177	album_big_hearts_Big	2025-11-11	18	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
98cacdd2-b2f3-4bdf-a742-6ad7136b3492	1e4d4fe5-14df-45a2-8d2e-bf07c078cbab	555528178	album_vetochki	2025-11-11	11	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
45aab76e-d721-4841-9fa6-8ff29b4a3392	d28d47c9-76eb-406d-af4a-17fbb8066281	473520915	penal_blue_cat	2025-11-11	9	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
d72f0ae3-7e33-4bbf-baae-6ccbb16d0af0	1e009158-8d26-4300-8f5a-cd5c53825801	558118815	альбом _военный_краснопесчаный	2025-11-11	12	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
9a3b0ac3-7452-4232-bb56-ebf9885de6cb	31f0df7a-98a3-4c01-a5a5-f73fc359bd28	558118819	альбом_веточки_Биг	2025-11-11	13	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
281ef969-71a0-4bbc-8811-ce81e7506ebc	4997211b-836f-4450-be30-b1c5f0449ac3	467207051	korzina_grey	2025-11-11	18	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
ae1308f9-cfab-4e60-aa59-161fd31f91bd	3a6ebe20-a8d6-4341-9c61-870c698c7636	556311215	album_paporotnik	2025-11-11	16	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
3bba30f4-b623-45c9-865a-aad1bd925f0e	6afe8407-24e1-4ae2-b973-859cdb04a549	558118817	album_kaktus	2025-11-11	11	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
9dc08fe3-43c1-4412-ba4a-614099d23fa9	b0361398-d919-47ba-8d2c-c8ba8dc852c8	555528182	album_wave_old	2025-11-11	12	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
1be7bc74-1e42-41ed-a056-3a6bdcabeed1	ca604ed0-475c-4d42-90b5-6b62234e0e17	555528188	album_leaves	2025-11-11	11	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
19fe7d1c-292e-46b4-a9ad-3126c50130e9	2ca071cb-1e7f-4a50-aebf-307d36c4e3c3	556902796	альбом_вырезанное_ сердце_ голубой	2025-11-11	25	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
5129b553-8f10-407c-a23b-8d87dc49469a	90621643-12da-46d9-b967-8abc695e5652	473520916	penal_pink_cat	2025-11-11	7	1	0	0	0.00	\N	\N	14.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
d6fd07c7-2a74-4714-9a7a-00ab42e62076	8dda1b27-c73e-4a12-a3ea-e36ffae2c0b8	473520919	penal_black_plush	2025-11-11	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
562db183-d2f4-409b-9ace-c1effad8e303	510f2e6e-1486-4082-91a2-154eddcd30a5	556902797	альбом_вырезанное_сердце_желтый	2025-11-11	13	1	0	0	0.00	\N	\N	8.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
be2b5ac3-367d-4e4a-934b-c52b720ce77e	531c3bfe-7a15-4d8f-aefe-6981fe3a179e	555528187	album_flamingo	2025-11-11	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
b311f3bd-9f0f-41ea-97de-77aebb3a27ca	622ab6c1-6d24-44c7-90a5-1a0d66d33768	473520917	penal_white_plush	2025-11-11	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
d498caa8-de20-404b-88d3-1395ce32fa37	bbab353e-6266-44fc-b2b5-fe615d67c650	556311216	альбом_лен_голубой	2025-11-11	11	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.327775+00	2025-11-13 12:45:18.327775+00
eb9961fe-9acf-4733-8589-e352a94f1f9a	aa9526c1-47a1-4cda-8ba3-1ac82917018a	555528176	album_serdechki	2025-11-13	72	15	8	0	4569.00	\N	\N	21.00	53.00	571.12	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
8d7b3113-a55f-41e4-9f39-50787db2fd47	675a4e11-aafc-4d40-a112-4b4ec05e0411	467207049	korzina_big	2025-11-13	67	4	1	0	1798.00	\N	\N	6.00	25.00	1798.00	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
657f1a92-196f-44c7-8da8-a0c7c076afe7	68b4da00-36ec-4cea-a8ff-367f1d27604c	467102886	korzina_tiger	2025-11-13	43	3	1	0	1632.00	\N	\N	7.00	33.00	1632.00	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
10472399-93d6-4869-9ee5-320a9627130a	855090a0-7e1a-45c1-8403-fe70a7c64942	473520920	penal_braun_plush	2025-11-13	39	8	1	0	420.00	\N	\N	21.00	13.00	420.00	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
21853d96-3c1e-42e4-95e6-b5278fd98024	686420b8-63de-4638-abaf-cc89e4604bd2	555528192	альбом_военный_ зеленый_2цвета	2025-11-13	29	3	0	0	0.00	\N	\N	10.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
72626c3f-acdc-46c1-b1da-9002201a0e7a	f2779a56-27de-4904-84db-ff5f25f3110d	473520918	penal_bezh_plush	2025-11-13	17	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
22a145b7-813b-4048-ab43-01fd33128b94	2dae1d26-2788-4a1f-b439-18510a6d45de	555528179	album_glyanec_grey	2025-11-13	13	1	0	0	0.00	\N	\N	8.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
9ac0c7a2-f31b-4a5a-a867-f56695a597c4	ba729003-0857-4378-91eb-7712858964b6	556311214	альбом_вырезанное_сердце_розовый	2025-11-13	12	1	1	0	901.00	\N	\N	8.00	100.00	901.00	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
b67e5c79-6d4a-44f6-b4fb-f16914f53655	bbab353e-6266-44fc-b2b5-fe615d67c650	556311216	альбом_лен_голубой	2025-11-13	12	1	1	0	780.00	\N	\N	8.00	100.00	780.00	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
c566234a-67c6-4388-8352-9eadf6109dd4	d3ed53c9-4b5d-4dd1-9647-f5369b4105de	556311217	альбом_папоротник_Биг	2025-11-13	12	1	0	0	0.00	\N	\N	8.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
fef0be1c-136e-4fdb-88e4-4fa76ae51dbe	2ca071cb-1e7f-4a50-aebf-307d36c4e3c3	556902796	альбом_вырезанное_ сердце_ голубой	2025-11-13	11	3	0	0	0.00	\N	\N	27.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
1d45408b-c593-4515-8472-d1d228ccda98	61d3359d-3112-42fb-9685-73c39b74dc92	556953442	альбом_военный _зеленый_4цвета	2025-11-13	10	1	0	0	0.00	\N	\N	10.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
c1584f55-eaaa-4fe9-9c47-502efb6f7029	b0361398-d919-47ba-8d2c-c8ba8dc852c8	555528182	album_wave_old	2025-11-13	9	2	1	0	780.00	\N	\N	22.00	50.00	780.00	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
513ab153-b957-486f-b840-5f3e9dd39f82	2585e4a1-16b5-4ff0-a298-c64655faa229	556311218	альбом_сердце_Биг	2025-11-13	9	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
95392116-4317-402a-a3cb-6e9e2b120bf0	a89f8530-97cc-49d5-89b3-dd37c5370f64	467102887	korzina_mini	2025-11-13	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
98b3093a-e416-41fb-9d95-6279add96244	a6b9bc34-aa25-4f84-aa91-f1d303c8643b	473520914	penal_black_cat	2025-11-13	8	2	1	0	429.00	\N	\N	25.00	50.00	429.00	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
7114c7f1-13f1-48c7-a8a1-c1a5735baec2	1a6edc21-1692-40ab-9f25-e01dd782082d	558118820	album_grey_heart	2025-11-13	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
b8bf9ea5-66c8-42fa-b825-3ea02cd36193	98319b3c-67ff-44e7-b4fc-deb47179c6d7	558118822	album_big_hearts	2025-11-13	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
22067501-891e-48f3-952e-f2e201f28d99	8db391e6-9ed3-489d-9cf9-ec1cbd0be071	555528177	album_big_hearts_Big	2025-11-13	7	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
346d0f2c-e704-47a6-a0ec-22a057e2ba43	3a6ebe20-a8d6-4341-9c61-870c698c7636	556311215	album_paporotnik	2025-11-13	7	1	0	0	0.00	\N	\N	14.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
4b5e9db1-fd8e-4fc9-8b57-4a7557f23eca	707722f9-8229-4dc2-ba2a-82dac805e6a4	558118821	album_glyanec_pink	2025-11-13	7	2	0	0	0.00	\N	\N	29.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
b202bb49-722e-4e63-88c3-b49aaf548e32	829b0392-47e7-44d7-a671-cd419b25d2b1	467207055	korzina_pink	2025-11-13	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
b096e838-20b8-427b-919e-453d66a6a7a5	c21b8ab9-258b-4db5-8769-1eea60ae0de2	472859760	holder_big	2025-11-13	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
9eaf5fc6-c729-47d0-b2be-5ebad64890e0	04d8589f-d352-4c7c-893a-6e2d41c567dd	507758935	korzina_svetlaya	2025-11-13	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
84919ba6-c81c-43c1-abea-fcd7d8b1e2cf	1e4d4fe5-14df-45a2-8d2e-bf07c078cbab	555528178	album_vetochki	2025-11-13	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
f5fe062f-2537-470b-a3f9-70ea86da3141	531c3bfe-7a15-4d8f-aefe-6981fe3a179e	555528187	album_flamingo	2025-11-13	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
42fc1246-8c6c-4cba-9ce6-c922aea319bf	1e009158-8d26-4300-8f5a-cd5c53825801	558118815	альбом _военный_краснопесчаный	2025-11-13	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
0c6d31f6-f879-44e5-950e-0e1b75c4a303	2e67e4ac-1929-40a1-9f86-2c6dc73d548d	558118816	альбом_военный _коричневый	2025-11-13	6	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
42182f99-60b7-40c4-988c-76d0c4369049	4997211b-836f-4450-be30-b1c5f0449ac3	467207051	korzina_grey	2025-11-13	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
8e6e7685-1d43-4892-8e04-8643e3f47951	ca604ed0-475c-4d42-90b5-6b62234e0e17	555528188	album_leaves	2025-11-13	5	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
d7545c0a-c95b-438b-b3ae-9c6687abf910	510f2e6e-1486-4082-91a2-154eddcd30a5	556902797	альбом_вырезанное_сердце_желтый	2025-11-13	5	1	0	0	0.00	\N	\N	20.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
771af7a2-508d-4760-ba16-964b210fbbb2	2b1ecf5a-ec30-4c27-9ea4-675f09dda3c9	472859758	holder_pink	2025-11-13	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
f7907458-5481-4112-a3fb-dd0e3b141dc5	90621643-12da-46d9-b967-8abc695e5652	473520916	penal_pink_cat	2025-11-13	4	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
13f23d0a-acb7-4e4c-986a-63bcf729adf7	d28d47c9-76eb-406d-af4a-17fbb8066281	473520915	penal_blue_cat	2025-11-13	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
21d4ece1-8c8d-46a5-8101-bf0f6601f448	31f0df7a-98a3-4c01-a5a5-f73fc359bd28	558118819	альбом_веточки_Биг	2025-11-13	3	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
47641107-9def-45d6-8eac-cfe443cd7f1a	8dda1b27-c73e-4a12-a3ea-e36ffae2c0b8	473520919	penal_black_plush	2025-11-13	2	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
d0cc24a9-3565-4987-b474-b6150058e24d	6afe8407-24e1-4ae2-b973-859cdb04a549	558118817	album_kaktus	2025-11-13	2	1	0	0	0.00	\N	\N	50.00	0.00	\N	2025-11-13 12:45:31.34101+00	2025-11-13 12:45:31.34101+00
d01d4bd1-6f66-4501-b3c4-4a7ddc80a453	aa9526c1-47a1-4cda-8ba3-1ac82917018a	555528176	album_serdechki	2025-11-12	268	41	12	0	6826.00	\N	\N	15.00	29.00	568.83	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
901b60d4-69d0-4c0f-9866-442e1ab26f1f	68b4da00-36ec-4cea-a8ff-367f1d27604c	467102886	korzina_tiger	2025-11-12	128	14	5	0	8064.00	\N	\N	11.00	36.00	1612.80	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
74dfe559-685e-4d77-9130-e1fe65bce7c7	855090a0-7e1a-45c1-8403-fe70a7c64942	473520920	penal_braun_plush	2025-11-12	79	7	2	0	840.00	\N	\N	9.00	29.00	420.00	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
e7f35e20-8353-44a8-b5e2-176a17ff3d44	686420b8-63de-4638-abaf-cc89e4604bd2	555528192	альбом_военный_ зеленый_2цвета	2025-11-12	76	6	0	0	0.00	\N	\N	8.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
5608ff04-b7d0-45de-aa08-3c9a79b770c8	f2779a56-27de-4904-84db-ff5f25f3110d	473520918	penal_bezh_plush	2025-11-12	54	6	2	0	858.00	\N	\N	11.00	33.00	429.00	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
bda428e2-3cc3-43b6-b845-e54590f4fcdc	2dae1d26-2788-4a1f-b439-18510a6d45de	555528179	album_glyanec_grey	2025-11-12	20	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
1e2631c3-5837-453c-81d7-c4b9272cc8c7	ba729003-0857-4378-91eb-7712858964b6	556311214	альбом_вырезанное_сердце_розовый	2025-11-12	27	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
b1b2664b-9c74-4a61-b7c6-f8a08460bc72	bbab353e-6266-44fc-b2b5-fe615d67c650	556311216	альбом_лен_голубой	2025-11-12	8	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
db16c3e5-a603-4bc0-ae21-51825590f7af	d3ed53c9-4b5d-4dd1-9647-f5369b4105de	556311217	альбом_папоротник_Биг	2025-11-12	20	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
58a9af3f-9363-4408-8813-e633b98aa654	2ca071cb-1e7f-4a50-aebf-307d36c4e3c3	556902796	альбом_вырезанное_ сердце_ голубой	2025-11-12	11	1	0	0	0.00	\N	\N	9.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
592a2ab3-b3ec-466b-a0e2-7a8f22164844	61d3359d-3112-42fb-9685-73c39b74dc92	556953442	альбом_военный _зеленый_4цвета	2025-11-12	42	3	3	0	2700.00	\N	\N	7.00	100.00	900.00	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
facddb48-e9f1-4fd9-ac12-6cb79e1743b2	b0361398-d919-47ba-8d2c-c8ba8dc852c8	555528182	album_wave_old	2025-11-12	12	1	0	0	0.00	\N	\N	8.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
d119380f-7290-4b4e-94c5-670e43792b15	2585e4a1-16b5-4ff0-a298-c64655faa229	556311218	альбом_сердце_Биг	2025-11-12	18	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
92a9e56c-bb52-4087-936f-ae752fd5a5bb	a89f8530-97cc-49d5-89b3-dd37c5370f64	467102887	korzina_mini	2025-11-12	18	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
bda02538-f202-459a-a6fe-cf48211634e0	a6b9bc34-aa25-4f84-aa91-f1d303c8643b	473520914	penal_black_cat	2025-11-12	18	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
374c17d7-3ec4-4cff-8ce3-74cda0bbc318	1a6edc21-1692-40ab-9f25-e01dd782082d	558118820	album_grey_heart	2025-11-12	24	2	0	0	0.00	\N	\N	8.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
4b0cc4ad-7c92-4b8d-bd93-380b43134c3c	98319b3c-67ff-44e7-b4fc-deb47179c6d7	558118822	album_big_hearts	2025-11-12	29	1	0	0	0.00	\N	\N	3.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
76985b04-5791-40ab-b17d-7857fb37086b	8db391e6-9ed3-489d-9cf9-ec1cbd0be071	555528177	album_big_hearts_Big	2025-11-12	17	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
40b82951-9fd1-4c32-b1ac-9ac5a3190192	3a6ebe20-a8d6-4341-9c61-870c698c7636	556311215	album_paporotnik	2025-11-12	13	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
bed7c740-1df6-439f-b287-bbd622695dee	707722f9-8229-4dc2-ba2a-82dac805e6a4	558118821	album_glyanec_pink	2025-11-12	19	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
3fe60f6c-6f13-4be9-b0c3-cc8e51cd1e9a	829b0392-47e7-44d7-a671-cd419b25d2b1	467207055	korzina_pink	2025-11-12	20	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
157aba70-177a-46bf-ab79-9218102da141	c21b8ab9-258b-4db5-8769-1eea60ae0de2	472859760	holder_big	2025-11-12	0	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:31.351493+00	2025-11-13 12:45:31.351493+00
a4a37a26-ed74-4953-83c2-e4458be86234	04d8589f-d352-4c7c-893a-6e2d41c567dd	507758935	korzina_svetlaya	2025-11-12	46	3	1	0	1827.00	\N	\N	7.00	33.00	1827.00	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
7bcdf8ad-286e-497b-8c0b-b80729b2d37c	1e4d4fe5-14df-45a2-8d2e-bf07c078cbab	555528178	album_vetochki	2025-11-12	17	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
0b5a7c6a-a553-4f9f-9c1b-430be039293d	531c3bfe-7a15-4d8f-aefe-6981fe3a179e	555528187	album_flamingo	2025-11-12	9	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
c12b97f5-f7c3-42ac-9a86-cc67182c5fe7	1e009158-8d26-4300-8f5a-cd5c53825801	558118815	альбом _военный_краснопесчаный	2025-11-12	15	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
a82428f4-4140-491e-8ce0-544190c1578f	2e67e4ac-1929-40a1-9f86-2c6dc73d548d	558118816	альбом_военный _коричневый	2025-11-12	24	5	1	0	900.00	\N	\N	21.00	20.00	900.00	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
2e603f1d-aa48-4585-85fd-b66d48f26c05	4997211b-836f-4450-be30-b1c5f0449ac3	467207051	korzina_grey	2025-11-12	13	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
bffabdee-785c-4706-a7bb-213e9f5e5b3e	ca604ed0-475c-4d42-90b5-6b62234e0e17	555528188	album_leaves	2025-11-12	12	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
23f63ed1-6729-4364-ae14-0772a89690f6	510f2e6e-1486-4082-91a2-154eddcd30a5	556902797	альбом_вырезанное_сердце_желтый	2025-11-12	10	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
bcff5c72-9f54-4f3f-8ba4-8b809f822f7c	2b1ecf5a-ec30-4c27-9ea4-675f09dda3c9	472859758	holder_pink	2025-11-12	0	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:31.351493+00	2025-11-13 12:45:31.351493+00
f44aeeaa-92bf-4e1b-a3a6-3e65d8eed763	90621643-12da-46d9-b967-8abc695e5652	473520916	penal_pink_cat	2025-11-12	10	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
abb64cd5-bd65-4053-a746-4d0ad67b6f3b	d28d47c9-76eb-406d-af4a-17fbb8066281	473520915	penal_blue_cat	2025-11-12	15	3	0	0	0.00	\N	\N	20.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
c392320b-61b4-4fa3-a21c-bebf7b6067ec	31f0df7a-98a3-4c01-a5a5-f73fc359bd28	558118819	альбом_веточки_Биг	2025-11-12	14	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
737dc344-e432-486e-a3ed-fd87bf68c46a	8dda1b27-c73e-4a12-a3ea-e36ffae2c0b8	473520919	penal_black_plush	2025-11-12	10	1	0	0	0.00	\N	\N	10.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
310eba90-3928-40b4-996e-987001cb159a	6afe8407-24e1-4ae2-b973-859cdb04a549	558118817	album_kaktus	2025-11-12	13	0	0	0	0.00	\N	\N	0.00	0.00	\N	2025-11-13 12:45:18.31773+00	2025-11-13 12:45:31.351493+00
\.


--
-- Data for Name: paid_acceptance; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.paid_acceptance (id, product_id, nm_id, vendor_code, shk_create_date, count, total, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: paid_storage; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.paid_storage (id, product_id, nm_id, vendor_code, date, warehouse_price, gi_ids, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: product_sizes; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.product_sizes (id, serial_id, product_id, barcode, size) FROM stdin;
f8fcc133-b3a7-473e-b482-3b019c873d54	1	09c49f5f-0130-4bad-8865-ae4236ce00ca	2044679208396	0
185bc932-ed31-40e9-8254-e17348910338	2	98319b3c-67ff-44e7-b4fc-deb47179c6d7	2046540126600	0
979f93f1-6149-4786-8293-6495bd8c5536	3	b0361398-d919-47ba-8d2c-c8ba8dc852c8	2046501429825	0
c9d4489c-2bad-46fe-a305-303b0ed2608d	4	1e4d4fe5-14df-45a2-8d2e-bf07c078cbab	2046500109445	0
3531ba36-87d2-4832-8740-25258061b38c	5	2e67e4ac-1929-40a1-9f86-2c6dc73d548d	2046539721014	0
4c977c93-80d0-4b03-8688-d3308cc79caf	6	686420b8-63de-4638-abaf-cc89e4604bd2	2046501945950	0
dc9db440-9031-44d2-adbc-0da13aa06237	7	ba729003-0857-4378-91eb-7712858964b6	2046519017625	0
f9e2c681-7c51-4b93-a58f-492135094aa2	8	aa9526c1-47a1-4cda-8ba3-1ac82917018a	2046499702863	0
beef4fd0-6102-4407-b73f-4d568501e995	9	2dae1d26-2788-4a1f-b439-18510a6d45de	2046500394988	0
30fd6af2-1745-432e-ad6a-0507a0f6f38b	10	2585e4a1-16b5-4ff0-a298-c64655faa229	2046519587036	0
c229c42b-d766-4940-8402-75ee019f4f63	11	707722f9-8229-4dc2-ba2a-82dac805e6a4	2046540040296	0
cd694110-26c0-4907-86d9-7c5e520be499	12	6afe8407-24e1-4ae2-b973-859cdb04a549	2046539870095	0
3aa5c04c-ec38-4df6-904b-6165ac238417	13	31f0df7a-98a3-4c01-a5a5-f73fc359bd28	2046539864216	0
74fc5b9b-5d5d-42b3-a938-c57be95a9753	14	1a6edc21-1692-40ab-9f25-e01dd782082d	2046539936289	0
6114c484-a3fb-464a-9321-be9c541207b7	15	bbab353e-6266-44fc-b2b5-fe615d67c650	2046519357554	0
8f013a77-e7b5-47c1-8dc3-dc513f2e7686	16	531c3bfe-7a15-4d8f-aefe-6981fe3a179e	2046501750851	0
8f08316b-12de-4a72-aec1-41e1952ca118	17	510f2e6e-1486-4082-91a2-154eddcd30a5	2046530753496	0
584c91a9-507b-401b-9320-e88f67b17680	18	ca604ed0-475c-4d42-90b5-6b62234e0e17	2046501796767	0
0b2c0eb9-1d5c-4e1a-9283-a99e0cb78a80	19	1e009158-8d26-4300-8f5a-cd5c53825801	2046539563256	0
5749737e-fcd8-4439-9cfd-e4fac96189d3	20	3a6ebe20-a8d6-4341-9c61-870c698c7636	2046519211634	0
23f8f2a0-98e8-4060-b9b3-c78bab91e5d4	21	2ca071cb-1e7f-4a50-aebf-307d36c4e3c3	2046530739568	0
4f2748d8-f73c-44f8-aa6a-0fd975edc76c	22	61d3359d-3112-42fb-9685-73c39b74dc92	2046531058811	0
513be3b9-6dd0-468a-9eaf-4f654776aa73	23	8db391e6-9ed3-489d-9cf9-ec1cbd0be071	2046500081703	0
f1833355-1831-4eea-a522-516c7dc45bea	24	d3ed53c9-4b5d-4dd1-9647-f5369b4105de	2046519397048	0
bbfae611-6c1d-42e4-a10b-1c11edc134d5	25	04d8589f-d352-4c7c-893a-6e2d41c567dd	2045591108801	0
0be5584d-62b2-4c57-9645-eb2abc1e546a	26	4997211b-836f-4450-be30-b1c5f0449ac3	2044866713368	0
7047a0aa-1dff-447f-bccc-66cf3a6aab3d	27	675a4e11-aafc-4d40-a112-4b4ec05e0411	2044866300032	0
62c4a92d-3e23-4948-a3f6-80c1f47095be	28	a89f8530-97cc-49d5-89b3-dd37c5370f64	2044847991303	0
07581bb6-fd3a-499a-b929-f73e54fc8e2b	29	68b4da00-36ec-4cea-a8ff-367f1d27604c	2044845455609	0
1070bb9f-b78c-455e-b60c-d19b9a0f735e	30	829b0392-47e7-44d7-a671-cd419b25d2b1	2044866709644	0
ade245e4-7800-43e4-8a5d-23af0537730e	31	a6b9bc34-aa25-4f84-aa91-f1d303c8643b	2044967815510	0
1f4dd42f-7636-4e52-9d79-5501838193d0	32	855090a0-7e1a-45c1-8403-fe70a7c64942	2044968823293	0
3dd8cf7a-0335-42b8-a99c-46aba23f0662	33	8dda1b27-c73e-4a12-a3ea-e36ffae2c0b8	2044968824641	0
40580d92-76c9-488a-aacd-188edcbea09c	34	f2779a56-27de-4904-84db-ff5f25f3110d	2044968607626	0
99f1ed7e-dfbf-49c8-bed4-7953235eb10c	35	622ab6c1-6d24-44c7-90a5-1a0d66d33768	2044968242315	0
f24d5c9a-3b32-4850-b21d-7b508e5f71bf	36	90621643-12da-46d9-b967-8abc695e5652	2044967972046	0
dc406219-28d5-4a55-b8ab-e6618e5e509b	37	d28d47c9-76eb-406d-af4a-17fbb8066281	2044967902852	0
78569e48-a1e7-48cf-b9bc-a01ee273a8b3	38	c21b8ab9-258b-4db5-8769-1eea60ae0de2	2044958487566	0
5121d629-c9c7-4bab-ad03-c351db904ec5	39	2b1ecf5a-ec30-4c27-9ea4-675f09dda3c9	2044957421592	0
\.


--
-- Data for Name: products; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.products (id, serial_id, vendor_code, nm_id, imt_id, category_wb, main_photo_url, title) FROM stdin;
04d8589f-d352-4c7c-893a-6e2d41c567dd	25	korzina_svetlaya	507758935	469008727	Корзины для белья	https://basket-27.wbbasket.ru/vol5077/part507758/507758935/images/big/1.webp	Плетеная корзина для белья 40 литров
c21b8ab9-258b-4db5-8769-1eea60ae0de2	38	holder_big	472859760	473348787	Подставки канцелярские	https://basket-26.wbbasket.ru/vol4728/part472859/472859760/images/big/1.webp	Настольный органайзер для канцелярии
2b1ecf5a-ec30-4c27-9ea4-675f09dda3c9	39	holder_pink	472859758	473348787	Подставки канцелярские	https://basket-26.wbbasket.ru/vol4728/part472859/472859758/images/big/1.webp	Настольный органайзер для канцелярии
09c49f5f-0130-4bad-8865-ae4236ce00ca	1	rykzak_black	456770543	455943526	Рюкзаки	https://basket-26.wbbasket.ru/vol4567/part456770/456770543/images/big/1.webp	Рюкзак городской школьный спортивный
98319b3c-67ff-44e7-b4fc-deb47179c6d7	2	album_big_hearts	558118822	568487722	Фотоальбомы	https://basket-29.wbbasket.ru/vol5581/part558118/558118822/images/big/1.webp	Фотоальбом семейный 10х15
b0361398-d919-47ba-8d2c-c8ba8dc852c8	3	album_wave_old	555528182	568487722	Фотоальбомы	https://basket-29.wbbasket.ru/vol5555/part555528/555528182/images/big/1.webp	Фотоальбом семейный 10х15
1e4d4fe5-14df-45a2-8d2e-bf07c078cbab	4	album_vetochki	555528178	568487722	Фотоальбомы	https://basket-29.wbbasket.ru/vol5555/part555528/555528178/images/big/1.webp	Фотоальбом семейный 10х15
2e67e4ac-1929-40a1-9f86-2c6dc73d548d	5	альбом_военный _коричневый	558118816	568487722	Фотоальбомы	https://basket-29.wbbasket.ru/vol5581/part558118/558118816/images/big/1.webp	Фотоальбом семейный 10х15
686420b8-63de-4638-abaf-cc89e4604bd2	6	альбом_военный_ зеленый_2цвета	555528192	568487722	Фотоальбомы	https://basket-29.wbbasket.ru/vol5555/part555528/555528192/images/big/1.webp	Фотоальбом военный семейный 10х15
ba729003-0857-4378-91eb-7712858964b6	7	альбом_вырезанное_сердце_розовый	556311214	568487722	Фотоальбомы	https://basket-29.wbbasket.ru/vol5563/part556311/556311214/images/big/1.webp	Фотоальбом семейный 10х15
aa9526c1-47a1-4cda-8ba3-1ac82917018a	8	album_serdechki	555528176	568487722	Фотоальбомы	https://basket-29.wbbasket.ru/vol5555/part555528/555528176/images/big/1.webp	Фотоальбом семейный 10х15
2dae1d26-2788-4a1f-b439-18510a6d45de	9	album_glyanec_grey	555528179	568487722	Фотоальбомы	https://basket-29.wbbasket.ru/vol5555/part555528/555528179/images/big/1.webp	Фотоальбом семейный 10х15 большой
829b0392-47e7-44d7-a671-cd419b25d2b1	30	korzina_pink	467207055	469008727	Корзины для белья	https://basket-26.wbbasket.ru/vol4672/part467207/467207055/images/big/1.webp	Плетеная корзина для белья
a6b9bc34-aa25-4f84-aa91-f1d303c8643b	31	penal_black_cat	473520914	474284177	Пеналы	https://basket-26.wbbasket.ru/vol4735/part473520/473520914/images/big/1.webp	Пенал школьный подростковый с котиком
855090a0-7e1a-45c1-8403-fe70a7c64942	32	penal_braun_plush	473520920	474284177	Пеналы	https://basket-26.wbbasket.ru/vol4735/part473520/473520920/images/big/1.webp	Пенал школьный плюшевый
8dda1b27-c73e-4a12-a3ea-e36ffae2c0b8	33	penal_black_plush	473520919	474284177	Пеналы	https://basket-26.wbbasket.ru/vol4735/part473520/473520919/images/big/1.webp	Пенал школьный плюшевый
f2779a56-27de-4904-84db-ff5f25f3110d	34	penal_bezh_plush	473520918	474284177	Пеналы	https://basket-26.wbbasket.ru/vol4735/part473520/473520918/images/big/1.webp	Пенал школьный плюшевый
622ab6c1-6d24-44c7-90a5-1a0d66d33768	35	penal_white_plush	473520917	474284177	Пеналы	https://basket-26.wbbasket.ru/vol4735/part473520/473520917/images/big/1.webp	Пенал школьный плюшевый
90621643-12da-46d9-b967-8abc695e5652	36	penal_pink_cat	473520916	474284177	Пеналы	https://basket-26.wbbasket.ru/vol4735/part473520/473520916/images/big/1.webp	Пенал школьный подростковый с котиком
d28d47c9-76eb-406d-af4a-17fbb8066281	37	penal_blue_cat	473520915	474284177	Пеналы	https://basket-26.wbbasket.ru/vol4735/part473520/473520915/images/big/1.webp	Пенал школьный подростковый с котиком
2585e4a1-16b5-4ff0-a298-c64655faa229	10	альбом_сердце_Биг	556311218	568487722	Фотоальбомы	https://basket-29.wbbasket.ru/vol5563/part556311/556311218/images/big/1.webp	Фотоальбом семейный 10х15 большой
707722f9-8229-4dc2-ba2a-82dac805e6a4	11	album_glyanec_pink	558118821	568487722	Фотоальбомы	https://basket-29.wbbasket.ru/vol5581/part558118/558118821/images/big/1.webp	Фотоальбом семейный 10х15 большой глянцевый
6afe8407-24e1-4ae2-b973-859cdb04a549	12	album_kaktus	558118817	568487722	Фотоальбомы	https://basket-29.wbbasket.ru/vol5581/part558118/558118817/images/big/1.webp	Фотоальбом семейный 10х15
31f0df7a-98a3-4c01-a5a5-f73fc359bd28	13	альбом_веточки_Биг	558118819	568487722	Фотоальбомы	https://basket-29.wbbasket.ru/vol5581/part558118/558118819/images/big/1.webp	Фотоальбом семейный 10х15 большой
1a6edc21-1692-40ab-9f25-e01dd782082d	14	album_grey_heart	558118820	568487722	Фотоальбомы	https://basket-29.wbbasket.ru/vol5581/part558118/558118820/images/big/1.webp	Фотоальбом семейный 10х15
bbab353e-6266-44fc-b2b5-fe615d67c650	15	альбом_лен_голубой	556311216	568487722	Фотоальбомы	https://basket-29.wbbasket.ru/vol5563/part556311/556311216/images/big/1.webp	Фотоальбом семейный 10х15
531c3bfe-7a15-4d8f-aefe-6981fe3a179e	16	album_flamingo	555528187	568487722	Фотоальбомы	https://basket-29.wbbasket.ru/vol5555/part555528/555528187/images/big/1.webp	Фотоальбом семейный 10х15
510f2e6e-1486-4082-91a2-154eddcd30a5	17	альбом_вырезанное_сердце_желтый	556902797	568487722	Фотоальбомы	https://basket-29.wbbasket.ru/vol5569/part556902/556902797/images/big/1.webp	Фотоальбом семейный 10х15
ca604ed0-475c-4d42-90b5-6b62234e0e17	18	album_leaves	555528188	568487722	Фотоальбомы	https://basket-29.wbbasket.ru/vol5555/part555528/555528188/images/big/1.webp	Фотоальбом семейный 10х15
1e009158-8d26-4300-8f5a-cd5c53825801	19	альбом _военный_краснопесчаный	558118815	568487722	Фотоальбомы	https://basket-29.wbbasket.ru/vol5581/part558118/558118815/images/big/1.webp	Фотоальбом семейный 10х15
3a6ebe20-a8d6-4341-9c61-870c698c7636	20	album_paporotnik	556311215	568487722	Фотоальбомы	https://basket-29.wbbasket.ru/vol5563/part556311/556311215/images/big/1.webp	Фотоальбом семейный 10х15
2ca071cb-1e7f-4a50-aebf-307d36c4e3c3	21	альбом_вырезанное_ сердце_ голубой	556902796	568487722	Фотоальбомы	https://basket-29.wbbasket.ru/vol5569/part556902/556902796/images/big/1.webp	Фотоальбом семейный 10х15 необычный
61d3359d-3112-42fb-9685-73c39b74dc92	22	альбом_военный _зеленый_4цвета	556953442	568487722	Фотоальбомы	https://basket-29.wbbasket.ru/vol5569/part556953/556953442/images/big/1.webp	Фотоальбом семейный 10х15
8db391e6-9ed3-489d-9cf9-ec1cbd0be071	23	album_big_hearts_Big	555528177	568487722	Фотоальбомы	https://basket-29.wbbasket.ru/vol5555/part555528/555528177/images/big/1.webp	Фотоальбом семейный 10х15 большой
d3ed53c9-4b5d-4dd1-9647-f5369b4105de	24	альбом_папоротник_Биг	556311217	568487722	Фотоальбомы	https://basket-29.wbbasket.ru/vol5563/part556311/556311217/images/big/1.webp	Фотоальбом семейный 10х15 большой
4997211b-836f-4450-be30-b1c5f0449ac3	26	korzina_grey	467207051	469008727	Корзины для белья	https://basket-26.wbbasket.ru/vol4672/part467207/467207051/images/big/1.webp	Плетеная корзина для белья 40 литров
675a4e11-aafc-4d40-a112-4b4ec05e0411	27	korzina_big	467207049	469008727	Корзины для белья	https://basket-26.wbbasket.ru/vol4672/part467207/467207049/images/big/1.webp	Плетеная корзина для белья 40 литров
a89f8530-97cc-49d5-89b3-dd37c5370f64	28	korzina_mini	467102887	469008727	Корзины для белья	https://basket-26.wbbasket.ru/vol4671/part467102/467102887/images/big/1.webp	Плетеная корзина для белья 21 литр
68b4da00-36ec-4cea-a8ff-367f1d27604c	29	korzina_tiger	467102886	469008727	Корзины для белья	https://basket-26.wbbasket.ru/vol4671/part467102/467102886/images/big/1.webp	Плетеная корзина для белья
\.


--
-- Data for Name: week_reports; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.week_reports (id, serial_id, report_type, realizationreport_id, date_from, date_to, quantity_sells_total, quantity_return_total, cancels_total, retail_price_total, retail_amount_total, rub_discountwb_both_total, perc_discountwb_both_total, ppvz_for_pay_total, rub_commision_both_total, perc_commisian_both_total, delivery_amount_total, return_amount_total, delivery_rub_total, penalty_total, storage_fee_total, deduction_total, acceptance_total, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: week_rows; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.week_rows (id, product_id, realizationreport_id, report_type, rr_id, gi_id, order_dt, sale_dt, srid, barcode, ts_name, nm_id, sa_name, doc_type_name, quantity, retail_price, retail_amount, rub_discountwb_both, perc_discountwb_both, ppvz_spp_prc, rub_spp, rub_wallet_dicount, perc_wallet_discount, ppvz_for_pay, rub_commision_both, perc_commisian_both, commission_percent, rub_commission, rub_excess_comission, perc_excess_comission, supplier_oper_name, bonus_type_name, delivery_amount, return_amount, delivery_rub, site_country, office_name, penalty, storage_fee, deduction, acceptance, kiz, created_at) FROM stdin;
\.


--
-- Data for Name: week_stats; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.week_stats (id, report_type, realizationreport_id, product_id, date_from, date_to, nm_id, sa_name, quantity_sells_nm, quantity_return_nm, cancels_nm, retail_price_nm, retail_amount_nm, rub_discountwb_both_nm, perc_discountwb_both_nm, rub_spp_nm, perc_spp_nm, perc_wallet_discount_nm, ppvz_for_pay_nm, rub_commision_both_nm, perc_commisian_both_nm, rub_commission_nm, perc_commission_nm, rub_excess_comission_nm, perc_excess_comission_nm, delivery_amount_nm, return_amount_nm, delivery_rub_nm, created_at) FROM stdin;
\.


--
-- Data for Name: messages_2025_11_12; Type: TABLE DATA; Schema: realtime; Owner: supabase_admin
--

COPY realtime.messages_2025_11_12 (topic, extension, payload, event, private, updated_at, inserted_at, id) FROM stdin;
\.


--
-- Data for Name: messages_2025_11_13; Type: TABLE DATA; Schema: realtime; Owner: supabase_admin
--

COPY realtime.messages_2025_11_13 (topic, extension, payload, event, private, updated_at, inserted_at, id) FROM stdin;
\.


--
-- Data for Name: messages_2025_11_14; Type: TABLE DATA; Schema: realtime; Owner: supabase_admin
--

COPY realtime.messages_2025_11_14 (topic, extension, payload, event, private, updated_at, inserted_at, id) FROM stdin;
\.


--
-- Data for Name: messages_2025_11_15; Type: TABLE DATA; Schema: realtime; Owner: supabase_admin
--

COPY realtime.messages_2025_11_15 (topic, extension, payload, event, private, updated_at, inserted_at, id) FROM stdin;
\.


--
-- Data for Name: messages_2025_11_16; Type: TABLE DATA; Schema: realtime; Owner: supabase_admin
--

COPY realtime.messages_2025_11_16 (topic, extension, payload, event, private, updated_at, inserted_at, id) FROM stdin;
\.


--
-- Data for Name: schema_migrations; Type: TABLE DATA; Schema: realtime; Owner: supabase_admin
--

COPY realtime.schema_migrations (version, inserted_at) FROM stdin;
20211116024918	2025-11-13 12:04:16
20211116045059	2025-11-13 12:04:16
20211116050929	2025-11-13 12:04:16
20211116051442	2025-11-13 12:04:16
20211116212300	2025-11-13 12:04:16
20211116213355	2025-11-13 12:04:16
20211116213934	2025-11-13 12:04:16
20211116214523	2025-11-13 12:04:16
20211122062447	2025-11-13 12:04:16
20211124070109	2025-11-13 12:04:16
20211202204204	2025-11-13 12:04:16
20211202204605	2025-11-13 12:04:16
20211210212804	2025-11-13 12:04:16
20211228014915	2025-11-13 12:04:16
20220107221237	2025-11-13 12:04:16
20220228202821	2025-11-13 12:04:16
20220312004840	2025-11-13 12:04:16
20220603231003	2025-11-13 12:04:16
20220603232444	2025-11-13 12:04:16
20220615214548	2025-11-13 12:04:16
20220712093339	2025-11-13 12:04:16
20220908172859	2025-11-13 12:04:16
20220916233421	2025-11-13 12:04:16
20230119133233	2025-11-13 12:04:16
20230128025114	2025-11-13 12:04:16
20230128025212	2025-11-13 12:04:16
20230227211149	2025-11-13 12:04:16
20230228184745	2025-11-13 12:04:16
20230308225145	2025-11-13 12:04:16
20230328144023	2025-11-13 12:04:16
20231018144023	2025-11-13 12:04:16
20231204144023	2025-11-13 12:04:16
20231204144024	2025-11-13 12:04:16
20231204144025	2025-11-13 12:04:16
20240108234812	2025-11-13 12:04:16
20240109165339	2025-11-13 12:04:16
20240227174441	2025-11-13 12:04:16
20240311171622	2025-11-13 12:04:16
20240321100241	2025-11-13 12:04:16
20240401105812	2025-11-13 12:04:16
20240418121054	2025-11-13 12:04:16
20240523004032	2025-11-13 12:04:16
20240618124746	2025-11-13 12:04:16
20240801235015	2025-11-13 12:04:16
20240805133720	2025-11-13 12:04:16
20240827160934	2025-11-13 12:04:16
20240919163303	2025-11-13 12:04:16
20240919163305	2025-11-13 12:04:16
20241019105805	2025-11-13 12:04:16
20241030150047	2025-11-13 12:04:16
20241108114728	2025-11-13 12:04:16
20241121104152	2025-11-13 12:04:16
20241130184212	2025-11-13 12:04:16
20241220035512	2025-11-13 12:04:16
20241220123912	2025-11-13 12:04:16
20241224161212	2025-11-13 12:04:16
20250107150512	2025-11-13 12:04:16
20250110162412	2025-11-13 12:04:16
20250123174212	2025-11-13 12:04:16
20250128220012	2025-11-13 12:04:16
20250506224012	2025-11-13 12:04:16
20250523164012	2025-11-13 12:04:16
20250714121412	2025-11-13 12:04:16
20250905041441	2025-11-13 12:04:16
\.


--
-- Data for Name: subscription; Type: TABLE DATA; Schema: realtime; Owner: supabase_admin
--

COPY realtime.subscription (id, subscription_id, entity, filters, claims, created_at) FROM stdin;
\.


--
-- Data for Name: buckets; Type: TABLE DATA; Schema: storage; Owner: supabase_storage_admin
--

COPY storage.buckets (id, name, owner, created_at, updated_at, public, avif_autodetection, file_size_limit, allowed_mime_types, owner_id, type) FROM stdin;
\.


--
-- Data for Name: buckets_analytics; Type: TABLE DATA; Schema: storage; Owner: supabase_storage_admin
--

COPY storage.buckets_analytics (id, type, format, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: iceberg_namespaces; Type: TABLE DATA; Schema: storage; Owner: supabase_storage_admin
--

COPY storage.iceberg_namespaces (id, bucket_id, name, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: iceberg_tables; Type: TABLE DATA; Schema: storage; Owner: supabase_storage_admin
--

COPY storage.iceberg_tables (id, namespace_id, bucket_id, name, location, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: migrations; Type: TABLE DATA; Schema: storage; Owner: supabase_storage_admin
--

COPY storage.migrations (id, name, hash, executed_at) FROM stdin;
0	create-migrations-table	e18db593bcde2aca2a408c4d1100f6abba2195df	2025-11-13 12:04:26.472129
1	initialmigration	6ab16121fbaa08bbd11b712d05f358f9b555d777	2025-11-13 12:04:26.474525
2	storage-schema	5c7968fd083fcea04050c1b7f6253c9771b99011	2025-11-13 12:04:26.475492
3	pathtoken-column	2cb1b0004b817b29d5b0a971af16bafeede4b70d	2025-11-13 12:04:26.481653
4	add-migrations-rls	427c5b63fe1c5937495d9c635c263ee7a5905058	2025-11-13 12:04:26.486194
5	add-size-functions	79e081a1455b63666c1294a440f8ad4b1e6a7f84	2025-11-13 12:04:26.487324
6	change-column-name-in-get-size	f93f62afdf6613ee5e7e815b30d02dc990201044	2025-11-13 12:04:26.488948
7	add-rls-to-buckets	e7e7f86adbc51049f341dfe8d30256c1abca17aa	2025-11-13 12:04:26.490349
8	add-public-to-buckets	fd670db39ed65f9d08b01db09d6202503ca2bab3	2025-11-13 12:04:26.491422
9	fix-search-function	3a0af29f42e35a4d101c259ed955b67e1bee6825	2025-11-13 12:04:26.492526
10	search-files-search-function	68dc14822daad0ffac3746a502234f486182ef6e	2025-11-13 12:04:26.493956
11	add-trigger-to-auto-update-updated_at-column	7425bdb14366d1739fa8a18c83100636d74dcaa2	2025-11-13 12:04:26.49539
12	add-automatic-avif-detection-flag	8e92e1266eb29518b6a4c5313ab8f29dd0d08df9	2025-11-13 12:04:26.497086
13	add-bucket-custom-limits	cce962054138135cd9a8c4bcd531598684b25e7d	2025-11-13 12:04:26.498061
14	use-bytes-for-max-size	941c41b346f9802b411f06f30e972ad4744dad27	2025-11-13 12:04:26.49915
15	add-can-insert-object-function	934146bc38ead475f4ef4b555c524ee5d66799e5	2025-11-13 12:04:26.506265
16	add-version	76debf38d3fd07dcfc747ca49096457d95b1221b	2025-11-13 12:04:26.507604
17	drop-owner-foreign-key	f1cbb288f1b7a4c1eb8c38504b80ae2a0153d101	2025-11-13 12:04:26.508553
18	add_owner_id_column_deprecate_owner	e7a511b379110b08e2f214be852c35414749fe66	2025-11-13 12:04:26.509657
19	alter-default-value-objects-id	02e5e22a78626187e00d173dc45f58fa66a4f043	2025-11-13 12:04:26.510953
20	list-objects-with-delimiter	cd694ae708e51ba82bf012bba00caf4f3b6393b7	2025-11-13 12:04:26.511997
21	s3-multipart-uploads	8c804d4a566c40cd1e4cc5b3725a664a9303657f	2025-11-13 12:04:26.513553
22	s3-multipart-uploads-big-ints	9737dc258d2397953c9953d9b86920b8be0cdb73	2025-11-13 12:04:26.517744
23	optimize-search-function	9d7e604cddc4b56a5422dc68c9313f4a1b6f132c	2025-11-13 12:04:26.520939
24	operation-function	8312e37c2bf9e76bbe841aa5fda889206d2bf8aa	2025-11-13 12:04:26.522281
25	custom-metadata	d974c6057c3db1c1f847afa0e291e6165693b990	2025-11-13 12:04:26.523509
26	objects-prefixes	ef3f7871121cdc47a65308e6702519e853422ae2	2025-11-13 12:04:26.524611
27	search-v2	33b8f2a7ae53105f028e13e9fcda9dc4f356b4a2	2025-11-13 12:04:26.529885
28	object-bucket-name-sorting	ba85ec41b62c6a30a3f136788227ee47f311c436	2025-11-13 12:04:26.553416
29	create-prefixes	a7b1a22c0dc3ab630e3055bfec7ce7d2045c5b7b	2025-11-13 12:04:26.555353
30	update-object-levels	6c6f6cc9430d570f26284a24cf7b210599032db7	2025-11-13 12:04:26.556493
31	objects-level-index	33f1fef7ec7fea08bb892222f4f0f5d79bab5eb8	2025-11-13 12:04:26.557717
32	backward-compatible-index-on-objects	2d51eeb437a96868b36fcdfb1ddefdf13bef1647	2025-11-13 12:04:26.558972
33	backward-compatible-index-on-prefixes	fe473390e1b8c407434c0e470655945b110507bf	2025-11-13 12:04:26.560016
34	optimize-search-function-v1	82b0e469a00e8ebce495e29bfa70a0797f7ebd2c	2025-11-13 12:04:26.560236
35	add-insert-trigger-prefixes	63bb9fd05deb3dc5e9fa66c83e82b152f0caf589	2025-11-13 12:04:26.562072
36	optimise-existing-functions	81cf92eb0c36612865a18016a38496c530443899	2025-11-13 12:04:26.562868
37	add-bucket-name-length-trigger	3944135b4e3e8b22d6d4cbb568fe3b0b51df15c1	2025-11-13 12:04:26.565554
38	iceberg-catalog-flag-on-buckets	19a8bd89d5dfa69af7f222a46c726b7c41e462c5	2025-11-13 12:04:26.566955
39	add-search-v2-sort-support	39cf7d1e6bf515f4b02e41237aba845a7b492853	2025-11-13 12:04:26.571599
40	fix-prefix-race-conditions-optimized	fd02297e1c67df25a9fc110bf8c8a9af7fb06d1f	2025-11-13 12:04:26.573325
41	add-object-level-update-trigger	44c22478bf01744b2129efc480cd2edc9a7d60e9	2025-11-13 12:04:26.576364
42	rollback-prefix-triggers	f2ab4f526ab7f979541082992593938c05ee4b47	2025-11-13 12:04:26.577781
43	fix-object-level	ab837ad8f1c7d00cc0b7310e989a23388ff29fc6	2025-11-13 12:04:26.579188
\.


--
-- Data for Name: objects; Type: TABLE DATA; Schema: storage; Owner: supabase_storage_admin
--

COPY storage.objects (id, bucket_id, name, owner, created_at, updated_at, last_accessed_at, metadata, version, owner_id, user_metadata, level) FROM stdin;
\.


--
-- Data for Name: prefixes; Type: TABLE DATA; Schema: storage; Owner: supabase_storage_admin
--

COPY storage.prefixes (bucket_id, name, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: s3_multipart_uploads; Type: TABLE DATA; Schema: storage; Owner: supabase_storage_admin
--

COPY storage.s3_multipart_uploads (id, in_progress_size, upload_signature, bucket_id, key, version, owner_id, created_at, user_metadata) FROM stdin;
\.


--
-- Data for Name: s3_multipart_uploads_parts; Type: TABLE DATA; Schema: storage; Owner: supabase_storage_admin
--

COPY storage.s3_multipart_uploads_parts (id, upload_id, size, part_number, bucket_id, key, etag, owner_id, version, created_at) FROM stdin;
\.


--
-- Data for Name: hooks; Type: TABLE DATA; Schema: supabase_functions; Owner: supabase_functions_admin
--

COPY supabase_functions.hooks (id, hook_table_id, hook_name, created_at, request_id) FROM stdin;
\.


--
-- Data for Name: migrations; Type: TABLE DATA; Schema: supabase_functions; Owner: supabase_functions_admin
--

COPY supabase_functions.migrations (version, inserted_at) FROM stdin;
initial	2025-11-13 12:04:13.989266+00
20210809183423_update_grants	2025-11-13 12:04:13.989266+00
\.


--
-- Data for Name: schema_migrations; Type: TABLE DATA; Schema: supabase_migrations; Owner: postgres
--

COPY supabase_migrations.schema_migrations (version, statements, name) FROM stdin;
01	{"CREATE TABLE products (\n  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n  serial_id BIGSERIAL UNIQUE NOT NULL,\n  vendor_code TEXT UNIQUE NOT NULL,\n  nm_id BIGINT UNIQUE NOT NULL,\n  imt_id BIGINT NOT NULL,\n  category_wb TEXT NOT NULL,\n  main_photo_url TEXT,\n  title TEXT NOT NULL\n)","CREATE INDEX idx_products_category ON products(category_wb)"}	products
02	{"CREATE TABLE product_sizes (\n  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n  serial_id BIGSERIAL UNIQUE NOT NULL,\n  product_id UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,\n  barcode TEXT UNIQUE NOT NULL,\n  size TEXT\n)","CREATE INDEX idx_product_sizes_product_id ON product_sizes(product_id)","CREATE INDEX idx_product_sizes_barcode ON product_sizes(barcode)"}	product_sizes
03	{"-- ========================================================================\n-- Создание таблицы cr_daily_stats для статистики CR (Conversion Rate)\n-- ========================================================================\n-- Включает:\n-- 1. Таблицу cr_daily_stats\n-- 2. Индексы\n-- 3. Триггер для автоматического обновления updated_at\n-- ========================================================================\n\n\n-- ========================================================================\n-- 1. Создание таблицы cr_daily_stats\n-- ========================================================================\nCREATE TABLE cr_daily_stats (\n  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n\n  -- Связи и ключи\n  product_id UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,\n  nm_id BIGINT NOT NULL,\n  vendor_code TEXT NOT NULL,\n\n  -- Бизнес-дата: к какому дню относятся метрики\n  -- Источник: Python вычисляет today/yesterday по Europe/Moscow\n  -- selectedPeriod → today, previousPeriod → yesterday\n  date_of_period DATE NOT NULL,\n\n  -- Метрики: счетчики (все допускают NULL)\n  open_card_count INTEGER,\n  add_to_cart_count INTEGER,\n  orders_count INTEGER,\n  cancel_count INTEGER,\n\n  -- Метрики: суммы\n  orders_sum_rub NUMERIC(14,2),\n\n  -- Остатки на складах\n  stocks_mp INTEGER,\n  stocks_wb INTEGER,\n\n  -- Конверсии\n  add_to_cart_percent NUMERIC(5,2),\n  cart_to_order_percent NUMERIC(5,2),\n\n  -- Агрегаты (вычисляются в Python)\n  order_price NUMERIC(14,2),   -- orders_sum_rub / orders_count (NULL если count=0/NULL)\n\n  -- Технические метки (управляются PostgreSQL)\n  created_at TIMESTAMPTZ DEFAULT NOW(),  -- когда создана\n  updated_at TIMESTAMPTZ DEFAULT NOW()   -- когда обновлена\n)","-- Уникальность: одна запись на артикул в день\nCREATE UNIQUE INDEX ux_cr_daily ON cr_daily_stats (nm_id, date_of_period)","-- Индексы для быстрого поиска\nCREATE INDEX idx_cr_nm_date ON cr_daily_stats (nm_id, date_of_period)","CREATE INDEX idx_cr_date ON cr_daily_stats (date_of_period)","CREATE INDEX idx_cr_product_id ON cr_daily_stats (product_id)","CREATE INDEX idx_cr_vendor_code ON cr_daily_stats (vendor_code)","-- ========================================================================\n-- 2. Создание функции для автоматического обновления updated_at\n-- ========================================================================\nCREATE OR REPLACE FUNCTION update_updated_at_column()\nRETURNS TRIGGER AS $$\nBEGIN\n    NEW.updated_at = NOW();\n    RETURN NEW;\nEND;\n$$ LANGUAGE plpgsql","COMMENT ON FUNCTION update_updated_at_column IS 'Автоматически обновляет поле updated_at при UPDATE'","-- ========================================================================\n-- 3. Создание триггера для cr_daily_stats\n-- ========================================================================\nDROP TRIGGER IF EXISTS update_cr_daily_stats_updated_at ON cr_daily_stats","CREATE TRIGGER update_cr_daily_stats_updated_at\n    BEFORE UPDATE ON cr_daily_stats\n    FOR EACH ROW\n    EXECUTE FUNCTION update_updated_at_column()","COMMENT ON TRIGGER update_cr_daily_stats_updated_at ON cr_daily_stats \nIS 'Автоматически обновляет updated_at при UPDATE записи'"}	cr_daily_stats
04	{"-- ========================================================================\n-- Создание таблиц и функций для рекламной статистики (adv_params)\n-- ========================================================================\n-- Включает:\n-- 1. Таблицу adv_campaign_daily_stats (детальная статистика)\n-- 2. Таблицу adv_params (агрегированная статистика)\n-- 3. Триггеры для автоматического обновления updated_at\n-- 4. RPC функцию для агрегации данных\n-- ========================================================================\n\n\n-- ========================================================================\n-- 1. Создание таблицы adv_campaign_daily_stats\n-- ========================================================================\nCREATE TABLE adv_campaign_daily_stats (\n  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n  \n  -- Ключи\n  advert_id BIGINT NOT NULL,            -- ID рекламной кампании из WB\n  nm_id BIGINT NOT NULL,                -- Артикул WB\n  vendor_code TEXT NOT NULL,            -- Артикул продавца (из products)\n  date DATE NOT NULL,                   -- Дата показателей\n  \n  -- Метрики артикула (из nms[], агрегат по всем платформам)\n  views INTEGER DEFAULT 0,              -- Показы (сумма по apps[].nms[])\n  clicks INTEGER DEFAULT 0,             -- Клики (сумма по apps[].nms[])\n  cpc NUMERIC(10,2),                    -- Средняя стоимость клика\n  ctr NUMERIC(5,2),                     -- CTR (%)\n  sum NUMERIC(14,2) DEFAULT 0,          -- Затраты (₽) (сумма по apps[].nms[])\n  \n  -- Заказы (из days[], включая склейку)\n  orders INTEGER DEFAULT 0,             -- Количество заказов (из days[].orders)\n  orders_sum NUMERIC(14,2) DEFAULT 0,   -- Сумма заказов (₽) (из days[].sum_price)\n  \n  -- Вычисляемые метрики\n  cpm NUMERIC(10,2),                    -- CPM = (sum / views) * 1000\n  \n  -- Технические метки\n  created_at TIMESTAMPTZ DEFAULT NOW(),\n  updated_at TIMESTAMPTZ DEFAULT NOW()\n)","-- Уникальность: одна кампания + один артикул + одна дата\nCREATE UNIQUE INDEX ux_adv_daily_stats ON adv_campaign_daily_stats(advert_id, nm_id, date)","-- Индексы для быстрого поиска\nCREATE INDEX idx_adv_daily_advert_id ON adv_campaign_daily_stats(advert_id)","CREATE INDEX idx_adv_daily_nm_id ON adv_campaign_daily_stats(nm_id)","CREATE INDEX idx_adv_daily_date ON adv_campaign_daily_stats(date)","CREATE INDEX idx_adv_daily_nm_date ON adv_campaign_daily_stats(nm_id, date)","CREATE INDEX idx_adv_daily_vendor_code ON adv_campaign_daily_stats(vendor_code)","-- Комментарии\nCOMMENT ON TABLE adv_campaign_daily_stats IS 'Детальная статистика по рекламным кампаниям: каждая строка = один артикул в одной кампании за один день'","COMMENT ON COLUMN adv_campaign_daily_stats.views IS 'Показы артикула (сумма по всем платформам из nms[])'","COMMENT ON COLUMN adv_campaign_daily_stats.orders IS 'Заказы из days[] - включает склейку (ассоциированные артикулы)'","COMMENT ON COLUMN adv_campaign_daily_stats.cpm IS 'CPM (Cost Per Mille) = затраты на 1000 показов'","-- ========================================================================\n-- 2. Создание таблицы adv_params\n-- ========================================================================\nCREATE TABLE adv_params (\n  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n  \n  -- Ключи\n  nm_id BIGINT NOT NULL,                -- Артикул WB\n  vendor_code TEXT NOT NULL,            -- Артикул продавца\n  date DATE NOT NULL,                   -- Дата показателей\n  \n  -- Агрегированные метрики (сумма из всех кампаний артикула)\n  views INTEGER DEFAULT 0,              -- Показы (сумма)\n  clicks INTEGER DEFAULT 0,             -- Клики (сумма)\n  sum NUMERIC(14,2) DEFAULT 0,          -- Затраты (₽) (сумма)\n  \n  -- Вычисляемые метрики\n  cpc NUMERIC(10,2),                    -- Средняя стоимость клика (sum / clicks)\n  cpm NUMERIC(10,2),                    -- CPM = (sum / views) * 1000\n  ctr NUMERIC(5,2),                     -- CTR = (clicks / views) * 100\n  \n  -- Заказы (сумма из всех кампаний)\n  orders INTEGER DEFAULT 0,             -- Количество заказов\n  orders_sum NUMERIC(14,2) DEFAULT 0,   -- Сумма заказов (₽)\n  \n  -- Технические метки\n  created_at TIMESTAMPTZ DEFAULT NOW(),\n  updated_at TIMESTAMPTZ DEFAULT NOW(),\n  \n  -- Связь с products\n  FOREIGN KEY (nm_id) REFERENCES products(nm_id) ON DELETE CASCADE\n)","-- Уникальность: один артикул - одна дата\nCREATE UNIQUE INDEX ux_adv_params_nm_date ON adv_params(nm_id, date)","-- Индексы для быстрого поиска\nCREATE INDEX idx_adv_params_nm_id ON adv_params(nm_id)","CREATE INDEX idx_adv_params_date ON adv_params(date)","CREATE INDEX idx_adv_params_vendor_code ON adv_params(vendor_code)","-- Комментарии\nCOMMENT ON TABLE adv_params IS 'Агрегированная рекламная статистика: каждая строка = один артикул за один день (суммируем все кампании)'","COMMENT ON COLUMN adv_params.views IS 'Суммарные показы артикула во всех кампаниях'","COMMENT ON COLUMN adv_params.orders IS 'Суммарные заказы артикула во всех кампаниях (включая склейку)'","COMMENT ON COLUMN adv_params.cpm IS 'CPM = (sum / views) * 1000, NULL если views = 0'","-- ========================================================================\n-- 3. Создание функции для автоматического обновления updated_at\n-- ========================================================================\nCREATE OR REPLACE FUNCTION update_updated_at_column()\nRETURNS TRIGGER AS $$\nBEGIN\n    NEW.updated_at = NOW();\n    RETURN NEW;\nEND;\n$$ LANGUAGE plpgsql","COMMENT ON FUNCTION update_updated_at_column IS 'Автоматически обновляет поле updated_at при UPDATE'","-- ========================================================================\n-- 4. Создание триггеров для updated_at\n-- ========================================================================\n\n-- Триггер для adv_campaign_daily_stats\nCREATE TRIGGER update_adv_campaign_daily_stats_updated_at\n    BEFORE UPDATE ON adv_campaign_daily_stats\n    FOR EACH ROW\n    EXECUTE FUNCTION update_updated_at_column()","-- Триггер для adv_params (ОТКЛЮЧЕН - updated_at управляется в aggregate_adv_params)\n-- Причина: триггер перезаписывает логику условного обновления в ON CONFLICT\n-- CREATE TRIGGER update_adv_params_updated_at\n--     BEFORE UPDATE ON adv_params\n--     FOR EACH ROW\n--     EXECUTE FUNCTION update_updated_at_column();\n\n\n-- ========================================================================\n-- 5. Создание RPC функции для агрегации\n-- ========================================================================\nCREATE OR REPLACE FUNCTION aggregate_adv_params(\n  p_date_from DATE DEFAULT NULL,\n  p_date_to DATE DEFAULT NULL\n)\nRETURNS INTEGER\nLANGUAGE plpgsql\nAS $$\nDECLARE\n  inserted_count INTEGER;\nBEGIN\n  INSERT INTO adv_params (\n    nm_id,\n    vendor_code,\n    date,\n    views,\n    clicks,\n    sum,\n    cpc,\n    cpm,\n    ctr,\n    orders,\n    orders_sum\n  )\n  SELECT\n    s.nm_id,\n    s.vendor_code,\n    s.date,\n    SUM(s.views)::INTEGER AS views,\n    SUM(s.clicks)::INTEGER AS clicks,\n    SUM(s.sum) AS sum,\n    -- CPC: средняя стоимость клика\n    CASE\n      WHEN SUM(s.clicks) > 0 THEN ROUND(SUM(s.sum) / SUM(s.clicks), 2)\n      ELSE NULL\n    END AS cpc,\n    -- CPM: стоимость 1000 показов\n    CASE\n      WHEN SUM(s.views) > 0 THEN ROUND((SUM(s.sum) / SUM(s.views)) * 1000, 2)\n      ELSE NULL\n    END AS cpm,\n    -- CTR: процент кликов от показов\n    CASE\n      WHEN SUM(s.views) > 0 THEN ROUND((SUM(s.clicks)::NUMERIC / SUM(s.views)) * 100, 2)\n      ELSE NULL\n    END AS ctr,\n    SUM(s.orders)::INTEGER AS orders,\n    SUM(s.orders_sum) AS orders_sum\n  FROM\n    adv_campaign_daily_stats s\n  WHERE\n    (p_date_from IS NULL OR s.date >= p_date_from)\n    AND (p_date_to IS NULL OR s.date <= p_date_to)\n  GROUP BY\n    s.nm_id,\n    s.vendor_code,\n    s.date\n  ON CONFLICT (nm_id, date) DO UPDATE SET\n    vendor_code = EXCLUDED.vendor_code,\n    views = EXCLUDED.views,\n    clicks = EXCLUDED.clicks,\n    sum = EXCLUDED.sum,\n    cpc = EXCLUDED.cpc,\n    cpm = EXCLUDED.cpm,\n    ctr = EXCLUDED.ctr,\n    orders = EXCLUDED.orders,\n    orders_sum = EXCLUDED.orders_sum,\n    -- Обновляем updated_at ТОЛЬКО если данные реально изменились\n    updated_at = CASE \n      WHEN (\n        adv_params.views IS DISTINCT FROM EXCLUDED.views OR\n        adv_params.clicks IS DISTINCT FROM EXCLUDED.clicks OR\n        adv_params.sum IS DISTINCT FROM EXCLUDED.sum OR\n        adv_params.orders IS DISTINCT FROM EXCLUDED.orders OR\n        adv_params.orders_sum IS DISTINCT FROM EXCLUDED.orders_sum\n      )\n      THEN NOW()\n      ELSE adv_params.updated_at\n    END;\n  \n  -- Возвращаем количество обработанных записей\n  GET DIAGNOSTICS inserted_count = ROW_COUNT;\n  RETURN inserted_count;\nEND;\n$$","COMMENT ON FUNCTION aggregate_adv_params IS 'Агрегирует данные из adv_campaign_daily_stats в adv_params, группируя по nm_id и date'"}	adv_params
05	{"-- ========================================================================\n-- Создание таблицы week_reports для хранения агрегированных данных weekly reports\n-- ========================================================================\n-- Включает:\n-- 1. Таблицу week_reports\n-- 2. Индексы\n-- 3. Триггер для автоматического обновления updated_at\n-- ========================================================================\n\n\n-- ========================================================================\n-- 1. Создание последовательности для serial_id\n-- ========================================================================\nCREATE SEQUENCE IF NOT EXISTS week_reports_serial_id_seq START 1","-- ========================================================================\n-- 2. Создание таблицы week_reports\n-- ========================================================================\nCREATE TABLE week_reports (\n  -- PK\n  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n  \n  -- Базовые атрибуты отчета\n  serial_id INTEGER DEFAULT nextval('week_reports_serial_id_seq'),\n  report_type TEXT,\n  realizationreport_id INTEGER NOT NULL UNIQUE,\n  date_from DATE NOT NULL,\n  date_to DATE NOT NULL,\n  \n  -- Агрегированные метрики по количеству\n  quantity_sells_total INTEGER,           -- Продажи (Продажа + Возврат с минусом)\n  quantity_return_total INTEGER,          -- Возвраты\n  cancels_total INTEGER,                  -- Отмены (return_amount Логистика - quantity_return_total)\n  \n  -- Агрегированные метрики по ценам\n  retail_price_total NUMERIC(14,2),       -- Продажи (Продажа + Возврат с минусом)\n  retail_amount_total NUMERIC(14,2),      -- Продажи (Продажа + Возврат с минусом)\n  rub_discountWB_both_total NUMERIC(14,2), -- Скидка WB (retail_price_total - retail_amount_total)\n  perc_discountWB_both_total NUMERIC(5,2), -- % скидки WB (rub_discountWB_both_total / retail_price_total)\n  \n  -- Агрегированные метрики по выплатам\n  ppvz_for_pay_total NUMERIC(14,2),       -- К перечислению (Продажа + Возврат с минусом)\n  rub_commision_both_total NUMERIC(14,2), -- Комиссия WB (retail_price_total - ppvz_for_pay_total)\n  perc_commisian_both_total NUMERIC(5,2), -- % комиссии WB (rub_commision_both_total / retail_price_total)\n  \n  -- Агрегированные метрики по логистике\n  delivery_amount_total NUMERIC(14,2),    -- Сумма доставки (Логистика)\n  return_amount_total NUMERIC(14,2),      -- Сумма возвратов (Логистика)\n  delivery_rub_total NUMERIC(14,2),       -- Стоимость доставки (Логистика)\n  \n  -- Агрегированные метрики по доп. услугам\n  penalty_total NUMERIC(14,2),            -- Штрафы\n  storage_fee_total NUMERIC(14,2),        -- Хранение\n  deduction_total NUMERIC(14,2),          -- Удержания\n  acceptance_total NUMERIC(14,2),         -- Платная приемка\n  \n  -- Технические метки (управляются PostgreSQL)\n  created_at TIMESTAMPTZ DEFAULT NOW(),   -- когда создана\n  updated_at TIMESTAMPTZ DEFAULT NOW()    -- когда обновлена\n)","-- Индексы для быстрого поиска\nCREATE INDEX idx_week_reports_realizationreport_id ON week_reports (realizationreport_id)","-- Связываем последовательность с полем serial_id\nALTER TABLE week_reports ALTER COLUMN serial_id SET DEFAULT nextval('week_reports_serial_id_seq')"}	week_reports
06	{"-- Create week_rows table\nCREATE TABLE week_rows (\n  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n  product_id UUID REFERENCES products(id) ON DELETE CASCADE,\n  realizationreport_id INTEGER NOT NULL REFERENCES week_reports(realizationreport_id) ON DELETE CASCADE,\n  report_type TEXT,\n  rr_id BIGINT,\n  gi_id BIGINT,\n  order_dt DATE,\n  sale_dt DATE,\n  srid TEXT,\n  barcode BIGINT,\n  ts_name TEXT,\n  nm_id BIGINT,\n  sa_name TEXT,\n  doc_type_name TEXT,\n  quantity INTEGER,\n  retail_price NUMERIC(12, 2),\n  retail_amount NUMERIC(12, 2),\n  rub_discountwb_both NUMERIC(12, 2),\n  perc_discountwb_both NUMERIC(12, 2),\n  ppvz_spp_prc NUMERIC(12, 2),\n  rub_spp NUMERIC(12, 2),\n  rub_wallet_dicount NUMERIC(12, 2),\n  perc_wallet_discount NUMERIC(12, 2),\n  ppvz_for_pay NUMERIC(12, 2),\n  rub_commision_both NUMERIC(12, 2),\n  perc_commisian_both NUMERIC(12, 2),\n  commission_percent NUMERIC(12, 2),\n  rub_commission NUMERIC(12, 2),\n  rub_excess_comission NUMERIC(12, 2),\n  perc_excess_comission NUMERIC(12, 2),\n  supplier_oper_name TEXT,\n  bonus_type_name TEXT,\n  delivery_amount NUMERIC(12, 2),\n  return_amount NUMERIC(12, 2),\n  delivery_rub NUMERIC(12, 2),\n  site_country TEXT,\n  office_name TEXT,\n  penalty NUMERIC(12, 2),\n  storage_fee NUMERIC(12, 2),\n  deduction NUMERIC(12, 2),\n  acceptance NUMERIC(12, 2),\n  kiz TEXT,\n  created_at TIMESTAMPTZ DEFAULT NOW()\n)","-- Create indexes\nCREATE INDEX idx_week_rows_product_id ON week_rows (product_id)","CREATE INDEX idx_week_rows_realizationreport_id ON week_rows (realizationreport_id)","CREATE INDEX idx_week_rows_nm_id ON week_rows (nm_id)","CREATE INDEX idx_week_rows_sale_dt ON week_rows (sale_dt)","-- Create UNIQUE constraint to prevent duplicates\nCREATE UNIQUE INDEX idx_week_rows_unique_rr_id ON week_rows (realizationreport_id, rr_id)"}	week_rows
07	{"-- week_stats: aggregated per nm_id per realizationreport_id\nCREATE TABLE week_stats (\n  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n  report_type TEXT,\n  realizationreport_id INTEGER NOT NULL REFERENCES week_reports(realizationreport_id) ON DELETE CASCADE,\n  product_id UUID REFERENCES products(id) ON DELETE CASCADE,\n  date_from DATE NOT NULL,\n  date_to DATE NOT NULL,\n  nm_id BIGINT NOT NULL,\n  sa_name TEXT,\n  quantity_sells_nm INTEGER,\n  quantity_return_nm INTEGER,\n  cancels_nm INTEGER,\n  retail_price_nm NUMERIC(12, 2),\n  retail_amount_nm NUMERIC(12, 2),\n  rub_discountwb_both_nm NUMERIC(12, 2),\n  perc_discountwb_both_nm NUMERIC(12, 2),\n  rub_spp_nm NUMERIC(12, 2),\n  perc_spp_nm NUMERIC(12, 2),\n  perc_wallet_discount_nm NUMERIC(12, 2),\n  ppvz_for_pay_nm NUMERIC(12, 2),\n  rub_commision_both_nm NUMERIC(12, 2),\n  perc_commisian_both_nm NUMERIC(12, 2),\n  rub_commission_nm NUMERIC(12, 2),\n  perc_commission_nm NUMERIC(12, 2),\n  rub_excess_comission_nm NUMERIC(12, 2),\n  perc_excess_comission_nm NUMERIC(12, 2),\n  delivery_amount_nm NUMERIC(12, 2),\n  return_amount_nm NUMERIC(12, 2),\n  delivery_rub_nm NUMERIC(12, 2),\n  created_at TIMESTAMPTZ DEFAULT NOW()\n)","-- Uniqueness\nCREATE UNIQUE INDEX idx_week_stats_unique ON week_stats (realizationreport_id, nm_id)","-- Indexes\nCREATE INDEX idx_week_stats_realizationreport_id ON week_stats (realizationreport_id)","CREATE INDEX idx_week_stats_nm_id ON week_stats (nm_id)"}	week_stats
08	{"-- ========================================================================\n-- Create paid_acceptance table for Seller Analytics Acceptance report\n-- ========================================================================\n\n-- Enable pgcrypto for gen_random_uuid if not enabled (safe to re-run)\n-- CREATE EXTENSION IF NOT EXISTS pgcrypto;\n\nCREATE TABLE IF NOT EXISTS paid_acceptance (\n  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n\n  -- Relations\n  product_id UUID REFERENCES products(id) ON DELETE SET NULL,\n\n  -- Business keys\n  nm_id BIGINT NOT NULL,\n  vendor_code TEXT NOT NULL,\n  shk_create_date DATE NOT NULL,\n\n  -- Aggregated metrics\n  count INTEGER NOT NULL,\n  total NUMERIC(14,2) NOT NULL,\n\n  -- Timestamps\n  created_at TIMESTAMPTZ DEFAULT NOW(),\n  updated_at TIMESTAMPTZ DEFAULT NOW()\n)","-- Uniqueness: one row per (nm_id, shk_create_date)\nCREATE UNIQUE INDEX IF NOT EXISTS ux_paid_acceptance_nm_date ON paid_acceptance (nm_id, shk_create_date)","-- Helpful indexes\nCREATE INDEX IF NOT EXISTS idx_paid_acceptance_nm_id ON paid_acceptance (nm_id)","CREATE INDEX IF NOT EXISTS idx_paid_acceptance_date ON paid_acceptance (shk_create_date)","-- Update updated_at automatically\nCREATE OR REPLACE FUNCTION update_updated_at_column()\nRETURNS TRIGGER AS $$\nBEGIN\n  NEW.updated_at = NOW();\n  RETURN NEW;\nEND;\n$$ LANGUAGE plpgsql","DROP TRIGGER IF EXISTS trg_paid_acceptance_updated_at ON paid_acceptance","CREATE TRIGGER trg_paid_acceptance_updated_at\n  BEFORE UPDATE ON paid_acceptance\n  FOR EACH ROW\n  EXECUTE FUNCTION update_updated_at_column()","COMMENT ON TABLE paid_acceptance IS 'Daily paid acceptance per nm_id aggregated by shk_create_date'","COMMENT ON COLUMN paid_acceptance.total IS 'Sum of total acceptance cost for the day & article'"}	paid_acceptance
09	{"-- ========================================================================\n-- Create paid_storage table for Seller Analytics Paid Storage report\n-- ========================================================================\n\n-- Enable pgcrypto for gen_random_uuid if not enabled (safe to re-run)\n-- CREATE EXTENSION IF NOT EXISTS pgcrypto;\n\nCREATE TABLE IF NOT EXISTS paid_storage (\n  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n\n  -- Relations\n  product_id UUID REFERENCES products(id) ON DELETE SET NULL,\n\n  -- Business keys\n  nm_id BIGINT NOT NULL,\n  vendor_code TEXT NOT NULL,\n  date DATE NOT NULL,\n\n  -- Aggregated metrics\n  warehouse_price NUMERIC(14,2) NOT NULL,\n\n  -- Optional details (multiple supplies per article per day)\n  gi_ids BIGINT[],\n\n  -- Timestamps\n  created_at TIMESTAMPTZ DEFAULT NOW(),\n  updated_at TIMESTAMPTZ DEFAULT NOW()\n)","-- Uniqueness: one row per (nm_id, date)\nCREATE UNIQUE INDEX IF NOT EXISTS ux_paid_storage_nm_date ON paid_storage (nm_id, date)","-- Helpful indexes\nCREATE INDEX IF NOT EXISTS idx_paid_storage_nm_id ON paid_storage (nm_id)","CREATE INDEX IF NOT EXISTS idx_paid_storage_date ON paid_storage (date)","-- Update updated_at automatically\nCREATE OR REPLACE FUNCTION update_updated_at_column()\nRETURNS TRIGGER AS $$\nBEGIN\n  NEW.updated_at = NOW();\n  RETURN NEW;\nEND;\n$$ LANGUAGE plpgsql","DROP TRIGGER IF EXISTS trg_paid_storage_updated_at ON paid_storage","CREATE TRIGGER trg_paid_storage_updated_at\n  BEFORE UPDATE ON paid_storage\n  FOR EACH ROW\n  EXECUTE FUNCTION update_updated_at_column()","COMMENT ON TABLE paid_storage IS 'Daily paid storage cost per nm_id (aggregated by date & nm_id)'","COMMENT ON COLUMN paid_storage.warehouse_price IS 'Sum of warehousePrice for the day & article'","COMMENT ON COLUMN paid_storage.gi_ids IS 'Array of related supply ids (giId) for the aggregated row'"}	paid_storage
10	{"-- ========================================================================\n-- Create cost_price table for product cost prices from Google Sheets\n-- ========================================================================\n\nCREATE TABLE IF NOT EXISTS cost_price (\n  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n\n  -- Relations\n  product_id UUID REFERENCES products(id) ON DELETE SET NULL,\n\n  -- Business keys\n  nm_id BIGINT NOT NULL,\n  vendor_code TEXT NOT NULL,\n  date DATE NOT NULL,\n\n  -- Cost price value from Google Sheets\n  cost_price NUMERIC(14,2) NOT NULL\n)","-- Uniqueness: one row per (nm_id, date)\nCREATE UNIQUE INDEX IF NOT EXISTS ux_cost_price_nm_date ON cost_price (nm_id, date)","-- Indexes\nCREATE INDEX IF NOT EXISTS idx_cost_price_nm_id ON cost_price (nm_id)","CREATE INDEX IF NOT EXISTS idx_cost_price_date ON cost_price (date)"}	cost_price
\.


--
-- Data for Name: secrets; Type: TABLE DATA; Schema: vault; Owner: supabase_admin
--

COPY vault.secrets (id, name, description, secret, key_id, nonce, created_at, updated_at) FROM stdin;
\.


--
-- Name: refresh_tokens_id_seq; Type: SEQUENCE SET; Schema: auth; Owner: supabase_auth_admin
--

SELECT pg_catalog.setval('auth.refresh_tokens_id_seq', 1, false);


--
-- Name: product_sizes_serial_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.product_sizes_serial_id_seq', 535, true);


--
-- Name: products_serial_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.products_serial_id_seq', 535, true);


--
-- Name: week_reports_serial_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.week_reports_serial_id_seq', 1, false);


--
-- Name: subscription_id_seq; Type: SEQUENCE SET; Schema: realtime; Owner: supabase_admin
--

SELECT pg_catalog.setval('realtime.subscription_id_seq', 1, false);


--
-- Name: hooks_id_seq; Type: SEQUENCE SET; Schema: supabase_functions; Owner: supabase_functions_admin
--

SELECT pg_catalog.setval('supabase_functions.hooks_id_seq', 1, false);


--
-- Name: extensions extensions_pkey; Type: CONSTRAINT; Schema: _realtime; Owner: supabase_admin
--

ALTER TABLE ONLY _realtime.extensions
    ADD CONSTRAINT extensions_pkey PRIMARY KEY (id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: _realtime; Owner: supabase_admin
--

ALTER TABLE ONLY _realtime.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: tenants tenants_pkey; Type: CONSTRAINT; Schema: _realtime; Owner: supabase_admin
--

ALTER TABLE ONLY _realtime.tenants
    ADD CONSTRAINT tenants_pkey PRIMARY KEY (id);


--
-- Name: mfa_amr_claims amr_id_pk; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.mfa_amr_claims
    ADD CONSTRAINT amr_id_pk PRIMARY KEY (id);


--
-- Name: audit_log_entries audit_log_entries_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.audit_log_entries
    ADD CONSTRAINT audit_log_entries_pkey PRIMARY KEY (id);


--
-- Name: flow_state flow_state_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.flow_state
    ADD CONSTRAINT flow_state_pkey PRIMARY KEY (id);


--
-- Name: identities identities_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.identities
    ADD CONSTRAINT identities_pkey PRIMARY KEY (id);


--
-- Name: identities identities_provider_id_provider_unique; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.identities
    ADD CONSTRAINT identities_provider_id_provider_unique UNIQUE (provider_id, provider);


--
-- Name: instances instances_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.instances
    ADD CONSTRAINT instances_pkey PRIMARY KEY (id);


--
-- Name: mfa_amr_claims mfa_amr_claims_session_id_authentication_method_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.mfa_amr_claims
    ADD CONSTRAINT mfa_amr_claims_session_id_authentication_method_pkey UNIQUE (session_id, authentication_method);


--
-- Name: mfa_challenges mfa_challenges_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.mfa_challenges
    ADD CONSTRAINT mfa_challenges_pkey PRIMARY KEY (id);


--
-- Name: mfa_factors mfa_factors_last_challenged_at_key; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.mfa_factors
    ADD CONSTRAINT mfa_factors_last_challenged_at_key UNIQUE (last_challenged_at);


--
-- Name: mfa_factors mfa_factors_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.mfa_factors
    ADD CONSTRAINT mfa_factors_pkey PRIMARY KEY (id);


--
-- Name: oauth_authorizations oauth_authorizations_authorization_code_key; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.oauth_authorizations
    ADD CONSTRAINT oauth_authorizations_authorization_code_key UNIQUE (authorization_code);


--
-- Name: oauth_authorizations oauth_authorizations_authorization_id_key; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.oauth_authorizations
    ADD CONSTRAINT oauth_authorizations_authorization_id_key UNIQUE (authorization_id);


--
-- Name: oauth_authorizations oauth_authorizations_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.oauth_authorizations
    ADD CONSTRAINT oauth_authorizations_pkey PRIMARY KEY (id);


--
-- Name: oauth_clients oauth_clients_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.oauth_clients
    ADD CONSTRAINT oauth_clients_pkey PRIMARY KEY (id);


--
-- Name: oauth_consents oauth_consents_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.oauth_consents
    ADD CONSTRAINT oauth_consents_pkey PRIMARY KEY (id);


--
-- Name: oauth_consents oauth_consents_user_client_unique; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.oauth_consents
    ADD CONSTRAINT oauth_consents_user_client_unique UNIQUE (user_id, client_id);


--
-- Name: one_time_tokens one_time_tokens_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.one_time_tokens
    ADD CONSTRAINT one_time_tokens_pkey PRIMARY KEY (id);


--
-- Name: refresh_tokens refresh_tokens_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.refresh_tokens
    ADD CONSTRAINT refresh_tokens_pkey PRIMARY KEY (id);


--
-- Name: refresh_tokens refresh_tokens_token_unique; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.refresh_tokens
    ADD CONSTRAINT refresh_tokens_token_unique UNIQUE (token);


--
-- Name: saml_providers saml_providers_entity_id_key; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.saml_providers
    ADD CONSTRAINT saml_providers_entity_id_key UNIQUE (entity_id);


--
-- Name: saml_providers saml_providers_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.saml_providers
    ADD CONSTRAINT saml_providers_pkey PRIMARY KEY (id);


--
-- Name: saml_relay_states saml_relay_states_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.saml_relay_states
    ADD CONSTRAINT saml_relay_states_pkey PRIMARY KEY (id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: sessions sessions_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.sessions
    ADD CONSTRAINT sessions_pkey PRIMARY KEY (id);


--
-- Name: sso_domains sso_domains_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.sso_domains
    ADD CONSTRAINT sso_domains_pkey PRIMARY KEY (id);


--
-- Name: sso_providers sso_providers_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.sso_providers
    ADD CONSTRAINT sso_providers_pkey PRIMARY KEY (id);


--
-- Name: users users_phone_key; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.users
    ADD CONSTRAINT users_phone_key UNIQUE (phone);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: adv_campaign_daily_stats adv_campaign_daily_stats_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.adv_campaign_daily_stats
    ADD CONSTRAINT adv_campaign_daily_stats_pkey PRIMARY KEY (id);


--
-- Name: adv_params adv_params_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.adv_params
    ADD CONSTRAINT adv_params_pkey PRIMARY KEY (id);


--
-- Name: cost_price cost_price_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cost_price
    ADD CONSTRAINT cost_price_pkey PRIMARY KEY (id);


--
-- Name: cr_daily_stats cr_daily_stats_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cr_daily_stats
    ADD CONSTRAINT cr_daily_stats_pkey PRIMARY KEY (id);


--
-- Name: paid_acceptance paid_acceptance_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.paid_acceptance
    ADD CONSTRAINT paid_acceptance_pkey PRIMARY KEY (id);


--
-- Name: paid_storage paid_storage_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.paid_storage
    ADD CONSTRAINT paid_storage_pkey PRIMARY KEY (id);


--
-- Name: product_sizes product_sizes_barcode_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.product_sizes
    ADD CONSTRAINT product_sizes_barcode_key UNIQUE (barcode);


--
-- Name: product_sizes product_sizes_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.product_sizes
    ADD CONSTRAINT product_sizes_pkey PRIMARY KEY (id);


--
-- Name: product_sizes product_sizes_serial_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.product_sizes
    ADD CONSTRAINT product_sizes_serial_id_key UNIQUE (serial_id);


--
-- Name: products products_nm_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.products
    ADD CONSTRAINT products_nm_id_key UNIQUE (nm_id);


--
-- Name: products products_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.products
    ADD CONSTRAINT products_pkey PRIMARY KEY (id);


--
-- Name: products products_serial_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.products
    ADD CONSTRAINT products_serial_id_key UNIQUE (serial_id);


--
-- Name: products products_vendor_code_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.products
    ADD CONSTRAINT products_vendor_code_key UNIQUE (vendor_code);


--
-- Name: week_reports week_reports_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.week_reports
    ADD CONSTRAINT week_reports_pkey PRIMARY KEY (id);


--
-- Name: week_reports week_reports_realizationreport_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.week_reports
    ADD CONSTRAINT week_reports_realizationreport_id_key UNIQUE (realizationreport_id);


--
-- Name: week_rows week_rows_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.week_rows
    ADD CONSTRAINT week_rows_pkey PRIMARY KEY (id);


--
-- Name: week_stats week_stats_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.week_stats
    ADD CONSTRAINT week_stats_pkey PRIMARY KEY (id);


--
-- Name: messages messages_pkey; Type: CONSTRAINT; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER TABLE ONLY realtime.messages
    ADD CONSTRAINT messages_pkey PRIMARY KEY (id, inserted_at);


--
-- Name: messages_2025_11_12 messages_2025_11_12_pkey; Type: CONSTRAINT; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages_2025_11_12
    ADD CONSTRAINT messages_2025_11_12_pkey PRIMARY KEY (id, inserted_at);


--
-- Name: messages_2025_11_13 messages_2025_11_13_pkey; Type: CONSTRAINT; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages_2025_11_13
    ADD CONSTRAINT messages_2025_11_13_pkey PRIMARY KEY (id, inserted_at);


--
-- Name: messages_2025_11_14 messages_2025_11_14_pkey; Type: CONSTRAINT; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages_2025_11_14
    ADD CONSTRAINT messages_2025_11_14_pkey PRIMARY KEY (id, inserted_at);


--
-- Name: messages_2025_11_15 messages_2025_11_15_pkey; Type: CONSTRAINT; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages_2025_11_15
    ADD CONSTRAINT messages_2025_11_15_pkey PRIMARY KEY (id, inserted_at);


--
-- Name: messages_2025_11_16 messages_2025_11_16_pkey; Type: CONSTRAINT; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages_2025_11_16
    ADD CONSTRAINT messages_2025_11_16_pkey PRIMARY KEY (id, inserted_at);


--
-- Name: subscription pk_subscription; Type: CONSTRAINT; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.subscription
    ADD CONSTRAINT pk_subscription PRIMARY KEY (id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: buckets_analytics buckets_analytics_pkey; Type: CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.buckets_analytics
    ADD CONSTRAINT buckets_analytics_pkey PRIMARY KEY (id);


--
-- Name: buckets buckets_pkey; Type: CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.buckets
    ADD CONSTRAINT buckets_pkey PRIMARY KEY (id);


--
-- Name: iceberg_namespaces iceberg_namespaces_pkey; Type: CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.iceberg_namespaces
    ADD CONSTRAINT iceberg_namespaces_pkey PRIMARY KEY (id);


--
-- Name: iceberg_tables iceberg_tables_pkey; Type: CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.iceberg_tables
    ADD CONSTRAINT iceberg_tables_pkey PRIMARY KEY (id);


--
-- Name: migrations migrations_name_key; Type: CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.migrations
    ADD CONSTRAINT migrations_name_key UNIQUE (name);


--
-- Name: migrations migrations_pkey; Type: CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.migrations
    ADD CONSTRAINT migrations_pkey PRIMARY KEY (id);


--
-- Name: objects objects_pkey; Type: CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.objects
    ADD CONSTRAINT objects_pkey PRIMARY KEY (id);


--
-- Name: prefixes prefixes_pkey; Type: CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.prefixes
    ADD CONSTRAINT prefixes_pkey PRIMARY KEY (bucket_id, level, name);


--
-- Name: s3_multipart_uploads_parts s3_multipart_uploads_parts_pkey; Type: CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.s3_multipart_uploads_parts
    ADD CONSTRAINT s3_multipart_uploads_parts_pkey PRIMARY KEY (id);


--
-- Name: s3_multipart_uploads s3_multipart_uploads_pkey; Type: CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.s3_multipart_uploads
    ADD CONSTRAINT s3_multipart_uploads_pkey PRIMARY KEY (id);


--
-- Name: hooks hooks_pkey; Type: CONSTRAINT; Schema: supabase_functions; Owner: supabase_functions_admin
--

ALTER TABLE ONLY supabase_functions.hooks
    ADD CONSTRAINT hooks_pkey PRIMARY KEY (id);


--
-- Name: migrations migrations_pkey; Type: CONSTRAINT; Schema: supabase_functions; Owner: supabase_functions_admin
--

ALTER TABLE ONLY supabase_functions.migrations
    ADD CONSTRAINT migrations_pkey PRIMARY KEY (version);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: supabase_migrations; Owner: postgres
--

ALTER TABLE ONLY supabase_migrations.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: extensions_tenant_external_id_index; Type: INDEX; Schema: _realtime; Owner: supabase_admin
--

CREATE INDEX extensions_tenant_external_id_index ON _realtime.extensions USING btree (tenant_external_id);


--
-- Name: extensions_tenant_external_id_type_index; Type: INDEX; Schema: _realtime; Owner: supabase_admin
--

CREATE UNIQUE INDEX extensions_tenant_external_id_type_index ON _realtime.extensions USING btree (tenant_external_id, type);


--
-- Name: tenants_external_id_index; Type: INDEX; Schema: _realtime; Owner: supabase_admin
--

CREATE UNIQUE INDEX tenants_external_id_index ON _realtime.tenants USING btree (external_id);


--
-- Name: audit_logs_instance_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX audit_logs_instance_id_idx ON auth.audit_log_entries USING btree (instance_id);


--
-- Name: confirmation_token_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE UNIQUE INDEX confirmation_token_idx ON auth.users USING btree (confirmation_token) WHERE ((confirmation_token)::text !~ '^[0-9 ]*$'::text);


--
-- Name: email_change_token_current_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE UNIQUE INDEX email_change_token_current_idx ON auth.users USING btree (email_change_token_current) WHERE ((email_change_token_current)::text !~ '^[0-9 ]*$'::text);


--
-- Name: email_change_token_new_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE UNIQUE INDEX email_change_token_new_idx ON auth.users USING btree (email_change_token_new) WHERE ((email_change_token_new)::text !~ '^[0-9 ]*$'::text);


--
-- Name: factor_id_created_at_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX factor_id_created_at_idx ON auth.mfa_factors USING btree (user_id, created_at);


--
-- Name: flow_state_created_at_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX flow_state_created_at_idx ON auth.flow_state USING btree (created_at DESC);


--
-- Name: identities_email_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX identities_email_idx ON auth.identities USING btree (email text_pattern_ops);


--
-- Name: INDEX identities_email_idx; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON INDEX auth.identities_email_idx IS 'Auth: Ensures indexed queries on the email column';


--
-- Name: identities_user_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX identities_user_id_idx ON auth.identities USING btree (user_id);


--
-- Name: idx_auth_code; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX idx_auth_code ON auth.flow_state USING btree (auth_code);


--
-- Name: idx_user_id_auth_method; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX idx_user_id_auth_method ON auth.flow_state USING btree (user_id, authentication_method);


--
-- Name: mfa_challenge_created_at_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX mfa_challenge_created_at_idx ON auth.mfa_challenges USING btree (created_at DESC);


--
-- Name: mfa_factors_user_friendly_name_unique; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE UNIQUE INDEX mfa_factors_user_friendly_name_unique ON auth.mfa_factors USING btree (friendly_name, user_id) WHERE (TRIM(BOTH FROM friendly_name) <> ''::text);


--
-- Name: mfa_factors_user_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX mfa_factors_user_id_idx ON auth.mfa_factors USING btree (user_id);


--
-- Name: oauth_auth_pending_exp_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX oauth_auth_pending_exp_idx ON auth.oauth_authorizations USING btree (expires_at) WHERE (status = 'pending'::auth.oauth_authorization_status);


--
-- Name: oauth_clients_deleted_at_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX oauth_clients_deleted_at_idx ON auth.oauth_clients USING btree (deleted_at);


--
-- Name: oauth_consents_active_client_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX oauth_consents_active_client_idx ON auth.oauth_consents USING btree (client_id) WHERE (revoked_at IS NULL);


--
-- Name: oauth_consents_active_user_client_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX oauth_consents_active_user_client_idx ON auth.oauth_consents USING btree (user_id, client_id) WHERE (revoked_at IS NULL);


--
-- Name: oauth_consents_user_order_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX oauth_consents_user_order_idx ON auth.oauth_consents USING btree (user_id, granted_at DESC);


--
-- Name: one_time_tokens_relates_to_hash_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX one_time_tokens_relates_to_hash_idx ON auth.one_time_tokens USING hash (relates_to);


--
-- Name: one_time_tokens_token_hash_hash_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX one_time_tokens_token_hash_hash_idx ON auth.one_time_tokens USING hash (token_hash);


--
-- Name: one_time_tokens_user_id_token_type_key; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE UNIQUE INDEX one_time_tokens_user_id_token_type_key ON auth.one_time_tokens USING btree (user_id, token_type);


--
-- Name: reauthentication_token_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE UNIQUE INDEX reauthentication_token_idx ON auth.users USING btree (reauthentication_token) WHERE ((reauthentication_token)::text !~ '^[0-9 ]*$'::text);


--
-- Name: recovery_token_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE UNIQUE INDEX recovery_token_idx ON auth.users USING btree (recovery_token) WHERE ((recovery_token)::text !~ '^[0-9 ]*$'::text);


--
-- Name: refresh_tokens_instance_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX refresh_tokens_instance_id_idx ON auth.refresh_tokens USING btree (instance_id);


--
-- Name: refresh_tokens_instance_id_user_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX refresh_tokens_instance_id_user_id_idx ON auth.refresh_tokens USING btree (instance_id, user_id);


--
-- Name: refresh_tokens_parent_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX refresh_tokens_parent_idx ON auth.refresh_tokens USING btree (parent);


--
-- Name: refresh_tokens_session_id_revoked_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX refresh_tokens_session_id_revoked_idx ON auth.refresh_tokens USING btree (session_id, revoked);


--
-- Name: refresh_tokens_updated_at_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX refresh_tokens_updated_at_idx ON auth.refresh_tokens USING btree (updated_at DESC);


--
-- Name: saml_providers_sso_provider_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX saml_providers_sso_provider_id_idx ON auth.saml_providers USING btree (sso_provider_id);


--
-- Name: saml_relay_states_created_at_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX saml_relay_states_created_at_idx ON auth.saml_relay_states USING btree (created_at DESC);


--
-- Name: saml_relay_states_for_email_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX saml_relay_states_for_email_idx ON auth.saml_relay_states USING btree (for_email);


--
-- Name: saml_relay_states_sso_provider_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX saml_relay_states_sso_provider_id_idx ON auth.saml_relay_states USING btree (sso_provider_id);


--
-- Name: sessions_not_after_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX sessions_not_after_idx ON auth.sessions USING btree (not_after DESC);


--
-- Name: sessions_oauth_client_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX sessions_oauth_client_id_idx ON auth.sessions USING btree (oauth_client_id);


--
-- Name: sessions_user_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX sessions_user_id_idx ON auth.sessions USING btree (user_id);


--
-- Name: sso_domains_domain_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE UNIQUE INDEX sso_domains_domain_idx ON auth.sso_domains USING btree (lower(domain));


--
-- Name: sso_domains_sso_provider_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX sso_domains_sso_provider_id_idx ON auth.sso_domains USING btree (sso_provider_id);


--
-- Name: sso_providers_resource_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE UNIQUE INDEX sso_providers_resource_id_idx ON auth.sso_providers USING btree (lower(resource_id));


--
-- Name: sso_providers_resource_id_pattern_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX sso_providers_resource_id_pattern_idx ON auth.sso_providers USING btree (resource_id text_pattern_ops);


--
-- Name: unique_phone_factor_per_user; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE UNIQUE INDEX unique_phone_factor_per_user ON auth.mfa_factors USING btree (user_id, phone);


--
-- Name: user_id_created_at_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX user_id_created_at_idx ON auth.sessions USING btree (user_id, created_at);


--
-- Name: users_email_partial_key; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE UNIQUE INDEX users_email_partial_key ON auth.users USING btree (email) WHERE (is_sso_user = false);


--
-- Name: INDEX users_email_partial_key; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON INDEX auth.users_email_partial_key IS 'Auth: A partial unique index that applies only when is_sso_user is false';


--
-- Name: users_instance_id_email_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX users_instance_id_email_idx ON auth.users USING btree (instance_id, lower((email)::text));


--
-- Name: users_instance_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX users_instance_id_idx ON auth.users USING btree (instance_id);


--
-- Name: users_is_anonymous_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX users_is_anonymous_idx ON auth.users USING btree (is_anonymous);


--
-- Name: idx_adv_daily_advert_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_adv_daily_advert_id ON public.adv_campaign_daily_stats USING btree (advert_id);


--
-- Name: idx_adv_daily_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_adv_daily_date ON public.adv_campaign_daily_stats USING btree (date);


--
-- Name: idx_adv_daily_nm_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_adv_daily_nm_date ON public.adv_campaign_daily_stats USING btree (nm_id, date);


--
-- Name: idx_adv_daily_nm_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_adv_daily_nm_id ON public.adv_campaign_daily_stats USING btree (nm_id);


--
-- Name: idx_adv_daily_vendor_code; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_adv_daily_vendor_code ON public.adv_campaign_daily_stats USING btree (vendor_code);


--
-- Name: idx_adv_params_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_adv_params_date ON public.adv_params USING btree (date);


--
-- Name: idx_adv_params_nm_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_adv_params_nm_id ON public.adv_params USING btree (nm_id);


--
-- Name: idx_adv_params_vendor_code; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_adv_params_vendor_code ON public.adv_params USING btree (vendor_code);


--
-- Name: idx_cost_price_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_cost_price_date ON public.cost_price USING btree (date);


--
-- Name: idx_cost_price_nm_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_cost_price_nm_id ON public.cost_price USING btree (nm_id);


--
-- Name: idx_cr_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_cr_date ON public.cr_daily_stats USING btree (date_of_period);


--
-- Name: idx_cr_nm_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_cr_nm_date ON public.cr_daily_stats USING btree (nm_id, date_of_period);


--
-- Name: idx_cr_product_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_cr_product_id ON public.cr_daily_stats USING btree (product_id);


--
-- Name: idx_cr_vendor_code; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_cr_vendor_code ON public.cr_daily_stats USING btree (vendor_code);


--
-- Name: idx_paid_acceptance_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_paid_acceptance_date ON public.paid_acceptance USING btree (shk_create_date);


--
-- Name: idx_paid_acceptance_nm_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_paid_acceptance_nm_id ON public.paid_acceptance USING btree (nm_id);


--
-- Name: idx_paid_storage_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_paid_storage_date ON public.paid_storage USING btree (date);


--
-- Name: idx_paid_storage_nm_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_paid_storage_nm_id ON public.paid_storage USING btree (nm_id);


--
-- Name: idx_product_sizes_barcode; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_product_sizes_barcode ON public.product_sizes USING btree (barcode);


--
-- Name: idx_product_sizes_product_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_product_sizes_product_id ON public.product_sizes USING btree (product_id);


--
-- Name: idx_products_category; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_products_category ON public.products USING btree (category_wb);


--
-- Name: idx_week_reports_realizationreport_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_week_reports_realizationreport_id ON public.week_reports USING btree (realizationreport_id);


--
-- Name: idx_week_rows_nm_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_week_rows_nm_id ON public.week_rows USING btree (nm_id);


--
-- Name: idx_week_rows_product_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_week_rows_product_id ON public.week_rows USING btree (product_id);


--
-- Name: idx_week_rows_realizationreport_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_week_rows_realizationreport_id ON public.week_rows USING btree (realizationreport_id);


--
-- Name: idx_week_rows_sale_dt; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_week_rows_sale_dt ON public.week_rows USING btree (sale_dt);


--
-- Name: idx_week_rows_unique_rr_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX idx_week_rows_unique_rr_id ON public.week_rows USING btree (realizationreport_id, rr_id);


--
-- Name: idx_week_stats_nm_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_week_stats_nm_id ON public.week_stats USING btree (nm_id);


--
-- Name: idx_week_stats_realizationreport_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_week_stats_realizationreport_id ON public.week_stats USING btree (realizationreport_id);


--
-- Name: idx_week_stats_unique; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX idx_week_stats_unique ON public.week_stats USING btree (realizationreport_id, nm_id);


--
-- Name: ux_adv_daily_stats; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX ux_adv_daily_stats ON public.adv_campaign_daily_stats USING btree (advert_id, nm_id, date);


--
-- Name: ux_adv_params_nm_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX ux_adv_params_nm_date ON public.adv_params USING btree (nm_id, date);


--
-- Name: ux_cost_price_nm_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX ux_cost_price_nm_date ON public.cost_price USING btree (nm_id, date);


--
-- Name: ux_cr_daily; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX ux_cr_daily ON public.cr_daily_stats USING btree (nm_id, date_of_period);


--
-- Name: ux_paid_acceptance_nm_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX ux_paid_acceptance_nm_date ON public.paid_acceptance USING btree (nm_id, shk_create_date);


--
-- Name: ux_paid_storage_nm_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX ux_paid_storage_nm_date ON public.paid_storage USING btree (nm_id, date);


--
-- Name: ix_realtime_subscription_entity; Type: INDEX; Schema: realtime; Owner: supabase_admin
--

CREATE INDEX ix_realtime_subscription_entity ON realtime.subscription USING btree (entity);


--
-- Name: messages_inserted_at_topic_index; Type: INDEX; Schema: realtime; Owner: supabase_realtime_admin
--

CREATE INDEX messages_inserted_at_topic_index ON ONLY realtime.messages USING btree (inserted_at DESC, topic) WHERE ((extension = 'broadcast'::text) AND (private IS TRUE));


--
-- Name: messages_2025_11_12_inserted_at_topic_idx; Type: INDEX; Schema: realtime; Owner: supabase_admin
--

CREATE INDEX messages_2025_11_12_inserted_at_topic_idx ON realtime.messages_2025_11_12 USING btree (inserted_at DESC, topic) WHERE ((extension = 'broadcast'::text) AND (private IS TRUE));


--
-- Name: messages_2025_11_13_inserted_at_topic_idx; Type: INDEX; Schema: realtime; Owner: supabase_admin
--

CREATE INDEX messages_2025_11_13_inserted_at_topic_idx ON realtime.messages_2025_11_13 USING btree (inserted_at DESC, topic) WHERE ((extension = 'broadcast'::text) AND (private IS TRUE));


--
-- Name: messages_2025_11_14_inserted_at_topic_idx; Type: INDEX; Schema: realtime; Owner: supabase_admin
--

CREATE INDEX messages_2025_11_14_inserted_at_topic_idx ON realtime.messages_2025_11_14 USING btree (inserted_at DESC, topic) WHERE ((extension = 'broadcast'::text) AND (private IS TRUE));


--
-- Name: messages_2025_11_15_inserted_at_topic_idx; Type: INDEX; Schema: realtime; Owner: supabase_admin
--

CREATE INDEX messages_2025_11_15_inserted_at_topic_idx ON realtime.messages_2025_11_15 USING btree (inserted_at DESC, topic) WHERE ((extension = 'broadcast'::text) AND (private IS TRUE));


--
-- Name: messages_2025_11_16_inserted_at_topic_idx; Type: INDEX; Schema: realtime; Owner: supabase_admin
--

CREATE INDEX messages_2025_11_16_inserted_at_topic_idx ON realtime.messages_2025_11_16 USING btree (inserted_at DESC, topic) WHERE ((extension = 'broadcast'::text) AND (private IS TRUE));


--
-- Name: subscription_subscription_id_entity_filters_key; Type: INDEX; Schema: realtime; Owner: supabase_admin
--

CREATE UNIQUE INDEX subscription_subscription_id_entity_filters_key ON realtime.subscription USING btree (subscription_id, entity, filters);


--
-- Name: bname; Type: INDEX; Schema: storage; Owner: supabase_storage_admin
--

CREATE UNIQUE INDEX bname ON storage.buckets USING btree (name);


--
-- Name: bucketid_objname; Type: INDEX; Schema: storage; Owner: supabase_storage_admin
--

CREATE UNIQUE INDEX bucketid_objname ON storage.objects USING btree (bucket_id, name);


--
-- Name: idx_iceberg_namespaces_bucket_id; Type: INDEX; Schema: storage; Owner: supabase_storage_admin
--

CREATE UNIQUE INDEX idx_iceberg_namespaces_bucket_id ON storage.iceberg_namespaces USING btree (bucket_id, name);


--
-- Name: idx_iceberg_tables_namespace_id; Type: INDEX; Schema: storage; Owner: supabase_storage_admin
--

CREATE UNIQUE INDEX idx_iceberg_tables_namespace_id ON storage.iceberg_tables USING btree (namespace_id, name);


--
-- Name: idx_multipart_uploads_list; Type: INDEX; Schema: storage; Owner: supabase_storage_admin
--

CREATE INDEX idx_multipart_uploads_list ON storage.s3_multipart_uploads USING btree (bucket_id, key, created_at);


--
-- Name: idx_name_bucket_level_unique; Type: INDEX; Schema: storage; Owner: supabase_storage_admin
--

CREATE UNIQUE INDEX idx_name_bucket_level_unique ON storage.objects USING btree (name COLLATE "C", bucket_id, level);


--
-- Name: idx_objects_bucket_id_name; Type: INDEX; Schema: storage; Owner: supabase_storage_admin
--

CREATE INDEX idx_objects_bucket_id_name ON storage.objects USING btree (bucket_id, name COLLATE "C");


--
-- Name: idx_objects_lower_name; Type: INDEX; Schema: storage; Owner: supabase_storage_admin
--

CREATE INDEX idx_objects_lower_name ON storage.objects USING btree ((path_tokens[level]), lower(name) text_pattern_ops, bucket_id, level);


--
-- Name: idx_prefixes_lower_name; Type: INDEX; Schema: storage; Owner: supabase_storage_admin
--

CREATE INDEX idx_prefixes_lower_name ON storage.prefixes USING btree (bucket_id, level, ((string_to_array(name, '/'::text))[level]), lower(name) text_pattern_ops);


--
-- Name: name_prefix_search; Type: INDEX; Schema: storage; Owner: supabase_storage_admin
--

CREATE INDEX name_prefix_search ON storage.objects USING btree (name text_pattern_ops);


--
-- Name: objects_bucket_id_level_idx; Type: INDEX; Schema: storage; Owner: supabase_storage_admin
--

CREATE UNIQUE INDEX objects_bucket_id_level_idx ON storage.objects USING btree (bucket_id, level, name COLLATE "C");


--
-- Name: supabase_functions_hooks_h_table_id_h_name_idx; Type: INDEX; Schema: supabase_functions; Owner: supabase_functions_admin
--

CREATE INDEX supabase_functions_hooks_h_table_id_h_name_idx ON supabase_functions.hooks USING btree (hook_table_id, hook_name);


--
-- Name: supabase_functions_hooks_request_id_idx; Type: INDEX; Schema: supabase_functions; Owner: supabase_functions_admin
--

CREATE INDEX supabase_functions_hooks_request_id_idx ON supabase_functions.hooks USING btree (request_id);


--
-- Name: messages_2025_11_12_inserted_at_topic_idx; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_inserted_at_topic_index ATTACH PARTITION realtime.messages_2025_11_12_inserted_at_topic_idx;


--
-- Name: messages_2025_11_12_pkey; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_pkey ATTACH PARTITION realtime.messages_2025_11_12_pkey;


--
-- Name: messages_2025_11_13_inserted_at_topic_idx; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_inserted_at_topic_index ATTACH PARTITION realtime.messages_2025_11_13_inserted_at_topic_idx;


--
-- Name: messages_2025_11_13_pkey; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_pkey ATTACH PARTITION realtime.messages_2025_11_13_pkey;


--
-- Name: messages_2025_11_14_inserted_at_topic_idx; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_inserted_at_topic_index ATTACH PARTITION realtime.messages_2025_11_14_inserted_at_topic_idx;


--
-- Name: messages_2025_11_14_pkey; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_pkey ATTACH PARTITION realtime.messages_2025_11_14_pkey;


--
-- Name: messages_2025_11_15_inserted_at_topic_idx; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_inserted_at_topic_index ATTACH PARTITION realtime.messages_2025_11_15_inserted_at_topic_idx;


--
-- Name: messages_2025_11_15_pkey; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_pkey ATTACH PARTITION realtime.messages_2025_11_15_pkey;


--
-- Name: messages_2025_11_16_inserted_at_topic_idx; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_inserted_at_topic_index ATTACH PARTITION realtime.messages_2025_11_16_inserted_at_topic_idx;


--
-- Name: messages_2025_11_16_pkey; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_pkey ATTACH PARTITION realtime.messages_2025_11_16_pkey;


--
-- Name: paid_acceptance trg_paid_acceptance_updated_at; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_paid_acceptance_updated_at BEFORE UPDATE ON public.paid_acceptance FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


--
-- Name: paid_storage trg_paid_storage_updated_at; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_paid_storage_updated_at BEFORE UPDATE ON public.paid_storage FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


--
-- Name: adv_campaign_daily_stats update_adv_campaign_daily_stats_updated_at; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER update_adv_campaign_daily_stats_updated_at BEFORE UPDATE ON public.adv_campaign_daily_stats FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


--
-- Name: cr_daily_stats update_cr_daily_stats_updated_at; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER update_cr_daily_stats_updated_at BEFORE UPDATE ON public.cr_daily_stats FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


--
-- Name: TRIGGER update_cr_daily_stats_updated_at ON cr_daily_stats; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TRIGGER update_cr_daily_stats_updated_at ON public.cr_daily_stats IS 'Автоматически обновляет updated_at при UPDATE записи';


--
-- Name: subscription tr_check_filters; Type: TRIGGER; Schema: realtime; Owner: supabase_admin
--

CREATE TRIGGER tr_check_filters BEFORE INSERT OR UPDATE ON realtime.subscription FOR EACH ROW EXECUTE FUNCTION realtime.subscription_check_filters();


--
-- Name: buckets enforce_bucket_name_length_trigger; Type: TRIGGER; Schema: storage; Owner: supabase_storage_admin
--

CREATE TRIGGER enforce_bucket_name_length_trigger BEFORE INSERT OR UPDATE OF name ON storage.buckets FOR EACH ROW EXECUTE FUNCTION storage.enforce_bucket_name_length();


--
-- Name: objects objects_delete_delete_prefix; Type: TRIGGER; Schema: storage; Owner: supabase_storage_admin
--

CREATE TRIGGER objects_delete_delete_prefix AFTER DELETE ON storage.objects FOR EACH ROW EXECUTE FUNCTION storage.delete_prefix_hierarchy_trigger();


--
-- Name: objects objects_insert_create_prefix; Type: TRIGGER; Schema: storage; Owner: supabase_storage_admin
--

CREATE TRIGGER objects_insert_create_prefix BEFORE INSERT ON storage.objects FOR EACH ROW EXECUTE FUNCTION storage.objects_insert_prefix_trigger();


--
-- Name: objects objects_update_create_prefix; Type: TRIGGER; Schema: storage; Owner: supabase_storage_admin
--

CREATE TRIGGER objects_update_create_prefix BEFORE UPDATE ON storage.objects FOR EACH ROW WHEN (((new.name <> old.name) OR (new.bucket_id <> old.bucket_id))) EXECUTE FUNCTION storage.objects_update_prefix_trigger();


--
-- Name: prefixes prefixes_create_hierarchy; Type: TRIGGER; Schema: storage; Owner: supabase_storage_admin
--

CREATE TRIGGER prefixes_create_hierarchy BEFORE INSERT ON storage.prefixes FOR EACH ROW WHEN ((pg_trigger_depth() < 1)) EXECUTE FUNCTION storage.prefixes_insert_trigger();


--
-- Name: prefixes prefixes_delete_hierarchy; Type: TRIGGER; Schema: storage; Owner: supabase_storage_admin
--

CREATE TRIGGER prefixes_delete_hierarchy AFTER DELETE ON storage.prefixes FOR EACH ROW EXECUTE FUNCTION storage.delete_prefix_hierarchy_trigger();


--
-- Name: objects update_objects_updated_at; Type: TRIGGER; Schema: storage; Owner: supabase_storage_admin
--

CREATE TRIGGER update_objects_updated_at BEFORE UPDATE ON storage.objects FOR EACH ROW EXECUTE FUNCTION storage.update_updated_at_column();


--
-- Name: extensions extensions_tenant_external_id_fkey; Type: FK CONSTRAINT; Schema: _realtime; Owner: supabase_admin
--

ALTER TABLE ONLY _realtime.extensions
    ADD CONSTRAINT extensions_tenant_external_id_fkey FOREIGN KEY (tenant_external_id) REFERENCES _realtime.tenants(external_id) ON DELETE CASCADE;


--
-- Name: identities identities_user_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.identities
    ADD CONSTRAINT identities_user_id_fkey FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;


--
-- Name: mfa_amr_claims mfa_amr_claims_session_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.mfa_amr_claims
    ADD CONSTRAINT mfa_amr_claims_session_id_fkey FOREIGN KEY (session_id) REFERENCES auth.sessions(id) ON DELETE CASCADE;


--
-- Name: mfa_challenges mfa_challenges_auth_factor_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.mfa_challenges
    ADD CONSTRAINT mfa_challenges_auth_factor_id_fkey FOREIGN KEY (factor_id) REFERENCES auth.mfa_factors(id) ON DELETE CASCADE;


--
-- Name: mfa_factors mfa_factors_user_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.mfa_factors
    ADD CONSTRAINT mfa_factors_user_id_fkey FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;


--
-- Name: oauth_authorizations oauth_authorizations_client_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.oauth_authorizations
    ADD CONSTRAINT oauth_authorizations_client_id_fkey FOREIGN KEY (client_id) REFERENCES auth.oauth_clients(id) ON DELETE CASCADE;


--
-- Name: oauth_authorizations oauth_authorizations_user_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.oauth_authorizations
    ADD CONSTRAINT oauth_authorizations_user_id_fkey FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;


--
-- Name: oauth_consents oauth_consents_client_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.oauth_consents
    ADD CONSTRAINT oauth_consents_client_id_fkey FOREIGN KEY (client_id) REFERENCES auth.oauth_clients(id) ON DELETE CASCADE;


--
-- Name: oauth_consents oauth_consents_user_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.oauth_consents
    ADD CONSTRAINT oauth_consents_user_id_fkey FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;


--
-- Name: one_time_tokens one_time_tokens_user_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.one_time_tokens
    ADD CONSTRAINT one_time_tokens_user_id_fkey FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;


--
-- Name: refresh_tokens refresh_tokens_session_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.refresh_tokens
    ADD CONSTRAINT refresh_tokens_session_id_fkey FOREIGN KEY (session_id) REFERENCES auth.sessions(id) ON DELETE CASCADE;


--
-- Name: saml_providers saml_providers_sso_provider_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.saml_providers
    ADD CONSTRAINT saml_providers_sso_provider_id_fkey FOREIGN KEY (sso_provider_id) REFERENCES auth.sso_providers(id) ON DELETE CASCADE;


--
-- Name: saml_relay_states saml_relay_states_flow_state_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.saml_relay_states
    ADD CONSTRAINT saml_relay_states_flow_state_id_fkey FOREIGN KEY (flow_state_id) REFERENCES auth.flow_state(id) ON DELETE CASCADE;


--
-- Name: saml_relay_states saml_relay_states_sso_provider_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.saml_relay_states
    ADD CONSTRAINT saml_relay_states_sso_provider_id_fkey FOREIGN KEY (sso_provider_id) REFERENCES auth.sso_providers(id) ON DELETE CASCADE;


--
-- Name: sessions sessions_oauth_client_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.sessions
    ADD CONSTRAINT sessions_oauth_client_id_fkey FOREIGN KEY (oauth_client_id) REFERENCES auth.oauth_clients(id) ON DELETE CASCADE;


--
-- Name: sessions sessions_user_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.sessions
    ADD CONSTRAINT sessions_user_id_fkey FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;


--
-- Name: sso_domains sso_domains_sso_provider_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.sso_domains
    ADD CONSTRAINT sso_domains_sso_provider_id_fkey FOREIGN KEY (sso_provider_id) REFERENCES auth.sso_providers(id) ON DELETE CASCADE;


--
-- Name: adv_params adv_params_nm_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.adv_params
    ADD CONSTRAINT adv_params_nm_id_fkey FOREIGN KEY (nm_id) REFERENCES public.products(nm_id) ON DELETE CASCADE;


--
-- Name: cost_price cost_price_product_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cost_price
    ADD CONSTRAINT cost_price_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.products(id) ON DELETE SET NULL;


--
-- Name: cr_daily_stats cr_daily_stats_product_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cr_daily_stats
    ADD CONSTRAINT cr_daily_stats_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.products(id) ON DELETE CASCADE;


--
-- Name: paid_acceptance paid_acceptance_product_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.paid_acceptance
    ADD CONSTRAINT paid_acceptance_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.products(id) ON DELETE SET NULL;


--
-- Name: paid_storage paid_storage_product_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.paid_storage
    ADD CONSTRAINT paid_storage_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.products(id) ON DELETE SET NULL;


--
-- Name: product_sizes product_sizes_product_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.product_sizes
    ADD CONSTRAINT product_sizes_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.products(id) ON DELETE CASCADE;


--
-- Name: week_rows week_rows_product_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.week_rows
    ADD CONSTRAINT week_rows_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.products(id) ON DELETE CASCADE;


--
-- Name: week_rows week_rows_realizationreport_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.week_rows
    ADD CONSTRAINT week_rows_realizationreport_id_fkey FOREIGN KEY (realizationreport_id) REFERENCES public.week_reports(realizationreport_id) ON DELETE CASCADE;


--
-- Name: week_stats week_stats_product_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.week_stats
    ADD CONSTRAINT week_stats_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.products(id) ON DELETE CASCADE;


--
-- Name: week_stats week_stats_realizationreport_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.week_stats
    ADD CONSTRAINT week_stats_realizationreport_id_fkey FOREIGN KEY (realizationreport_id) REFERENCES public.week_reports(realizationreport_id) ON DELETE CASCADE;


--
-- Name: iceberg_namespaces iceberg_namespaces_bucket_id_fkey; Type: FK CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.iceberg_namespaces
    ADD CONSTRAINT iceberg_namespaces_bucket_id_fkey FOREIGN KEY (bucket_id) REFERENCES storage.buckets_analytics(id) ON DELETE CASCADE;


--
-- Name: iceberg_tables iceberg_tables_bucket_id_fkey; Type: FK CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.iceberg_tables
    ADD CONSTRAINT iceberg_tables_bucket_id_fkey FOREIGN KEY (bucket_id) REFERENCES storage.buckets_analytics(id) ON DELETE CASCADE;


--
-- Name: iceberg_tables iceberg_tables_namespace_id_fkey; Type: FK CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.iceberg_tables
    ADD CONSTRAINT iceberg_tables_namespace_id_fkey FOREIGN KEY (namespace_id) REFERENCES storage.iceberg_namespaces(id) ON DELETE CASCADE;


--
-- Name: objects objects_bucketId_fkey; Type: FK CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.objects
    ADD CONSTRAINT "objects_bucketId_fkey" FOREIGN KEY (bucket_id) REFERENCES storage.buckets(id);


--
-- Name: prefixes prefixes_bucketId_fkey; Type: FK CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.prefixes
    ADD CONSTRAINT "prefixes_bucketId_fkey" FOREIGN KEY (bucket_id) REFERENCES storage.buckets(id);


--
-- Name: s3_multipart_uploads s3_multipart_uploads_bucket_id_fkey; Type: FK CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.s3_multipart_uploads
    ADD CONSTRAINT s3_multipart_uploads_bucket_id_fkey FOREIGN KEY (bucket_id) REFERENCES storage.buckets(id);


--
-- Name: s3_multipart_uploads_parts s3_multipart_uploads_parts_bucket_id_fkey; Type: FK CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.s3_multipart_uploads_parts
    ADD CONSTRAINT s3_multipart_uploads_parts_bucket_id_fkey FOREIGN KEY (bucket_id) REFERENCES storage.buckets(id);


--
-- Name: s3_multipart_uploads_parts s3_multipart_uploads_parts_upload_id_fkey; Type: FK CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.s3_multipart_uploads_parts
    ADD CONSTRAINT s3_multipart_uploads_parts_upload_id_fkey FOREIGN KEY (upload_id) REFERENCES storage.s3_multipart_uploads(id) ON DELETE CASCADE;


--
-- Name: audit_log_entries; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.audit_log_entries ENABLE ROW LEVEL SECURITY;

--
-- Name: flow_state; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.flow_state ENABLE ROW LEVEL SECURITY;

--
-- Name: identities; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.identities ENABLE ROW LEVEL SECURITY;

--
-- Name: instances; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.instances ENABLE ROW LEVEL SECURITY;

--
-- Name: mfa_amr_claims; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.mfa_amr_claims ENABLE ROW LEVEL SECURITY;

--
-- Name: mfa_challenges; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.mfa_challenges ENABLE ROW LEVEL SECURITY;

--
-- Name: mfa_factors; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.mfa_factors ENABLE ROW LEVEL SECURITY;

--
-- Name: one_time_tokens; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.one_time_tokens ENABLE ROW LEVEL SECURITY;

--
-- Name: refresh_tokens; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.refresh_tokens ENABLE ROW LEVEL SECURITY;

--
-- Name: saml_providers; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.saml_providers ENABLE ROW LEVEL SECURITY;

--
-- Name: saml_relay_states; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.saml_relay_states ENABLE ROW LEVEL SECURITY;

--
-- Name: schema_migrations; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.schema_migrations ENABLE ROW LEVEL SECURITY;

--
-- Name: sessions; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.sessions ENABLE ROW LEVEL SECURITY;

--
-- Name: sso_domains; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.sso_domains ENABLE ROW LEVEL SECURITY;

--
-- Name: sso_providers; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.sso_providers ENABLE ROW LEVEL SECURITY;

--
-- Name: users; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.users ENABLE ROW LEVEL SECURITY;

--
-- Name: messages; Type: ROW SECURITY; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER TABLE realtime.messages ENABLE ROW LEVEL SECURITY;

--
-- Name: buckets; Type: ROW SECURITY; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE storage.buckets ENABLE ROW LEVEL SECURITY;

--
-- Name: buckets_analytics; Type: ROW SECURITY; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE storage.buckets_analytics ENABLE ROW LEVEL SECURITY;

--
-- Name: iceberg_namespaces; Type: ROW SECURITY; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE storage.iceberg_namespaces ENABLE ROW LEVEL SECURITY;

--
-- Name: iceberg_tables; Type: ROW SECURITY; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE storage.iceberg_tables ENABLE ROW LEVEL SECURITY;

--
-- Name: migrations; Type: ROW SECURITY; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE storage.migrations ENABLE ROW LEVEL SECURITY;

--
-- Name: objects; Type: ROW SECURITY; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE storage.objects ENABLE ROW LEVEL SECURITY;

--
-- Name: prefixes; Type: ROW SECURITY; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE storage.prefixes ENABLE ROW LEVEL SECURITY;

--
-- Name: s3_multipart_uploads; Type: ROW SECURITY; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE storage.s3_multipart_uploads ENABLE ROW LEVEL SECURITY;

--
-- Name: s3_multipart_uploads_parts; Type: ROW SECURITY; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE storage.s3_multipart_uploads_parts ENABLE ROW LEVEL SECURITY;

--
-- Name: supabase_realtime; Type: PUBLICATION; Schema: -; Owner: postgres
--

CREATE PUBLICATION supabase_realtime WITH (publish = 'insert, update, delete, truncate');


ALTER PUBLICATION supabase_realtime OWNER TO postgres;

--
-- Name: SCHEMA auth; Type: ACL; Schema: -; Owner: supabase_admin
--

GRANT USAGE ON SCHEMA auth TO anon;
GRANT USAGE ON SCHEMA auth TO authenticated;
GRANT USAGE ON SCHEMA auth TO service_role;
GRANT ALL ON SCHEMA auth TO supabase_auth_admin;
GRANT ALL ON SCHEMA auth TO dashboard_user;
GRANT USAGE ON SCHEMA auth TO postgres;


--
-- Name: SCHEMA extensions; Type: ACL; Schema: -; Owner: postgres
--

GRANT USAGE ON SCHEMA extensions TO anon;
GRANT USAGE ON SCHEMA extensions TO authenticated;
GRANT USAGE ON SCHEMA extensions TO service_role;
GRANT ALL ON SCHEMA extensions TO dashboard_user;


--
-- Name: SCHEMA net; Type: ACL; Schema: -; Owner: supabase_admin
--

GRANT USAGE ON SCHEMA net TO supabase_functions_admin;
GRANT USAGE ON SCHEMA net TO postgres;
GRANT USAGE ON SCHEMA net TO anon;
GRANT USAGE ON SCHEMA net TO authenticated;
GRANT USAGE ON SCHEMA net TO service_role;


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: pg_database_owner
--

GRANT USAGE ON SCHEMA public TO postgres;
GRANT USAGE ON SCHEMA public TO anon;
GRANT USAGE ON SCHEMA public TO authenticated;
GRANT USAGE ON SCHEMA public TO service_role;


--
-- Name: SCHEMA realtime; Type: ACL; Schema: -; Owner: supabase_admin
--

GRANT USAGE ON SCHEMA realtime TO postgres;
GRANT USAGE ON SCHEMA realtime TO anon;
GRANT USAGE ON SCHEMA realtime TO authenticated;
GRANT USAGE ON SCHEMA realtime TO service_role;
GRANT ALL ON SCHEMA realtime TO supabase_realtime_admin;


--
-- Name: SCHEMA storage; Type: ACL; Schema: -; Owner: supabase_admin
--

GRANT USAGE ON SCHEMA storage TO postgres WITH GRANT OPTION;
GRANT USAGE ON SCHEMA storage TO anon;
GRANT USAGE ON SCHEMA storage TO authenticated;
GRANT USAGE ON SCHEMA storage TO service_role;
GRANT ALL ON SCHEMA storage TO supabase_storage_admin;
GRANT ALL ON SCHEMA storage TO dashboard_user;


--
-- Name: SCHEMA supabase_functions; Type: ACL; Schema: -; Owner: supabase_admin
--

GRANT USAGE ON SCHEMA supabase_functions TO postgres;
GRANT USAGE ON SCHEMA supabase_functions TO anon;
GRANT USAGE ON SCHEMA supabase_functions TO authenticated;
GRANT USAGE ON SCHEMA supabase_functions TO service_role;
GRANT ALL ON SCHEMA supabase_functions TO supabase_functions_admin;


--
-- Name: SCHEMA vault; Type: ACL; Schema: -; Owner: supabase_admin
--

GRANT USAGE ON SCHEMA vault TO postgres WITH GRANT OPTION;
GRANT USAGE ON SCHEMA vault TO service_role;


--
-- Name: FUNCTION email(); Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT ALL ON FUNCTION auth.email() TO dashboard_user;


--
-- Name: FUNCTION jwt(); Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT ALL ON FUNCTION auth.jwt() TO postgres;
GRANT ALL ON FUNCTION auth.jwt() TO dashboard_user;


--
-- Name: FUNCTION role(); Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT ALL ON FUNCTION auth.role() TO dashboard_user;


--
-- Name: FUNCTION uid(); Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT ALL ON FUNCTION auth.uid() TO dashboard_user;


--
-- Name: FUNCTION armor(bytea); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.armor(bytea) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.armor(bytea) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION armor(bytea, text[], text[]); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.armor(bytea, text[], text[]) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.armor(bytea, text[], text[]) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION crypt(text, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.crypt(text, text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.crypt(text, text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION dearmor(text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.dearmor(text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.dearmor(text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION decrypt(bytea, bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.decrypt(bytea, bytea, text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.decrypt(bytea, bytea, text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION decrypt_iv(bytea, bytea, bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.decrypt_iv(bytea, bytea, bytea, text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.decrypt_iv(bytea, bytea, bytea, text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION digest(bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.digest(bytea, text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.digest(bytea, text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION digest(text, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.digest(text, text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.digest(text, text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION encrypt(bytea, bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.encrypt(bytea, bytea, text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.encrypt(bytea, bytea, text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION encrypt_iv(bytea, bytea, bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.encrypt_iv(bytea, bytea, bytea, text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.encrypt_iv(bytea, bytea, bytea, text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION gen_random_bytes(integer); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.gen_random_bytes(integer) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.gen_random_bytes(integer) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION gen_random_uuid(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.gen_random_uuid() TO dashboard_user;
GRANT ALL ON FUNCTION extensions.gen_random_uuid() TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION gen_salt(text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.gen_salt(text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.gen_salt(text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION gen_salt(text, integer); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.gen_salt(text, integer) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.gen_salt(text, integer) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION grant_pg_cron_access(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

REVOKE ALL ON FUNCTION extensions.grant_pg_cron_access() FROM supabase_admin;
GRANT ALL ON FUNCTION extensions.grant_pg_cron_access() TO supabase_admin WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.grant_pg_cron_access() TO dashboard_user;


--
-- Name: FUNCTION grant_pg_graphql_access(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.grant_pg_graphql_access() TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION grant_pg_net_access(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

REVOKE ALL ON FUNCTION extensions.grant_pg_net_access() FROM supabase_admin;
GRANT ALL ON FUNCTION extensions.grant_pg_net_access() TO supabase_admin WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.grant_pg_net_access() TO dashboard_user;


--
-- Name: FUNCTION hmac(bytea, bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.hmac(bytea, bytea, text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.hmac(bytea, bytea, text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION hmac(text, text, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.hmac(text, text, text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.hmac(text, text, text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pg_stat_statements(showtext boolean, OUT userid oid, OUT dbid oid, OUT toplevel boolean, OUT queryid bigint, OUT query text, OUT plans bigint, OUT total_plan_time double precision, OUT min_plan_time double precision, OUT max_plan_time double precision, OUT mean_plan_time double precision, OUT stddev_plan_time double precision, OUT calls bigint, OUT total_exec_time double precision, OUT min_exec_time double precision, OUT max_exec_time double precision, OUT mean_exec_time double precision, OUT stddev_exec_time double precision, OUT rows bigint, OUT shared_blks_hit bigint, OUT shared_blks_read bigint, OUT shared_blks_dirtied bigint, OUT shared_blks_written bigint, OUT local_blks_hit bigint, OUT local_blks_read bigint, OUT local_blks_dirtied bigint, OUT local_blks_written bigint, OUT temp_blks_read bigint, OUT temp_blks_written bigint, OUT shared_blk_read_time double precision, OUT shared_blk_write_time double precision, OUT local_blk_read_time double precision, OUT local_blk_write_time double precision, OUT temp_blk_read_time double precision, OUT temp_blk_write_time double precision, OUT wal_records bigint, OUT wal_fpi bigint, OUT wal_bytes numeric, OUT jit_functions bigint, OUT jit_generation_time double precision, OUT jit_inlining_count bigint, OUT jit_inlining_time double precision, OUT jit_optimization_count bigint, OUT jit_optimization_time double precision, OUT jit_emission_count bigint, OUT jit_emission_time double precision, OUT jit_deform_count bigint, OUT jit_deform_time double precision, OUT stats_since timestamp with time zone, OUT minmax_stats_since timestamp with time zone); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pg_stat_statements(showtext boolean, OUT userid oid, OUT dbid oid, OUT toplevel boolean, OUT queryid bigint, OUT query text, OUT plans bigint, OUT total_plan_time double precision, OUT min_plan_time double precision, OUT max_plan_time double precision, OUT mean_plan_time double precision, OUT stddev_plan_time double precision, OUT calls bigint, OUT total_exec_time double precision, OUT min_exec_time double precision, OUT max_exec_time double precision, OUT mean_exec_time double precision, OUT stddev_exec_time double precision, OUT rows bigint, OUT shared_blks_hit bigint, OUT shared_blks_read bigint, OUT shared_blks_dirtied bigint, OUT shared_blks_written bigint, OUT local_blks_hit bigint, OUT local_blks_read bigint, OUT local_blks_dirtied bigint, OUT local_blks_written bigint, OUT temp_blks_read bigint, OUT temp_blks_written bigint, OUT shared_blk_read_time double precision, OUT shared_blk_write_time double precision, OUT local_blk_read_time double precision, OUT local_blk_write_time double precision, OUT temp_blk_read_time double precision, OUT temp_blk_write_time double precision, OUT wal_records bigint, OUT wal_fpi bigint, OUT wal_bytes numeric, OUT jit_functions bigint, OUT jit_generation_time double precision, OUT jit_inlining_count bigint, OUT jit_inlining_time double precision, OUT jit_optimization_count bigint, OUT jit_optimization_time double precision, OUT jit_emission_count bigint, OUT jit_emission_time double precision, OUT jit_deform_count bigint, OUT jit_deform_time double precision, OUT stats_since timestamp with time zone, OUT minmax_stats_since timestamp with time zone) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pg_stat_statements_info(OUT dealloc bigint, OUT stats_reset timestamp with time zone); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pg_stat_statements_info(OUT dealloc bigint, OUT stats_reset timestamp with time zone) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pg_stat_statements_reset(userid oid, dbid oid, queryid bigint, minmax_only boolean); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pg_stat_statements_reset(userid oid, dbid oid, queryid bigint, minmax_only boolean) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pgp_armor_headers(text, OUT key text, OUT value text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_armor_headers(text, OUT key text, OUT value text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.pgp_armor_headers(text, OUT key text, OUT value text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pgp_key_id(bytea); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_key_id(bytea) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.pgp_key_id(bytea) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pgp_pub_decrypt(bytea, bytea); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt(bytea, bytea) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt(bytea, bytea) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pgp_pub_decrypt(bytea, bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt(bytea, bytea, text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt(bytea, bytea, text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pgp_pub_decrypt(bytea, bytea, text, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt(bytea, bytea, text, text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt(bytea, bytea, text, text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pgp_pub_decrypt_bytea(bytea, bytea); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt_bytea(bytea, bytea) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt_bytea(bytea, bytea) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pgp_pub_decrypt_bytea(bytea, bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt_bytea(bytea, bytea, text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt_bytea(bytea, bytea, text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pgp_pub_decrypt_bytea(bytea, bytea, text, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt_bytea(bytea, bytea, text, text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt_bytea(bytea, bytea, text, text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pgp_pub_encrypt(text, bytea); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_pub_encrypt(text, bytea) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.pgp_pub_encrypt(text, bytea) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pgp_pub_encrypt(text, bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_pub_encrypt(text, bytea, text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.pgp_pub_encrypt(text, bytea, text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pgp_pub_encrypt_bytea(bytea, bytea); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_pub_encrypt_bytea(bytea, bytea) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.pgp_pub_encrypt_bytea(bytea, bytea) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pgp_pub_encrypt_bytea(bytea, bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_pub_encrypt_bytea(bytea, bytea, text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.pgp_pub_encrypt_bytea(bytea, bytea, text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pgp_sym_decrypt(bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_sym_decrypt(bytea, text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.pgp_sym_decrypt(bytea, text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pgp_sym_decrypt(bytea, text, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_sym_decrypt(bytea, text, text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.pgp_sym_decrypt(bytea, text, text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pgp_sym_decrypt_bytea(bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_sym_decrypt_bytea(bytea, text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.pgp_sym_decrypt_bytea(bytea, text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pgp_sym_decrypt_bytea(bytea, text, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_sym_decrypt_bytea(bytea, text, text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.pgp_sym_decrypt_bytea(bytea, text, text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pgp_sym_encrypt(text, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_sym_encrypt(text, text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.pgp_sym_encrypt(text, text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pgp_sym_encrypt(text, text, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_sym_encrypt(text, text, text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.pgp_sym_encrypt(text, text, text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pgp_sym_encrypt_bytea(bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_sym_encrypt_bytea(bytea, text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.pgp_sym_encrypt_bytea(bytea, text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pgp_sym_encrypt_bytea(bytea, text, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_sym_encrypt_bytea(bytea, text, text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.pgp_sym_encrypt_bytea(bytea, text, text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pgrst_ddl_watch(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgrst_ddl_watch() TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pgrst_drop_watch(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgrst_drop_watch() TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION set_graphql_placeholder(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.set_graphql_placeholder() TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION uuid_generate_v1(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.uuid_generate_v1() TO dashboard_user;
GRANT ALL ON FUNCTION extensions.uuid_generate_v1() TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION uuid_generate_v1mc(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.uuid_generate_v1mc() TO dashboard_user;
GRANT ALL ON FUNCTION extensions.uuid_generate_v1mc() TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION uuid_generate_v3(namespace uuid, name text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.uuid_generate_v3(namespace uuid, name text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.uuid_generate_v3(namespace uuid, name text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION uuid_generate_v4(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.uuid_generate_v4() TO dashboard_user;
GRANT ALL ON FUNCTION extensions.uuid_generate_v4() TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION uuid_generate_v5(namespace uuid, name text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.uuid_generate_v5(namespace uuid, name text) TO dashboard_user;
GRANT ALL ON FUNCTION extensions.uuid_generate_v5(namespace uuid, name text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION uuid_nil(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.uuid_nil() TO dashboard_user;
GRANT ALL ON FUNCTION extensions.uuid_nil() TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION uuid_ns_dns(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.uuid_ns_dns() TO dashboard_user;
GRANT ALL ON FUNCTION extensions.uuid_ns_dns() TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION uuid_ns_oid(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.uuid_ns_oid() TO dashboard_user;
GRANT ALL ON FUNCTION extensions.uuid_ns_oid() TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION uuid_ns_url(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.uuid_ns_url() TO dashboard_user;
GRANT ALL ON FUNCTION extensions.uuid_ns_url() TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION uuid_ns_x500(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.uuid_ns_x500() TO dashboard_user;
GRANT ALL ON FUNCTION extensions.uuid_ns_x500() TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION graphql("operationName" text, query text, variables jsonb, extensions jsonb); Type: ACL; Schema: graphql_public; Owner: supabase_admin
--

GRANT ALL ON FUNCTION graphql_public.graphql("operationName" text, query text, variables jsonb, extensions jsonb) TO postgres;
GRANT ALL ON FUNCTION graphql_public.graphql("operationName" text, query text, variables jsonb, extensions jsonb) TO anon;
GRANT ALL ON FUNCTION graphql_public.graphql("operationName" text, query text, variables jsonb, extensions jsonb) TO authenticated;
GRANT ALL ON FUNCTION graphql_public.graphql("operationName" text, query text, variables jsonb, extensions jsonb) TO service_role;


--
-- Name: FUNCTION http_get(url text, params jsonb, headers jsonb, timeout_milliseconds integer); Type: ACL; Schema: net; Owner: supabase_admin
--

REVOKE ALL ON FUNCTION net.http_get(url text, params jsonb, headers jsonb, timeout_milliseconds integer) FROM PUBLIC;
GRANT ALL ON FUNCTION net.http_get(url text, params jsonb, headers jsonb, timeout_milliseconds integer) TO supabase_functions_admin;
GRANT ALL ON FUNCTION net.http_get(url text, params jsonb, headers jsonb, timeout_milliseconds integer) TO postgres;
GRANT ALL ON FUNCTION net.http_get(url text, params jsonb, headers jsonb, timeout_milliseconds integer) TO anon;
GRANT ALL ON FUNCTION net.http_get(url text, params jsonb, headers jsonb, timeout_milliseconds integer) TO authenticated;
GRANT ALL ON FUNCTION net.http_get(url text, params jsonb, headers jsonb, timeout_milliseconds integer) TO service_role;


--
-- Name: FUNCTION http_post(url text, body jsonb, params jsonb, headers jsonb, timeout_milliseconds integer); Type: ACL; Schema: net; Owner: supabase_admin
--

REVOKE ALL ON FUNCTION net.http_post(url text, body jsonb, params jsonb, headers jsonb, timeout_milliseconds integer) FROM PUBLIC;
GRANT ALL ON FUNCTION net.http_post(url text, body jsonb, params jsonb, headers jsonb, timeout_milliseconds integer) TO supabase_functions_admin;
GRANT ALL ON FUNCTION net.http_post(url text, body jsonb, params jsonb, headers jsonb, timeout_milliseconds integer) TO postgres;
GRANT ALL ON FUNCTION net.http_post(url text, body jsonb, params jsonb, headers jsonb, timeout_milliseconds integer) TO anon;
GRANT ALL ON FUNCTION net.http_post(url text, body jsonb, params jsonb, headers jsonb, timeout_milliseconds integer) TO authenticated;
GRANT ALL ON FUNCTION net.http_post(url text, body jsonb, params jsonb, headers jsonb, timeout_milliseconds integer) TO service_role;


--
-- Name: FUNCTION get_auth(p_usename text); Type: ACL; Schema: pgbouncer; Owner: supabase_admin
--

REVOKE ALL ON FUNCTION pgbouncer.get_auth(p_usename text) FROM PUBLIC;
GRANT ALL ON FUNCTION pgbouncer.get_auth(p_usename text) TO pgbouncer;
GRANT ALL ON FUNCTION pgbouncer.get_auth(p_usename text) TO postgres;


--
-- Name: FUNCTION aggregate_adv_params(p_date_from date, p_date_to date); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.aggregate_adv_params(p_date_from date, p_date_to date) TO anon;
GRANT ALL ON FUNCTION public.aggregate_adv_params(p_date_from date, p_date_to date) TO authenticated;
GRANT ALL ON FUNCTION public.aggregate_adv_params(p_date_from date, p_date_to date) TO service_role;


--
-- Name: FUNCTION update_updated_at_column(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.update_updated_at_column() TO anon;
GRANT ALL ON FUNCTION public.update_updated_at_column() TO authenticated;
GRANT ALL ON FUNCTION public.update_updated_at_column() TO service_role;


--
-- Name: FUNCTION apply_rls(wal jsonb, max_record_bytes integer); Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON FUNCTION realtime.apply_rls(wal jsonb, max_record_bytes integer) TO postgres;
GRANT ALL ON FUNCTION realtime.apply_rls(wal jsonb, max_record_bytes integer) TO dashboard_user;
GRANT ALL ON FUNCTION realtime.apply_rls(wal jsonb, max_record_bytes integer) TO anon;
GRANT ALL ON FUNCTION realtime.apply_rls(wal jsonb, max_record_bytes integer) TO authenticated;
GRANT ALL ON FUNCTION realtime.apply_rls(wal jsonb, max_record_bytes integer) TO service_role;
GRANT ALL ON FUNCTION realtime.apply_rls(wal jsonb, max_record_bytes integer) TO supabase_realtime_admin;


--
-- Name: FUNCTION broadcast_changes(topic_name text, event_name text, operation text, table_name text, table_schema text, new record, old record, level text); Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON FUNCTION realtime.broadcast_changes(topic_name text, event_name text, operation text, table_name text, table_schema text, new record, old record, level text) TO postgres;
GRANT ALL ON FUNCTION realtime.broadcast_changes(topic_name text, event_name text, operation text, table_name text, table_schema text, new record, old record, level text) TO dashboard_user;


--
-- Name: FUNCTION build_prepared_statement_sql(prepared_statement_name text, entity regclass, columns realtime.wal_column[]); Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON FUNCTION realtime.build_prepared_statement_sql(prepared_statement_name text, entity regclass, columns realtime.wal_column[]) TO postgres;
GRANT ALL ON FUNCTION realtime.build_prepared_statement_sql(prepared_statement_name text, entity regclass, columns realtime.wal_column[]) TO dashboard_user;
GRANT ALL ON FUNCTION realtime.build_prepared_statement_sql(prepared_statement_name text, entity regclass, columns realtime.wal_column[]) TO anon;
GRANT ALL ON FUNCTION realtime.build_prepared_statement_sql(prepared_statement_name text, entity regclass, columns realtime.wal_column[]) TO authenticated;
GRANT ALL ON FUNCTION realtime.build_prepared_statement_sql(prepared_statement_name text, entity regclass, columns realtime.wal_column[]) TO service_role;
GRANT ALL ON FUNCTION realtime.build_prepared_statement_sql(prepared_statement_name text, entity regclass, columns realtime.wal_column[]) TO supabase_realtime_admin;


--
-- Name: FUNCTION "cast"(val text, type_ regtype); Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON FUNCTION realtime."cast"(val text, type_ regtype) TO postgres;
GRANT ALL ON FUNCTION realtime."cast"(val text, type_ regtype) TO dashboard_user;
GRANT ALL ON FUNCTION realtime."cast"(val text, type_ regtype) TO anon;
GRANT ALL ON FUNCTION realtime."cast"(val text, type_ regtype) TO authenticated;
GRANT ALL ON FUNCTION realtime."cast"(val text, type_ regtype) TO service_role;
GRANT ALL ON FUNCTION realtime."cast"(val text, type_ regtype) TO supabase_realtime_admin;


--
-- Name: FUNCTION check_equality_op(op realtime.equality_op, type_ regtype, val_1 text, val_2 text); Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON FUNCTION realtime.check_equality_op(op realtime.equality_op, type_ regtype, val_1 text, val_2 text) TO postgres;
GRANT ALL ON FUNCTION realtime.check_equality_op(op realtime.equality_op, type_ regtype, val_1 text, val_2 text) TO dashboard_user;
GRANT ALL ON FUNCTION realtime.check_equality_op(op realtime.equality_op, type_ regtype, val_1 text, val_2 text) TO anon;
GRANT ALL ON FUNCTION realtime.check_equality_op(op realtime.equality_op, type_ regtype, val_1 text, val_2 text) TO authenticated;
GRANT ALL ON FUNCTION realtime.check_equality_op(op realtime.equality_op, type_ regtype, val_1 text, val_2 text) TO service_role;
GRANT ALL ON FUNCTION realtime.check_equality_op(op realtime.equality_op, type_ regtype, val_1 text, val_2 text) TO supabase_realtime_admin;


--
-- Name: FUNCTION is_visible_through_filters(columns realtime.wal_column[], filters realtime.user_defined_filter[]); Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON FUNCTION realtime.is_visible_through_filters(columns realtime.wal_column[], filters realtime.user_defined_filter[]) TO postgres;
GRANT ALL ON FUNCTION realtime.is_visible_through_filters(columns realtime.wal_column[], filters realtime.user_defined_filter[]) TO dashboard_user;
GRANT ALL ON FUNCTION realtime.is_visible_through_filters(columns realtime.wal_column[], filters realtime.user_defined_filter[]) TO anon;
GRANT ALL ON FUNCTION realtime.is_visible_through_filters(columns realtime.wal_column[], filters realtime.user_defined_filter[]) TO authenticated;
GRANT ALL ON FUNCTION realtime.is_visible_through_filters(columns realtime.wal_column[], filters realtime.user_defined_filter[]) TO service_role;
GRANT ALL ON FUNCTION realtime.is_visible_through_filters(columns realtime.wal_column[], filters realtime.user_defined_filter[]) TO supabase_realtime_admin;


--
-- Name: FUNCTION list_changes(publication name, slot_name name, max_changes integer, max_record_bytes integer); Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON FUNCTION realtime.list_changes(publication name, slot_name name, max_changes integer, max_record_bytes integer) TO postgres;
GRANT ALL ON FUNCTION realtime.list_changes(publication name, slot_name name, max_changes integer, max_record_bytes integer) TO dashboard_user;
GRANT ALL ON FUNCTION realtime.list_changes(publication name, slot_name name, max_changes integer, max_record_bytes integer) TO anon;
GRANT ALL ON FUNCTION realtime.list_changes(publication name, slot_name name, max_changes integer, max_record_bytes integer) TO authenticated;
GRANT ALL ON FUNCTION realtime.list_changes(publication name, slot_name name, max_changes integer, max_record_bytes integer) TO service_role;
GRANT ALL ON FUNCTION realtime.list_changes(publication name, slot_name name, max_changes integer, max_record_bytes integer) TO supabase_realtime_admin;


--
-- Name: FUNCTION quote_wal2json(entity regclass); Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON FUNCTION realtime.quote_wal2json(entity regclass) TO postgres;
GRANT ALL ON FUNCTION realtime.quote_wal2json(entity regclass) TO dashboard_user;
GRANT ALL ON FUNCTION realtime.quote_wal2json(entity regclass) TO anon;
GRANT ALL ON FUNCTION realtime.quote_wal2json(entity regclass) TO authenticated;
GRANT ALL ON FUNCTION realtime.quote_wal2json(entity regclass) TO service_role;
GRANT ALL ON FUNCTION realtime.quote_wal2json(entity regclass) TO supabase_realtime_admin;


--
-- Name: FUNCTION send(payload jsonb, event text, topic text, private boolean); Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON FUNCTION realtime.send(payload jsonb, event text, topic text, private boolean) TO postgres;
GRANT ALL ON FUNCTION realtime.send(payload jsonb, event text, topic text, private boolean) TO dashboard_user;


--
-- Name: FUNCTION subscription_check_filters(); Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON FUNCTION realtime.subscription_check_filters() TO postgres;
GRANT ALL ON FUNCTION realtime.subscription_check_filters() TO dashboard_user;
GRANT ALL ON FUNCTION realtime.subscription_check_filters() TO anon;
GRANT ALL ON FUNCTION realtime.subscription_check_filters() TO authenticated;
GRANT ALL ON FUNCTION realtime.subscription_check_filters() TO service_role;
GRANT ALL ON FUNCTION realtime.subscription_check_filters() TO supabase_realtime_admin;


--
-- Name: FUNCTION to_regrole(role_name text); Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON FUNCTION realtime.to_regrole(role_name text) TO postgres;
GRANT ALL ON FUNCTION realtime.to_regrole(role_name text) TO dashboard_user;
GRANT ALL ON FUNCTION realtime.to_regrole(role_name text) TO anon;
GRANT ALL ON FUNCTION realtime.to_regrole(role_name text) TO authenticated;
GRANT ALL ON FUNCTION realtime.to_regrole(role_name text) TO service_role;
GRANT ALL ON FUNCTION realtime.to_regrole(role_name text) TO supabase_realtime_admin;


--
-- Name: FUNCTION topic(); Type: ACL; Schema: realtime; Owner: supabase_realtime_admin
--

GRANT ALL ON FUNCTION realtime.topic() TO postgres;
GRANT ALL ON FUNCTION realtime.topic() TO dashboard_user;


--
-- Name: FUNCTION http_request(); Type: ACL; Schema: supabase_functions; Owner: supabase_functions_admin
--

REVOKE ALL ON FUNCTION supabase_functions.http_request() FROM PUBLIC;
GRANT ALL ON FUNCTION supabase_functions.http_request() TO postgres;
GRANT ALL ON FUNCTION supabase_functions.http_request() TO anon;
GRANT ALL ON FUNCTION supabase_functions.http_request() TO authenticated;
GRANT ALL ON FUNCTION supabase_functions.http_request() TO service_role;


--
-- Name: FUNCTION _crypto_aead_det_decrypt(message bytea, additional bytea, key_id bigint, context bytea, nonce bytea); Type: ACL; Schema: vault; Owner: supabase_admin
--

GRANT ALL ON FUNCTION vault._crypto_aead_det_decrypt(message bytea, additional bytea, key_id bigint, context bytea, nonce bytea) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION vault._crypto_aead_det_decrypt(message bytea, additional bytea, key_id bigint, context bytea, nonce bytea) TO service_role;


--
-- Name: FUNCTION create_secret(new_secret text, new_name text, new_description text, new_key_id uuid); Type: ACL; Schema: vault; Owner: supabase_admin
--

GRANT ALL ON FUNCTION vault.create_secret(new_secret text, new_name text, new_description text, new_key_id uuid) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION vault.create_secret(new_secret text, new_name text, new_description text, new_key_id uuid) TO service_role;


--
-- Name: FUNCTION update_secret(secret_id uuid, new_secret text, new_name text, new_description text, new_key_id uuid); Type: ACL; Schema: vault; Owner: supabase_admin
--

GRANT ALL ON FUNCTION vault.update_secret(secret_id uuid, new_secret text, new_name text, new_description text, new_key_id uuid) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION vault.update_secret(secret_id uuid, new_secret text, new_name text, new_description text, new_key_id uuid) TO service_role;


--
-- Name: TABLE audit_log_entries; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT ALL ON TABLE auth.audit_log_entries TO dashboard_user;
GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,MAINTAIN,UPDATE ON TABLE auth.audit_log_entries TO postgres;
GRANT SELECT ON TABLE auth.audit_log_entries TO postgres WITH GRANT OPTION;


--
-- Name: TABLE flow_state; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,MAINTAIN,UPDATE ON TABLE auth.flow_state TO postgres;
GRANT SELECT ON TABLE auth.flow_state TO postgres WITH GRANT OPTION;
GRANT ALL ON TABLE auth.flow_state TO dashboard_user;


--
-- Name: TABLE identities; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,MAINTAIN,UPDATE ON TABLE auth.identities TO postgres;
GRANT SELECT ON TABLE auth.identities TO postgres WITH GRANT OPTION;
GRANT ALL ON TABLE auth.identities TO dashboard_user;


--
-- Name: TABLE instances; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT ALL ON TABLE auth.instances TO dashboard_user;
GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,MAINTAIN,UPDATE ON TABLE auth.instances TO postgres;
GRANT SELECT ON TABLE auth.instances TO postgres WITH GRANT OPTION;


--
-- Name: TABLE mfa_amr_claims; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,MAINTAIN,UPDATE ON TABLE auth.mfa_amr_claims TO postgres;
GRANT SELECT ON TABLE auth.mfa_amr_claims TO postgres WITH GRANT OPTION;
GRANT ALL ON TABLE auth.mfa_amr_claims TO dashboard_user;


--
-- Name: TABLE mfa_challenges; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,MAINTAIN,UPDATE ON TABLE auth.mfa_challenges TO postgres;
GRANT SELECT ON TABLE auth.mfa_challenges TO postgres WITH GRANT OPTION;
GRANT ALL ON TABLE auth.mfa_challenges TO dashboard_user;


--
-- Name: TABLE mfa_factors; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,MAINTAIN,UPDATE ON TABLE auth.mfa_factors TO postgres;
GRANT SELECT ON TABLE auth.mfa_factors TO postgres WITH GRANT OPTION;
GRANT ALL ON TABLE auth.mfa_factors TO dashboard_user;


--
-- Name: TABLE oauth_authorizations; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT ALL ON TABLE auth.oauth_authorizations TO postgres;
GRANT ALL ON TABLE auth.oauth_authorizations TO dashboard_user;


--
-- Name: TABLE oauth_clients; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT ALL ON TABLE auth.oauth_clients TO postgres;
GRANT ALL ON TABLE auth.oauth_clients TO dashboard_user;


--
-- Name: TABLE oauth_consents; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT ALL ON TABLE auth.oauth_consents TO postgres;
GRANT ALL ON TABLE auth.oauth_consents TO dashboard_user;


--
-- Name: TABLE one_time_tokens; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,MAINTAIN,UPDATE ON TABLE auth.one_time_tokens TO postgres;
GRANT SELECT ON TABLE auth.one_time_tokens TO postgres WITH GRANT OPTION;
GRANT ALL ON TABLE auth.one_time_tokens TO dashboard_user;


--
-- Name: TABLE refresh_tokens; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT ALL ON TABLE auth.refresh_tokens TO dashboard_user;
GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,MAINTAIN,UPDATE ON TABLE auth.refresh_tokens TO postgres;
GRANT SELECT ON TABLE auth.refresh_tokens TO postgres WITH GRANT OPTION;


--
-- Name: SEQUENCE refresh_tokens_id_seq; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT ALL ON SEQUENCE auth.refresh_tokens_id_seq TO dashboard_user;
GRANT ALL ON SEQUENCE auth.refresh_tokens_id_seq TO postgres;


--
-- Name: TABLE saml_providers; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,MAINTAIN,UPDATE ON TABLE auth.saml_providers TO postgres;
GRANT SELECT ON TABLE auth.saml_providers TO postgres WITH GRANT OPTION;
GRANT ALL ON TABLE auth.saml_providers TO dashboard_user;


--
-- Name: TABLE saml_relay_states; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,MAINTAIN,UPDATE ON TABLE auth.saml_relay_states TO postgres;
GRANT SELECT ON TABLE auth.saml_relay_states TO postgres WITH GRANT OPTION;
GRANT ALL ON TABLE auth.saml_relay_states TO dashboard_user;


--
-- Name: TABLE schema_migrations; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT SELECT ON TABLE auth.schema_migrations TO postgres WITH GRANT OPTION;


--
-- Name: TABLE sessions; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,MAINTAIN,UPDATE ON TABLE auth.sessions TO postgres;
GRANT SELECT ON TABLE auth.sessions TO postgres WITH GRANT OPTION;
GRANT ALL ON TABLE auth.sessions TO dashboard_user;


--
-- Name: TABLE sso_domains; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,MAINTAIN,UPDATE ON TABLE auth.sso_domains TO postgres;
GRANT SELECT ON TABLE auth.sso_domains TO postgres WITH GRANT OPTION;
GRANT ALL ON TABLE auth.sso_domains TO dashboard_user;


--
-- Name: TABLE sso_providers; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,MAINTAIN,UPDATE ON TABLE auth.sso_providers TO postgres;
GRANT SELECT ON TABLE auth.sso_providers TO postgres WITH GRANT OPTION;
GRANT ALL ON TABLE auth.sso_providers TO dashboard_user;


--
-- Name: TABLE users; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT ALL ON TABLE auth.users TO dashboard_user;
GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,MAINTAIN,UPDATE ON TABLE auth.users TO postgres;
GRANT SELECT ON TABLE auth.users TO postgres WITH GRANT OPTION;


--
-- Name: TABLE pg_stat_statements; Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON TABLE extensions.pg_stat_statements TO postgres WITH GRANT OPTION;


--
-- Name: TABLE pg_stat_statements_info; Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON TABLE extensions.pg_stat_statements_info TO postgres WITH GRANT OPTION;


--
-- Name: TABLE adv_campaign_daily_stats; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.adv_campaign_daily_stats TO anon;
GRANT ALL ON TABLE public.adv_campaign_daily_stats TO authenticated;
GRANT ALL ON TABLE public.adv_campaign_daily_stats TO service_role;


--
-- Name: TABLE adv_params; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.adv_params TO anon;
GRANT ALL ON TABLE public.adv_params TO authenticated;
GRANT ALL ON TABLE public.adv_params TO service_role;


--
-- Name: TABLE cost_price; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.cost_price TO anon;
GRANT ALL ON TABLE public.cost_price TO authenticated;
GRANT ALL ON TABLE public.cost_price TO service_role;


--
-- Name: TABLE cr_daily_stats; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.cr_daily_stats TO anon;
GRANT ALL ON TABLE public.cr_daily_stats TO authenticated;
GRANT ALL ON TABLE public.cr_daily_stats TO service_role;


--
-- Name: TABLE paid_acceptance; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.paid_acceptance TO anon;
GRANT ALL ON TABLE public.paid_acceptance TO authenticated;
GRANT ALL ON TABLE public.paid_acceptance TO service_role;


--
-- Name: TABLE paid_storage; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.paid_storage TO anon;
GRANT ALL ON TABLE public.paid_storage TO authenticated;
GRANT ALL ON TABLE public.paid_storage TO service_role;


--
-- Name: TABLE product_sizes; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.product_sizes TO anon;
GRANT ALL ON TABLE public.product_sizes TO authenticated;
GRANT ALL ON TABLE public.product_sizes TO service_role;


--
-- Name: SEQUENCE product_sizes_serial_id_seq; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON SEQUENCE public.product_sizes_serial_id_seq TO anon;
GRANT ALL ON SEQUENCE public.product_sizes_serial_id_seq TO authenticated;
GRANT ALL ON SEQUENCE public.product_sizes_serial_id_seq TO service_role;


--
-- Name: TABLE products; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.products TO anon;
GRANT ALL ON TABLE public.products TO authenticated;
GRANT ALL ON TABLE public.products TO service_role;


--
-- Name: SEQUENCE products_serial_id_seq; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON SEQUENCE public.products_serial_id_seq TO anon;
GRANT ALL ON SEQUENCE public.products_serial_id_seq TO authenticated;
GRANT ALL ON SEQUENCE public.products_serial_id_seq TO service_role;


--
-- Name: SEQUENCE week_reports_serial_id_seq; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON SEQUENCE public.week_reports_serial_id_seq TO anon;
GRANT ALL ON SEQUENCE public.week_reports_serial_id_seq TO authenticated;
GRANT ALL ON SEQUENCE public.week_reports_serial_id_seq TO service_role;


--
-- Name: TABLE week_reports; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.week_reports TO anon;
GRANT ALL ON TABLE public.week_reports TO authenticated;
GRANT ALL ON TABLE public.week_reports TO service_role;


--
-- Name: TABLE week_rows; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.week_rows TO anon;
GRANT ALL ON TABLE public.week_rows TO authenticated;
GRANT ALL ON TABLE public.week_rows TO service_role;


--
-- Name: TABLE week_stats; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.week_stats TO anon;
GRANT ALL ON TABLE public.week_stats TO authenticated;
GRANT ALL ON TABLE public.week_stats TO service_role;


--
-- Name: TABLE messages; Type: ACL; Schema: realtime; Owner: supabase_realtime_admin
--

GRANT ALL ON TABLE realtime.messages TO postgres;
GRANT ALL ON TABLE realtime.messages TO dashboard_user;
GRANT SELECT,INSERT,UPDATE ON TABLE realtime.messages TO anon;
GRANT SELECT,INSERT,UPDATE ON TABLE realtime.messages TO authenticated;
GRANT SELECT,INSERT,UPDATE ON TABLE realtime.messages TO service_role;


--
-- Name: TABLE messages_2025_11_12; Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON TABLE realtime.messages_2025_11_12 TO postgres;
GRANT ALL ON TABLE realtime.messages_2025_11_12 TO dashboard_user;


--
-- Name: TABLE messages_2025_11_13; Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON TABLE realtime.messages_2025_11_13 TO postgres;
GRANT ALL ON TABLE realtime.messages_2025_11_13 TO dashboard_user;


--
-- Name: TABLE messages_2025_11_14; Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON TABLE realtime.messages_2025_11_14 TO postgres;
GRANT ALL ON TABLE realtime.messages_2025_11_14 TO dashboard_user;


--
-- Name: TABLE messages_2025_11_15; Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON TABLE realtime.messages_2025_11_15 TO postgres;
GRANT ALL ON TABLE realtime.messages_2025_11_15 TO dashboard_user;


--
-- Name: TABLE messages_2025_11_16; Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON TABLE realtime.messages_2025_11_16 TO postgres;
GRANT ALL ON TABLE realtime.messages_2025_11_16 TO dashboard_user;


--
-- Name: TABLE schema_migrations; Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON TABLE realtime.schema_migrations TO postgres;
GRANT ALL ON TABLE realtime.schema_migrations TO dashboard_user;
GRANT SELECT ON TABLE realtime.schema_migrations TO anon;
GRANT SELECT ON TABLE realtime.schema_migrations TO authenticated;
GRANT SELECT ON TABLE realtime.schema_migrations TO service_role;
GRANT ALL ON TABLE realtime.schema_migrations TO supabase_realtime_admin;


--
-- Name: TABLE subscription; Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON TABLE realtime.subscription TO postgres;
GRANT ALL ON TABLE realtime.subscription TO dashboard_user;
GRANT SELECT ON TABLE realtime.subscription TO anon;
GRANT SELECT ON TABLE realtime.subscription TO authenticated;
GRANT SELECT ON TABLE realtime.subscription TO service_role;
GRANT ALL ON TABLE realtime.subscription TO supabase_realtime_admin;


--
-- Name: SEQUENCE subscription_id_seq; Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON SEQUENCE realtime.subscription_id_seq TO postgres;
GRANT ALL ON SEQUENCE realtime.subscription_id_seq TO dashboard_user;
GRANT USAGE ON SEQUENCE realtime.subscription_id_seq TO anon;
GRANT USAGE ON SEQUENCE realtime.subscription_id_seq TO authenticated;
GRANT USAGE ON SEQUENCE realtime.subscription_id_seq TO service_role;
GRANT ALL ON SEQUENCE realtime.subscription_id_seq TO supabase_realtime_admin;


--
-- Name: TABLE buckets; Type: ACL; Schema: storage; Owner: supabase_storage_admin
--

GRANT ALL ON TABLE storage.buckets TO anon;
GRANT ALL ON TABLE storage.buckets TO authenticated;
GRANT ALL ON TABLE storage.buckets TO service_role;
GRANT ALL ON TABLE storage.buckets TO postgres WITH GRANT OPTION;


--
-- Name: TABLE buckets_analytics; Type: ACL; Schema: storage; Owner: supabase_storage_admin
--

GRANT ALL ON TABLE storage.buckets_analytics TO service_role;
GRANT ALL ON TABLE storage.buckets_analytics TO authenticated;
GRANT ALL ON TABLE storage.buckets_analytics TO anon;


--
-- Name: TABLE iceberg_namespaces; Type: ACL; Schema: storage; Owner: supabase_storage_admin
--

GRANT ALL ON TABLE storage.iceberg_namespaces TO service_role;
GRANT SELECT ON TABLE storage.iceberg_namespaces TO authenticated;
GRANT SELECT ON TABLE storage.iceberg_namespaces TO anon;


--
-- Name: TABLE iceberg_tables; Type: ACL; Schema: storage; Owner: supabase_storage_admin
--

GRANT ALL ON TABLE storage.iceberg_tables TO service_role;
GRANT SELECT ON TABLE storage.iceberg_tables TO authenticated;
GRANT SELECT ON TABLE storage.iceberg_tables TO anon;


--
-- Name: TABLE objects; Type: ACL; Schema: storage; Owner: supabase_storage_admin
--

GRANT ALL ON TABLE storage.objects TO anon;
GRANT ALL ON TABLE storage.objects TO authenticated;
GRANT ALL ON TABLE storage.objects TO service_role;
GRANT ALL ON TABLE storage.objects TO postgres WITH GRANT OPTION;


--
-- Name: TABLE prefixes; Type: ACL; Schema: storage; Owner: supabase_storage_admin
--

GRANT ALL ON TABLE storage.prefixes TO service_role;
GRANT ALL ON TABLE storage.prefixes TO authenticated;
GRANT ALL ON TABLE storage.prefixes TO anon;


--
-- Name: TABLE s3_multipart_uploads; Type: ACL; Schema: storage; Owner: supabase_storage_admin
--

GRANT ALL ON TABLE storage.s3_multipart_uploads TO service_role;
GRANT SELECT ON TABLE storage.s3_multipart_uploads TO authenticated;
GRANT SELECT ON TABLE storage.s3_multipart_uploads TO anon;


--
-- Name: TABLE s3_multipart_uploads_parts; Type: ACL; Schema: storage; Owner: supabase_storage_admin
--

GRANT ALL ON TABLE storage.s3_multipart_uploads_parts TO service_role;
GRANT SELECT ON TABLE storage.s3_multipart_uploads_parts TO authenticated;
GRANT SELECT ON TABLE storage.s3_multipart_uploads_parts TO anon;


--
-- Name: TABLE hooks; Type: ACL; Schema: supabase_functions; Owner: supabase_functions_admin
--

GRANT ALL ON TABLE supabase_functions.hooks TO postgres;
GRANT ALL ON TABLE supabase_functions.hooks TO anon;
GRANT ALL ON TABLE supabase_functions.hooks TO authenticated;
GRANT ALL ON TABLE supabase_functions.hooks TO service_role;


--
-- Name: SEQUENCE hooks_id_seq; Type: ACL; Schema: supabase_functions; Owner: supabase_functions_admin
--

GRANT ALL ON SEQUENCE supabase_functions.hooks_id_seq TO postgres;
GRANT ALL ON SEQUENCE supabase_functions.hooks_id_seq TO anon;
GRANT ALL ON SEQUENCE supabase_functions.hooks_id_seq TO authenticated;
GRANT ALL ON SEQUENCE supabase_functions.hooks_id_seq TO service_role;


--
-- Name: TABLE migrations; Type: ACL; Schema: supabase_functions; Owner: supabase_functions_admin
--

GRANT ALL ON TABLE supabase_functions.migrations TO postgres;
GRANT ALL ON TABLE supabase_functions.migrations TO anon;
GRANT ALL ON TABLE supabase_functions.migrations TO authenticated;
GRANT ALL ON TABLE supabase_functions.migrations TO service_role;


--
-- Name: TABLE secrets; Type: ACL; Schema: vault; Owner: supabase_admin
--

GRANT SELECT,REFERENCES,DELETE,TRUNCATE ON TABLE vault.secrets TO postgres WITH GRANT OPTION;
GRANT SELECT,DELETE ON TABLE vault.secrets TO service_role;


--
-- Name: TABLE decrypted_secrets; Type: ACL; Schema: vault; Owner: supabase_admin
--

GRANT SELECT,REFERENCES,DELETE,TRUNCATE ON TABLE vault.decrypted_secrets TO postgres WITH GRANT OPTION;
GRANT SELECT,DELETE ON TABLE vault.decrypted_secrets TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: auth; Owner: supabase_auth_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_auth_admin IN SCHEMA auth GRANT ALL ON SEQUENCES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_auth_admin IN SCHEMA auth GRANT ALL ON SEQUENCES TO dashboard_user;


--
-- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: auth; Owner: supabase_auth_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_auth_admin IN SCHEMA auth GRANT ALL ON FUNCTIONS TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_auth_admin IN SCHEMA auth GRANT ALL ON FUNCTIONS TO dashboard_user;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: auth; Owner: supabase_auth_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_auth_admin IN SCHEMA auth GRANT ALL ON TABLES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_auth_admin IN SCHEMA auth GRANT ALL ON TABLES TO dashboard_user;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: extensions; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA extensions GRANT ALL ON SEQUENCES TO postgres WITH GRANT OPTION;


--
-- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: extensions; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA extensions GRANT ALL ON FUNCTIONS TO postgres WITH GRANT OPTION;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: extensions; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA extensions GRANT ALL ON TABLES TO postgres WITH GRANT OPTION;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: graphql; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT ALL ON SEQUENCES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT ALL ON SEQUENCES TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT ALL ON SEQUENCES TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT ALL ON SEQUENCES TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: graphql; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT ALL ON FUNCTIONS TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT ALL ON FUNCTIONS TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT ALL ON FUNCTIONS TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT ALL ON FUNCTIONS TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: graphql; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT ALL ON TABLES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT ALL ON TABLES TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT ALL ON TABLES TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT ALL ON TABLES TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: graphql_public; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT ALL ON SEQUENCES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT ALL ON SEQUENCES TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT ALL ON SEQUENCES TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT ALL ON SEQUENCES TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: graphql_public; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT ALL ON FUNCTIONS TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT ALL ON FUNCTIONS TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT ALL ON FUNCTIONS TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT ALL ON FUNCTIONS TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: graphql_public; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT ALL ON TABLES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT ALL ON TABLES TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT ALL ON TABLES TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT ALL ON TABLES TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: public; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON SEQUENCES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON SEQUENCES TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON SEQUENCES TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON SEQUENCES TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: public; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT ALL ON SEQUENCES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT ALL ON SEQUENCES TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT ALL ON SEQUENCES TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT ALL ON SEQUENCES TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: public; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON FUNCTIONS TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON FUNCTIONS TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON FUNCTIONS TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON FUNCTIONS TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: public; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT ALL ON FUNCTIONS TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT ALL ON FUNCTIONS TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT ALL ON FUNCTIONS TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT ALL ON FUNCTIONS TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: public; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON TABLES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON TABLES TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON TABLES TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON TABLES TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: public; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT ALL ON TABLES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT ALL ON TABLES TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT ALL ON TABLES TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT ALL ON TABLES TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: realtime; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA realtime GRANT ALL ON SEQUENCES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA realtime GRANT ALL ON SEQUENCES TO dashboard_user;


--
-- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: realtime; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA realtime GRANT ALL ON FUNCTIONS TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA realtime GRANT ALL ON FUNCTIONS TO dashboard_user;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: realtime; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA realtime GRANT ALL ON TABLES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA realtime GRANT ALL ON TABLES TO dashboard_user;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: storage; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT ALL ON SEQUENCES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT ALL ON SEQUENCES TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT ALL ON SEQUENCES TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT ALL ON SEQUENCES TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: storage; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT ALL ON FUNCTIONS TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT ALL ON FUNCTIONS TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT ALL ON FUNCTIONS TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT ALL ON FUNCTIONS TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: storage; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT ALL ON TABLES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT ALL ON TABLES TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT ALL ON TABLES TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT ALL ON TABLES TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: supabase_functions; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA supabase_functions GRANT ALL ON SEQUENCES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA supabase_functions GRANT ALL ON SEQUENCES TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA supabase_functions GRANT ALL ON SEQUENCES TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA supabase_functions GRANT ALL ON SEQUENCES TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: supabase_functions; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA supabase_functions GRANT ALL ON FUNCTIONS TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA supabase_functions GRANT ALL ON FUNCTIONS TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA supabase_functions GRANT ALL ON FUNCTIONS TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA supabase_functions GRANT ALL ON FUNCTIONS TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: supabase_functions; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA supabase_functions GRANT ALL ON TABLES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA supabase_functions GRANT ALL ON TABLES TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA supabase_functions GRANT ALL ON TABLES TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA supabase_functions GRANT ALL ON TABLES TO service_role;


--
-- Name: issue_graphql_placeholder; Type: EVENT TRIGGER; Schema: -; Owner: supabase_admin
--

CREATE EVENT TRIGGER issue_graphql_placeholder ON sql_drop
         WHEN TAG IN ('DROP EXTENSION')
   EXECUTE FUNCTION extensions.set_graphql_placeholder();


ALTER EVENT TRIGGER issue_graphql_placeholder OWNER TO supabase_admin;

--
-- Name: issue_pg_cron_access; Type: EVENT TRIGGER; Schema: -; Owner: supabase_admin
--

CREATE EVENT TRIGGER issue_pg_cron_access ON ddl_command_end
         WHEN TAG IN ('CREATE EXTENSION')
   EXECUTE FUNCTION extensions.grant_pg_cron_access();


ALTER EVENT TRIGGER issue_pg_cron_access OWNER TO supabase_admin;

--
-- Name: issue_pg_graphql_access; Type: EVENT TRIGGER; Schema: -; Owner: supabase_admin
--

CREATE EVENT TRIGGER issue_pg_graphql_access ON ddl_command_end
         WHEN TAG IN ('CREATE FUNCTION')
   EXECUTE FUNCTION extensions.grant_pg_graphql_access();


ALTER EVENT TRIGGER issue_pg_graphql_access OWNER TO supabase_admin;

--
-- Name: issue_pg_net_access; Type: EVENT TRIGGER; Schema: -; Owner: supabase_admin
--

CREATE EVENT TRIGGER issue_pg_net_access ON ddl_command_end
         WHEN TAG IN ('CREATE EXTENSION')
   EXECUTE FUNCTION extensions.grant_pg_net_access();


ALTER EVENT TRIGGER issue_pg_net_access OWNER TO supabase_admin;

--
-- Name: pgrst_ddl_watch; Type: EVENT TRIGGER; Schema: -; Owner: supabase_admin
--

CREATE EVENT TRIGGER pgrst_ddl_watch ON ddl_command_end
   EXECUTE FUNCTION extensions.pgrst_ddl_watch();


ALTER EVENT TRIGGER pgrst_ddl_watch OWNER TO supabase_admin;

--
-- Name: pgrst_drop_watch; Type: EVENT TRIGGER; Schema: -; Owner: supabase_admin
--

CREATE EVENT TRIGGER pgrst_drop_watch ON sql_drop
   EXECUTE FUNCTION extensions.pgrst_drop_watch();


ALTER EVENT TRIGGER pgrst_drop_watch OWNER TO supabase_admin;

--
-- PostgreSQL database dump complete
--

\unrestrict lTrOKOohzCCgrT7KFFnbhmeB9Bgr2voqKN95473HdPmSTcGZAQUedZgb4ALjRVq

