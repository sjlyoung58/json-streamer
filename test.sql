with js as (
SELECT (body_data->>'id64')::bigint as system_address,
       body_data->>'name' as system,
       round((body_data->'coords'->>'x')::numeric(10,5),2) as x,
       round((body_data->'coords'->>'y')::numeric(10,5),2) as y,
       round((body_data->'coords'->>'z')::numeric(10,5),2) as z,
       jsonb_array_length(body_data->'bodies') as bodies,
       jsonb_array_length(jsonb_path_query_array(body_data->'bodies', '$[*]?(@.type=="Star")')) as stars,
       jsonb_array_length(jsonb_path_query_array(body_data->'bodies', '$[*]?(@.type=="Planet")')) as planets,
       jsonb_path_query_array(body_data->'bodies', '$[*].subType') as type
  FROM ed_stg.stg_bubble500 bub)
select * from js;
