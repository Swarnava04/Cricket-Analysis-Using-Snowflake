use warehouse compute_wh;
use role accountadmin;
use schema cricket.clean;

select 
    raw.info:match_type_number::int as match_type_number,
    i.value:team::text as team_name,
    o.value:over+1::int as over,
    d.value:bowler::text as bowler,
    d.value:batter::text as batter,
    d.value:non_striker::text as non_striker,
    d.value:runs.batter::text as runs,
    d.value:runs.extras::text as extras,
    d.value:runs.total::text as total,
    e.key::text as extra_type,
    e.value::int as extra_runs,
    w.value:player_out::text as player_out,
    w.value:kind::text as player_out_kind,
    w.value:fielders::variant as player_out_fielders 
from cricket.raw.match_raw_tbl raw,
lateral flatten(input=>raw.innings) i,
lateral flatten(input=>i.value:overs) o,
lateral flatten(input=>o.value:deliveries) d,
lateral flatten(input=>d.value:extras, outer=>true) e,
lateral flatten(input=>d.value:wickets, outer=>true) w
where raw.info:match_type_number::int is not null;



create or replace transient table cricket.clean.delivery_clean_tbl as
select 
    raw.info:match_type_number::int as match_type_number,
    i.value:team::text as team_name,
    o.value:over+1::int as over,
    d.value:bowler::text as bowler,
    d.value:batter::text as batter,
    d.value:non_striker::text as non_striker,
    d.value:runs.batter::text as runs,
    d.value:runs.extras::text as extras,
    d.value:runs.total::text as total,
    e.key::text as extra_type,
    e.value::int as extra_runs,
    w.value:player_out::text as player_out,
    w.value:kind::text as player_out_kind,
    w.value:fielders::variant as player_out_fielders,
    raw.stg_file_name ,
    raw.stg_file_row_number,
    raw.stg_file_haskey,
    raw.stg_modified_ts
from cricket.raw.match_raw_tbl raw,
lateral flatten(input=>raw.innings) i,
lateral flatten(input=>i.value:overs) o,
lateral flatten(input=>o.value:deliveries) d,
lateral flatten(input=>d.value:extras, outer=>true) e,
lateral flatten(input=>d.value:wickets, outer=>true) w
where raw.info:match_type_number::int is not null;



desc table cricket.clean.delivery_clean_tbl;

alter table cricket.clean.delivery_clean_tbl
modify column match_type_number set not null;

alter table cricket.clean.delivery_clean_tbl
modify column team_name set not null;

alter table cricket.clean.delivery_clean_tbl
modify column over set not null;

alter table cricket.clean.delivery_clean_tbl
modify column bowler set not null;

alter table cricket.clean.delivery_clean_tbl
modify column non_striker set not null;

alter table cricket.clean.delivery_clean_tbl
add constraint fk_deliver_match_id
foreign key(match_type_number) references cricket.clean.match_detail_clean(match_type_number);