use role accountadmin;
use warehouse compute_wh;
use schema cricket.clean;

--version 1
select 
    raw.info:match_type_number::int as match_type_number,
    raw.info:players,
    raw.info:teams
from cricket.raw.match_raw_tbl raw
where raw.info:match_type_number::int is not null
and
raw.info:match_type_number::int = 4690;


--version 2
select 
  raw.info:match_type_number::int as match_type_number,
  raw.info:players,
  raw.info:teams
from cricket.raw.match_raw_tbl raw
where raw.info:match_type_number::int=4690;




--version3(flatten)
select
    raw.info:match_type_number::int as match_type_number,
    p.key::text as country
from cricket.raw.match_raw_tbl raw,
lateral flatten(input=>raw.info:players) p
where raw.info:match_type_number::int = 4690;

--version 4
select
    raw.info:match_type_number::int as match_type_number,
    p.key::text as country,
    team.value::string as player_name
from cricket.raw.match_raw_tbl raw,
lateral flatten(input=>raw.info:players) p,
lateral flatten(input=>p.value) team,
where raw.info:match_type_number::int = 4690;


--version 5
select 
    rcm.info:match_type_number::int as match_type_number, 
    p.key::text as country,
    team.value:: text as player_name,
    stg_file_name ,
    stg_file_row_number,
    stg_file_haskey,
    stg_modified_ts
from cricket.raw.match_raw_tbl rcm,
lateral flatten (input => rcm.info:players) p,
lateral flatten (input => p.value) team
where match_type_number::int is not null;

create or replace transient table cricket.clean.player_clean_tbl as
select 
    rcm.info:match_type_number::int as match_type_number, 
    p.key::text as country,
    team.value:: text as player_name,
    stg_file_name ,
    stg_file_row_number,
    stg_file_haskey,
    stg_modified_ts
from cricket.raw.match_raw_tbl rcm,
lateral flatten (input => rcm.info:players) p,
lateral flatten (input => p.value) team
where match_type_number::int is not null;


select * from player_clean_tbl;


alter table cricket.clean.player_clean_tbl
modify column match_type_number set not null;

alter table cricket.clean.player_clean_tbl
modify column country set not null;

alter table cricket.clean.player_clean_tbl
modify column player_name set not null;

alter table cricket.clean.player_clean_tbl
add constraint fk_match_id
foreign key(match_type_number)
references cricket.clean.match_detail_clean(match_type_number);

desc table cricket.clean.player_clean_tbl;
select get_ddl('table', 'cricket.clean.player_clean_tbl');


