use role accountadmin;
use schema cricket.raw;

create or replace transient table cricket.raw.match_raw_tbl(
    info variant not null,
    innings array not null,
    stg_file_name text not null,
    stg_file_row_number int not null,
    stg_file_haskey text not null,
    stg_modified_ts timestamp not null
)
comment = 'This is raw table to store all the json data file with root elements extracted'
;

copy into cricket.raw.match_raw_tbl from 
(
select 
    t.$1:info::variant as info,
    t.$1:innings::array as innings,
    metadata$filename,
    metadata$file_row_number,
    metadata$file_content_key,        
    metadata$file_last_modified
from @cricket.land.my_stg/cricket/json t
)
file_format=(format_name=cricket.land.my_json_format)
on_error=continue;


select count(*) from cricket.raw.match_raw_tbl;
select * from cricket.raw.match_raw_tbl;