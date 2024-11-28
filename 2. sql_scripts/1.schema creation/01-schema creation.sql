use role accountadmin;
use warehouse compute_wh;

create database if not exists cricket;
create or replace schema cricket.land;
create or replace schema cricket.raw;
create or replace schema cricket.clean;
create or replace schema cricket.consumption;


use schema cricket.land;

create or replace file format my_json_format
    type=json
    null_if=('\\n', 'null', '')
    strip_outer_array= true
    comment='JSON file format with outer strip array flag true';

create or replace stage cricket.land.my_stg;

list @cricket.land.my_stg;

select * from @my_stg/cricket/json/1384392.json(file_format=>'cricket.land.my_json_format');
select * from @my_stg/cricket/json/1383534.json(file_format=>'cricket.land.my_json_format');

select t.*
from @cricket.land.my_stg/cricket/json(file_format=>'cricket.land.my_json_format') t;

select 
    t.$1:info::variant as info,
    t.$1:innings::array as innings,
    metadata$filename as file_name,
    metadata$filename as file_name,
    metadata$file_row_number int,
    metadata$file_content_key text,
    metadata$file_last_modified stg_modified_ts 
from  @cricket.land.my_stg/cricket/json(file_format=>'cricket.land.my_json_format') t;
