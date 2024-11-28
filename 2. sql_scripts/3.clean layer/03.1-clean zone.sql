use role accountadmin;
use warehouse compute_wh;
use schema cricket.clean;

--step: select data from info variant in raw table in separate columns

select   
    info:match_type_number::int as match_type_number,
    info:match_type:: text as match_type,
    info:season:: text as season,
    info:team_type:: text as team_type,
    info:overs:: text as overs,
    info:city:: text as city,
    info:venue:: text as venue
from cricket.raw.match_raw_tbl
where info:match_type_number::int is not null;

select 
    info:match_type_number::int as match_type_number,
    info:event.name::text as event_name,
    case
        when
            info:event.match_number::text is not null then info:event.match_number::text
        when 
            info:event.stage::text is not null then info:event.stage::text
        else
            'NA'
    end as match_stage,
    info:dates[0]::date as event_date,
    date_part('year', info:dates[0]::date) as event_year,
    date_part('month', info:dates[0]::date) as event_month,
    date_part('day', info:dates[0]::date) as event_day,
    info:match_type::text as team_type,
    info:season::text as season,
    info:team_type::text as team_type,
    info:overs::text as overs,
    info:city::text as city,
    info:venue::text as venue,
    info:gender::text as gender,
    info:teams[0]::text as first_team,
    info:teams[1]::text as second_team,
    case
        when 
            info:outcome.winner is not null then 'Result Declared'
        when 
            info:outcome.result='tie' then 'Tie'
        when
            info:outcome.result='no result' then 'No Result'
        else
            info:outcome.result::text
    end
    as match_result,
    case 
        when 
            info:outcome.winner is not null then info:outcome.winner::text
        else
            'NA'
    end
    as winner,
    info:toss.winner::text as toss_winner,
    initcap(info:toss.decision::text) as toss_decision
from cricket.raw.match_raw_tbl
where info:match_type_number::int is not null;

create or replace transient table cricket.clean.match_detail_clean as
select 
    info:match_type_number::int as match_type_number,
    info:event.name::text as event_name,
    case
        when
            info:event.match_number::text is not null then info:event.match_number::text
        when 
            info:event.stage::text is not null then info:event.stage::text
        else
            'NA'
    end as match_stage,
    info:dates[0]::date as event_date,
    date_part('year', info:dates[0]::date) as event_year,
    date_part('month', info:dates[0]::date) as event_month,
    date_part('day', info:dates[0]::date) as event_day,
    info:match_type::text as match_type,
    info:season::text as season,
    info:team_type::text as team_type,
    info:overs::text as overs,
    info:city::text as city,
    info:venue::text as venue,
    info:gender::text as gender,
    info:teams[0]::text as first_team,
    info:teams[1]::text as second_team,
    case
        when 
            info:outcome.winner is not null then 'Result Declared'
        when 
            info:outcome.result='tie' then 'Tie'
        when
            info:outcome.result='no result' then 'No Result'
        else
            info:outcome.result::text
    end
    as match_result,
    case 
        when 
            info:outcome.winner is not null then info:outcome.winner::text
        else
            'NA'
    end
    as winner,
    info:toss.winner::text as toss_winner,
    initcap(info:toss.decision::text) as toss_decision,
    stg_file_name,
    stg_file_row_number,
    stg_file_haskey
from cricket.raw.match_raw_tbl
where info:match_type_number::int is not null;

select * from cricket.clean.match_detail_clean;

desc table cricket.clean.match_detail_clean;

--setting match_type_number column as the primary key
alter table cricket.clean.match_detail_clean
modify column match_type_number set primary key;

ALTER TABLE cricket.clean.match_detail_clean
ADD CONSTRAINT unique_match_type_number UNIQUE (match_type_number);

select get_ddl('table','cricket.clean.match_detail_clean');
