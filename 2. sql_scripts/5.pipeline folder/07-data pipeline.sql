--step 1 - creating streams to track down any latest insert into match_raw_tbl
create or replace stream cricket.raw.for_match_stream on table cricket.raw.match_raw_tbl append_only=true;
create or replace stream cricket.raw.for_player_stream on table cricket.raw.match_raw_tbl append_only=true;
create or replace stream cricket.raw.for_delivery_stream on table cricket.raw.match_raw_tbl append_only=true;

--step 2 - creating a task that runs every 10 mins and loads the data from the stage into raw layer(json format)
create or replace task cricket.raw.load_json_to_raw
    warehouse='COMPUTE_WH'
    schedule='10 minute'
        as
    copy into cricket.raw.match_raw_tbl from
    (
        select 
            t.$1:info::variant as info,
            t.$1:innings::array as innings,
            metadata$filename,
            metadata$file_row_number,
            metadata$file_content_key,        
            metadata$file_last_modified          
        from @cricket.land.my_stg/cricket/json(file_format=>'cricket.land.my_json_format') t
    )  
    on_error=continue;

--step 3 - creating a child task to read from the above created streams and load into the clean layer
create or replace task cricket.raw.load_to_clean_match
    warehouse='COMPUTE_WH'
    after cricket.raw.load_json_to_raw
    when system$stream_has_data('cricket.raw.for_match_stream')
    as 
    insert into cricket.clean.match_detail_clean
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
    from cricket.raw.for_match_stream    
;


--step 4 - creating a child task after populating match_clean table
create or replace task cricket.raw.load_to_clean_player
    warehouse='COMPUTE_WH'
    after cricket.raw.load_to_clean_match
    when system$stream_has_data('cricket.raw.for_player_stream')
    as 
    insert into cricket.clean.player_clean_tbl
    select 
        rcm.info:match_type_number::int as match_type_number,
        p.key::text as country,
        team.value as player_name,
        stg_file_name ,
        stg_file_row_number,
        stg_file_haskey,
        stg_modified_ts
    from cricket.raw.for_player_stream rcm,
    lateral flatten(input => rcm.info:match_type_number) p,
    lateral flatten(input => p.value) team
;


--step 5 - creating a child task after populating  player_clean_table 
create or replace task cricket.raw.load_to_clean_delivery
    warehouse='COMPUTE_WH'
    after cricket.raw.load_to_clean_player
    when system$stream_has_data('cricket.raw.for_delivery_stream')
    as 
    insert into cricket.clean.player_clean_tbl
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
    from cricket.raw. rcm,for_delivery_stream rcm, 
    lateral flatten(input=>raw.innings) i,
    lateral flatten(input=>i.value:overs) o,
    lateral flatten(input=>o.value:deliveries) d,
    lateral flatten(input=>d.value:extras, outer=>true) e,
    lateral flatten(input=>d.value:wickets, outer=>true) w
;

--step 6 - creating a task for populating team_dim table after load_to_clean_delivery 
create or replace task cricket.raw.load_to_team_dim
    warehouse='COMPUTE_WH'
    after cricket.raw.load_to_clean_delivery
    as
    insert into cricket.consumption.team_dim(team_name)
    select distinct team_name from 
    (select first_team as team_name from cricket.clean.match_detail_clean
    union all
    select second_team as team_name from cricket.clean.match_detail_clean)
    minus 
    select team_name from cricket.consumption.team_dim;    

--step 7 - creating a task for populating venue_dim table after load_to_clean_delivery
create or replace task cricket.raw.load_to_venue_dim
    warehouse='COMPUTE_WH'
    after cricket.raw.load_to_clean_delivery
    as
    insert into cricket.consumption.venue_dim(venue_name, city)
    with venue_cte as
    (select 
        venue,
        case when city is null then 'NA'
        else city
        end as city
    from cricket.clean.match_detail_clean)
    
    select venue as venue_name, city
    from venue_cte
    group by venue_name, city
    minus
    select venue_name, city from cricket.consumption.venue_dim;
;

--step 8 - creating a task for populating player_dim table after load_to_clean_delivery
create or replace task cricket.raw.load_to_player_dim
    warehouse='COMPUTE_WH'
    after cricket.raw.load_to_clean_delivery
    as
    insert into cricket.consumption.player_dim(team_id, player_name)
    select   b.team_id, a.player_name
    from 
        cricket.clean.player_clean_tbl a join cricket.consumption.team_dim b
    on 
        a.country=b.team_name
    group by
        b.team_id,
        a.player_name
    minus
    select team_id, player_name from cricket.consumption.player_dim;
    
--step 9 - creating a task for populating date_dim table after load_to_clean_delivery
CREATE OR REPLACE TASK cricket.raw.load_to_date_dim
    WAREHOUSE = 'COMPUTE_WH'
    AFTER cricket.raw.load_to_clean_delivery
AS
INSERT INTO cricket.consumption.date_dim
WITH RECURSIVE missing_dates AS (
    
    SELECT 
        MIN(event_date) AS new_min_date, 
        MAX(event_date) AS new_max_date
    FROM cricket.clean.match_detail_clean

    UNION ALL

    
    SELECT 
        CASE
            WHEN new_min_date < (SELECT MIN(full_dt) FROM cricket.consumption.date_dim)
            THEN DATEADD(day, -1, new_min_date) -- Backfill missing dates before the range
            ELSE new_min_date -- Keep constant for dates after the range
        END AS new_min_date,
        CASE
            WHEN new_max_date > (SELECT MAX(full_dt) FROM cricket.consumption.date_dim)
            THEN DATEADD(day, 1, new_max_date) -- Add missing dates after the range
            ELSE new_max_date -- Keep constant for dates before the range
        END AS new_max_date
    FROM missing_dates
    WHERE 
        new_min_date < (SELECT MIN(full_dt) FROM cricket.consumption.date_dim)
        OR 
        new_max_date > (SELECT MAX(full_dt) FROM cricket.consumption.date_dim)
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY new_min_date) 
    + COALESCE((SELECT MAX(date_id) FROM cricket.consumption.date_dim), 0) AS date_id, -- Generate date_id
    new_min_date AS full_dt, -- Full date
    EXTRACT(day FROM new_min_date) AS day, -- Day of the month
    EXTRACT(month FROM new_min_date) AS month, -- Month
    EXTRACT(year FROM new_min_date) AS year, -- Year
    EXTRACT(quarter FROM new_min_date) AS quarter, -- Quarter
    DAYOFWEEKISO(new_min_date) AS dayofweek, -- Day of the week (ISO)
    EXTRACT(day FROM new_min_date) AS dayofmonth, -- Day of the month
    DAYOFYEAR(new_min_date) AS dayofyear, -- Day of the year
    DAYNAME(new_min_date) AS dateofweekname, -- Name of the day
    CASE WHEN DAYNAME(new_min_date) IN ('Sat', 'Sun') THEN 1 ELSE 0 END AS isweekend -- Is weekend
FROM missing_dates;


--step 10 - creating a task for populating match_type table after load_to_clean_delivery

create or replace task cricket.raw.load_to_match_type    
    warehouse='COMPUTE_WH'
    after cricket.raw.load_to_clean_delivery
    as 
    insert into cricket.consumption.match_type_dim(match_type)
    select match_type from
    (select 
        case
            when match_type is null then 'NA'
            else match_type
        end as match_type
     from cricket.clean.match_detail_clean
     group by match_type)
     minus
     select match_type
     from cricket.consumption.match_type_dim;

-- step 11 - creating a task for populating match_fact table after all the paralled tasks
create or replace task cricket.raw.load_to_match_fact
    warehouse='COMPUTE_WH'
    after cricket.raw.load_to_team_dim, cricket.raw.load_to_venue_dim, cricket.raw.load_to_player_dim, cricket.raw.load_to_date_dim, cricket.raw.load_to_match_type 
    as
    insert into cricket.consumption.match_fact
    select a.* from (
    select 
    m.match_type_number as match_id,
    dd.date_id as date_id,
    0 as referee_id,
    ftd.team_id as first_team_id,
    std.team_id as second_team_id,
    mtd.match_type_id as match_type_id,
    vd.venue_id as venue_id,
    50 as total_overs,
    6 as balls_per_overs,
    max(case when d.team_name = m.first_team then  d.over else 0 end ) as OVERS_PLAYED_BY_TEAM_A,
    sum(case when d.team_name = m.first_team then  1 else 0 end ) as balls_PLAYED_BY_TEAM_A,
    sum(case when d.team_name = m.first_team then  d.extras else 0 end ) as extra_balls_PLAYED_BY_TEAM_A,
    sum(case when d.team_name = m.first_team then  d.extra_runs else 0 end ) as extra_runs_scored_BY_TEAM_A,
    0 fours_by_team_a,
    0 sixes_by_team_a,
    (sum(case when d.team_name = m.first_team then  d.runs else 0 end ) + sum(case when d.team_name = m.first_team then  d.extra_runs else 0 end ) ) as total_runs_scored_BY_TEAM_A,
    sum(case when d.team_name = m.first_team and player_out is not null then  1 else 0 end ) as wicket_lost_by_team_a,    
    
    max(case when d.team_name = m.second_team then  d.over else 0 end ) as OVERS_PLAYED_BY_TEAM_B,
    sum(case when d.team_name = m.second_team then  1 else 0 end ) as balls_PLAYED_BY_TEAM_B,
    sum(case when d.team_name = m.second_team then  d.extras else 0 end ) as extra_balls_PLAYED_BY_TEAM_B,
    sum(case when d.team_name = m.second_team then  d.extra_runs else 0 end ) as extra_runs_scored_BY_TEAM_B,
    0 fours_by_team_b,
    0 sixes_by_team_b,
    (sum(case when d.team_name = m.second_team then  d.runs else 0 end ) + sum(case when d.team_name = m.second_team then  d.extra_runs else 0 end ) ) as total_runs_scored_BY_TEAM_B,
    sum(case when d.team_name = m.second_team and player_out is not null then  1 else 0 end ) as wicket_lost_by_team_b,
    tw.team_id as toss_winner_team_id,
    m.toss_decision as toss_decision,
    m.match_result as matach_result,
    mw.team_id as winner_team_id
     
from 
    cricket.clean.match_detail_clean m
    join date_dim dd on m.event_date = dd.full_dt
    join team_dim ftd on m.first_team = ftd.team_name 
    join team_dim std on m.second_team = std.team_name 
    join match_type_dim mtd on m.match_type = mtd.match_type
    join venue_dim vd on m.venue = vd.venue_name and m.city = vd.city
    join cricket.clean.delivery_clean_tbl d  on d.match_type_number = m.match_type_number 
    join team_dim tw on m.toss_winner = tw.team_name 
    join team_dim mw on m.winner= mw.team_name 
    --where m.match_type_number = 4686
    group by
        m.match_type_number,
        date_id,
        referee_id,
        first_team_id,
        second_team_id,
        match_type_id,
        venue_id,
        total_overs,
        toss_winner_team_id,
        toss_decision,
        matach_result,
        winner_team_id

    ) a
    left join cricket.consumption.match_fact b 
    on a.match_id=b.match_id
    where b.match_id is null;


-- step 12 - creating a task for populating delivery_fact table after load_to_match_fact
create or replace task cricket.raw.load_delivery_fact
    warehouse = 'COMPUTE_WH'
    after cricket.raw.load_to_match_fact
    as
    insert into cricket.consumption.delivery_fact
   select a.* from (
   
       select 
        d.match_type_number as match_id,
        td.team_id,
        bpd.player_id as bower_id, 
        spd.player_id batter_id, 
        nspd.player_id as non_stricker_id,
        d.over,
        d.runs,
        case when d.extra_runs is null then 0 else d.extra_runs end as extra_runs,
        case when d.extra_type is null then 'None' else d.extra_type end as extra_type,
        case when d.player_out is null then 'None' else d.player_out end as player_out,
        case when d.player_out_kind is null then 'None' else d.player_out_kind end as player_out_kind
    from 
        cricket.clean.delivery_clean_tbl d
        join team_dim td on d.team_name = td.team_name
        join player_dim bpd on d.bowler = bpd.player_name
        join player_dim spd on d.batter = spd.player_name
        join player_dim nspd on d.non_striker = nspd.player_name
   )a
   left join cricket.consumption.delivery_fact b
   on a.match_id=b.match_id
   where b.match_id is null;

