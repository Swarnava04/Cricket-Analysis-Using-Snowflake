select * from cricket.clean.match_detail_clean
where match_type_number=4690;

select 
    team_name,
    batter,
    sum(runs)
from 
    Cricket.clean.DELIVERY_CLEAN_TBL
where match_type_number=4690
group by team_name, batter
order by 1,2,3 desc;

select 
    team_name,
    sum(runs)+sum(extra_runs)
from cricket.clean.delivery_clean_tbl
where match_type_number=4690
group by team_name
order by 1,2 desc;