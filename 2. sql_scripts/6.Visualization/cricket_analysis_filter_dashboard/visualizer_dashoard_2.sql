--total runs scored in a particular day
select 
    d.day,
    sum(total.total_score) as total_score_particular_day
from
    cricket.consumption.date_dim d
join 
(select
    match_id,
    date_id,
    total_score_by_team_a+total_score_by_team_b as total_score
from cricket.consumption.match_fact) total
on total.date_id=d.date_id
where day=:cricket_year_filter
group by 
    d.day;