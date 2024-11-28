--total wickets in a particular day
select 
    d.day,
    sum(total.total_wickets) as total_wickets_particular_day
from
    cricket.consumption.date_dim d
join 
(select
    match_id,
    date_id,
    wicket_lost_by_team_a + wicket_lost_by_team_b as total_wickets
from cricket.consumption.match_fact) total
on total.date_id=d.date_id
where day=:cricket_year_filter
group by 
    d.day;