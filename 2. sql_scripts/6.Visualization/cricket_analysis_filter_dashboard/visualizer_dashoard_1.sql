--total matches on the ith day of the month
select d.day, count(match_id) as no_of_matches
from cricket.consumption.match_fact m
join cricket.consumption.date_dim d
on m.date_id=d.date_id
where d.day=:cricket_year_filter
group by d.day;