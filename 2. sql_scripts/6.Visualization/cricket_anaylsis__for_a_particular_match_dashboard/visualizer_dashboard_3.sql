--matches played in each venue
select * from cricket.consumption.venue_dim;

select 
    v.venue_name,
    count(*) as no_of_matches_played
from cricket.consumption.match_fact m
join cricket.consumption.venue_dim v
on v.venue_id=m.venue_id
group by v.venue_name;


select * from cricket.consumption.match_fact;