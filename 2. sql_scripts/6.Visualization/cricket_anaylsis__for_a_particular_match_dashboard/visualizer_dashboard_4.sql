select * from match_detail_clean;

select * from cricket.consumption.delivery_fact where match_id=4690;

--runs score by each team across the overs
select
    t.team_name,
    d.over,
    sum(runs)
from cricket.consumption.delivery_fact d
join cricket.consumption.team_dim t
on d.team_id=t.team_id
where d.match_id=4690
group by 
    t.team_name,
    d.over
order by
    t.team_name,
    d.over asc;
    
