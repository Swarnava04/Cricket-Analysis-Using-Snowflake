select distinct d.month from 
cricket.consumption.match_fact m
join cricket.consumption.date_dim d
on m.date_id = d.date_id
order by d.month;


select distinct d.day from 
cricket.consumption.match_fact m
join cricket.consumption.date_dim d
on m.date_id = d.date_id
order by d.day;