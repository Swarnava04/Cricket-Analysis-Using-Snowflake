--Sri Lanka(team b) runs per deliver
create or replace sequence cricket.consumption.sequence_2 start =1 increment=1;


select t.team_name, sequence_2.nextval as  Delivery, d.runs as runs,   
from cricket.consumption.delivery_fact d
join cricket.consumption.team_dim t
on d.team_id=t.team_id 
where d.match_id=4690
and t.team_id=247;
