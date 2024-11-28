--India(team a) runs per delivery
CREATE or REPLACE SEQUENCE my_sequence START = 1 INCREMENT = 1;
select my_sequence.nextval;

select t.team_name,my_sequence.nextval as delivery,  d.runs as runs,   
from cricket.consumption.delivery_fact d
join cricket.consumption.team_dim t
on d.team_id=t.team_id 
where d.match_id=4690
AND 
t.team_id=203;
