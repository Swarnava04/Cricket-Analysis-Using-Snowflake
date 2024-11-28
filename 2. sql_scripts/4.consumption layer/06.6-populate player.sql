select * from cricket.clean.player_clean_tbl;

select country, player_name from cricket.clean.player_clean_tbl group by country, player_name
order by country;

select* from cricket.consumption.team_dim;

select b.team_id, a.country, a.player_name
from 
    cricket.clean.player_clean_tbl a join cricket.consumption.team_dim b
on 
    a.country=b.team_name
group by
    a.country,
    b.team_id,
    a.player_name
;


insert into  cricket.consumption.player_dim(team_id, player_name)
select   b.team_id, a.player_name
from 
    cricket.clean.player_clean_tbl a join cricket.consumption.team_dim b
on 
    a.country=b.team_name
group by
    b.team_id,
    a.player_name
;




















