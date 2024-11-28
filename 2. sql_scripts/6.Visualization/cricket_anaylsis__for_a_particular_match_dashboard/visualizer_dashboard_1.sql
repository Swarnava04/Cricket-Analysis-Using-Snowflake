select * from cricket.consumption.match_fact where match_id=4690;

select * from cricket.consumption.team_dim where team_id in (203, 247);
select * from cricket.consumption.match_type_dim where match_type_id=1;
select * from cricket.consumption.venue_dim where venue_id=3;


--runs scored by team_a
select  total_score_by_team_a from cricket.consumption.match_fact where match_id=4690; --run this one 

select total_score_by_team_b from cricket.consumption.match_fact where match_id=4690;