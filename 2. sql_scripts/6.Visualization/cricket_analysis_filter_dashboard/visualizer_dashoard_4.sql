--final display dashboard
select 
    match_id, 
    d.day,
    team_a.team_name as TEAM_A,
    team_b.team_name as TEAM_B,
    total_score_by_team_a::string || '/' || wicket_lost_by_team_a::string || '(' || overs_played_by_team_a::string || ')' AS team_a_score,
    total_score_by_team_b::string || '/' || wicket_lost_by_team_b::string || '(' || overs_played_by_team_b::string || ')' AS team_b_score,
    winner.team_name
from match_fact m
join date_dim d on m.date_id=d.date_id
join team_dim team_a on team_a.team_id=m.team_a_id
join team_dim team_b on team_b.team_id=m.team_b_id
join team_dim winner on winner.team_id=m.winner_team_id,
where d.day=:cricket_year_filter;