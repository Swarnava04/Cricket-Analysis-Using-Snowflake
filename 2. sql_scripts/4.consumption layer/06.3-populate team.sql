select * from cricket.clean.match_detail_clean;
select distinct team_name from(
    select first_team as team_name from cricket.clean.match_detail_clean
    union all
    select second_team as team_name from cricket.clean.match_detail_clean
);

insert into cricket.consumption.team_dim(team_name)
select distinct team_name from(
    select first_team as team_name from cricket.clean.match_detail_clean
    union all
    select second_team as team_name from cricket.clean.match_detail_clean
);

select * from team_dim;