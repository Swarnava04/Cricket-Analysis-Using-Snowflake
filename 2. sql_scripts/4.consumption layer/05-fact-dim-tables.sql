--date dimension
create or replace table cricket.consumption.date_dim(
    date_id int primary key autoincrement,
    full_dt date,
    day int,
    month int,
    year int,
    quarter int,
    dayofweek int,
    dayofmonth int,
    dayofyear int,
    dateofweekname varchar(3),
    isweekend boolean
);

--referee dimension
create or replace table referee_dim(
    referee_id int primary key autoincrement,
    referee_name text not null,
    referee_type text not null
);

--team dimension
create or replace table team_dim(
    team_id int primary key autoincrement,
    team_name text not null
);


--player dimension
create or replace table player_dim(
    player_id int primary key autoincrement,
    team_id int not null,
    player_name text not null
);

--team to player relationships
alter table cricket.consumption.player_dim
add constraint fk_team_player_id
foreign key(team_id)
references cricket.consumption.team_dim(team_id);

select get_ddl('table', 'cricket.consumption.player_dim');

--venue dimension
create or replace table venue_dim(
    venue_id int primary key autoincrement,
    venue_name text not null,
    city text not null,
    state text,
    country text,
    continent text,
    end_Names text,
    capacity number,
    pitch text,
    flood_light boolean,
    established_dt date,
    playing_area text,
    other_sports text,
    curator text,
    lattitude number(10, 6),
    longitude number(10, 6)
);

--match_type dimension
create or replace table match_type_dim(
    match_type_id int primary key autoincrement,
    match_type text not null
);

--match fact table
CREATE OR REPLACE TABLE match_fact (
    match_id INT PRIMARY KEY,
    date_id INT NOT NULL,
    referee_id INT NOT NULL,
    team_a_id INT NOT NULL,
    team_b_id INT NOT NULL,
    match_type_id INT NOT NULL,
    venue_id INT NOT NULL,
    total_overs NUMBER(3),
    balls_per_over NUMBER(1),

    overs_played_by_team_a NUMBER(3),
    bowls_played_by_team_a NUMBER(4), -- increased precision
    extra_bowls_played_by_team_a NUMBER(4), -- increased precision
    extra_runs_scored_by_team_a NUMBER(4), -- increased precision
    fours_by_team_a NUMBER(4), -- increased precision
    sixes_by_team_a NUMBER(4), -- increased precision
    total_score_by_team_a NUMBER(5), -- increased precision for total score
    wicket_lost_by_team_a NUMBER(2),

    overs_played_by_team_b NUMBER(3),
    bowls_played_by_team_b NUMBER(4), -- increased precision
    extra_bowls_played_by_team_b NUMBER(4), -- increased precision
    extra_runs_scored_by_team_b NUMBER(4), -- increased precision
    fours_by_team_b NUMBER(4), -- increased precision
    sixes_by_team_b NUMBER(4), -- increased precision
    total_score_by_team_b NUMBER(5), -- increased precision for total score
    wicket_lost_by_team_b NUMBER(2),

    toss_winner_team_id INT NOT NULL,
    toss_decision TEXT NOT NULL,
    match_result TEXT NOT NULL,
    winner_team_id INT NOT NULL,

    CONSTRAINT fk_date FOREIGN KEY(date_id) REFERENCES date_dim(date_id),
    CONSTRAINT fk_referee FOREIGN KEY(referee_id) REFERENCES referee_dim(referee_id),
    CONSTRAINT fk_team1 FOREIGN KEY(team_a_id) REFERENCES team_dim(team_id),
    CONSTRAINT fk_team2 FOREIGN KEY(team_b_id) REFERENCES team_dim(team_id),
    CONSTRAINT fk_match_type FOREIGN KEY(match_type_id) REFERENCES match_type_dim(match_type_id),
    CONSTRAINT fk_venue FOREIGN KEY(venue_id) REFERENCES venue_dim(venue_id),
    CONSTRAINT fk_toss_winner_team FOREIGN KEY(toss_winner_team_id) REFERENCES team_dim(team_id),
    CONSTRAINT fk_winner_team FOREIGN KEY(winner_team_id) REFERENCES team_dim(team_id)
);


--delivery fact table
CREATE or replace TABLE delivery_fact (
    match_id INT ,
    team_id INT,
    bowler_id INT,
    batter_id INT,
    non_striker_id INT,
    over INT,
    runs INT,
    extra_runs INT,
    extra_type VARCHAR(255),
    player_out VARCHAR(255),
    player_out_kind VARCHAR(255),

    CONSTRAINT fk_del_match_id FOREIGN KEY (match_id) REFERENCES match_fact (match_id),
    CONSTRAINT fk_del_team FOREIGN KEY (team_id) REFERENCES team_dim (team_id),
    CONSTRAINT fk_bowler FOREIGN KEY (bowler_id) REFERENCES player_dim (player_id),
    CONSTRAINT fk_batter FOREIGN KEY (batter_id) REFERENCES player_dim (player_id),
    CONSTRAINT fk_stricker FOREIGN KEY (non_striker_id) REFERENCES player_dim (player_id)
);



