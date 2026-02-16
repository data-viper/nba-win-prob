-- Row count + date range
SELECT COUNT(*) AS rows, MIN(game_date_utc) AS min_dt, MAX(game_date_utc) AS max_dt
FROM games;

-- Completed vs not completed
SELECT
  CASE WHEN home_score IS NOT NULL AND away_score IS NOT NULL THEN 'completed'
       ELSE 'not_completed' END AS game_state,
  COUNT(*) AS cnt
FROM games
GROUP BY 1
ORDER BY 2 DESC;

-- Home win rate overall (completed only)
SELECT
  AVG(CASE WHEN home_score > away_score THEN 1 ELSE 0 END) AS home_win_rate,
  COUNT(*) AS completed_games
FROM games
WHERE home_score IS NOT NULL AND away_score IS NOT NULL;


-- Team games
with team_games as (
    select home_team_name as team,
    1 as home_game,
    0 as away_game,
    case when home_score > away_score then 1 else 0 end as win
    From games
    where home_score is not null and away_score is not null

    union all

    select away_team_name as team,
    0 as home_game,
    1 as away_game,
    case when away_score > home_score then 1 else 0 end as win
    from games
    where home_score is not null and away_score is not null 
)
select 
team,
count(*) as games_played,
sum(home_game) as home_games,
sum(away_game) as away_games,
sum (win) as games_won
from team_games
group by team
order by games_played desc, team;
