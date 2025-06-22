SELECT COUNT(DISTINCT Player) AS total_players
FROM `basketball-sql.basketball_sql.season_stats`;

SELECT Year, 
       ROUND(AVG(PTS), 2) AS avg_points_per_player
FROM `basketball-sql.basketball_sql.season_stats`
WHERE PTS IS NOT NULL
GROUP BY Year
ORDER BY avg_points_per_player DESC
LIMIT 10;

SELECT Tm AS team,
       COUNT(*) AS appearances
FROM `basketball-sql.basketball_sql.season_stats`
GROUP BY team
ORDER BY appearances DESC
LIMIT 10;

SELECT COUNT(*) AS veteran_players
FROM (
  SELECT Player,
         COUNT(DISTINCT Year) AS active_seasons
  FROM `basketball-sql.basketball_sql.season_stats`
  GROUP BY Player
  HAVING active_seasons > 10
);

SELECT *
FROM `basketball-sql.basketball_sql.players`
LIMIT 10;

WITH first_appearance AS (
  SELECT Player, MIN(Year) AS first_year
  FROM `basketball-sql.basketball_sql.season_stats`
  GROUP BY Player
),
joined AS (
  SELECT f.Player, f.first_year, p.born
  FROM first_appearance f
  JOIN `basketball-sql.basketball_sql.players` p
    ON f.Player = p.Player
  WHERE p.born IS NOT NULL
)
SELECT ROUND(AVG(first_year - CAST(born AS INT64)), 2) AS avg_age_first_season
FROM joined;

SELECT Player, 
       Year,
       PTS,
       ROUND(AVG(PTS) OVER (
           PARTITION BY Player 
           ORDER BY Year 
           ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
       ), 2) AS rolling_avg_pts
FROM `basketball-sql.basketball_sql.season_stats`
WHERE PTS IS NOT NULL
ORDER BY Player, Year;

SELECT Year, 
       Player,
       PTS,
       RANK() OVER (
         PARTITION BY Year 
         ORDER BY PTS DESC
       ) AS season_rank
FROM `basketball-sql.basketball_sql.season_stats`
WHERE PTS IS NOT NULL
ORDER BY Year, season_rank;

SELECT Player,
       SUM(PTS) AS total_points
FROM `basketball-sql.basketball_sql.season_stats`
WHERE PTS IS NOT NULL
GROUP BY Player
ORDER BY total_points DESC
LIMIT 10;

WITH ranked AS (
  SELECT Year, 
         Player,
         PTS,
         RANK() OVER (
           PARTITION BY Year 
           ORDER BY PTS DESC
         ) AS season_rank
  FROM `basketball-sql.basketball_sql.season_stats`
  WHERE PTS IS NOT NULL
)
SELECT Player,
       COUNT(*) AS top_3_appearances
FROM ranked
WHERE season_rank <= 3
GROUP BY Player
ORDER BY top_3_appearances DESC
LIMIT 10;

SELECT Year,
       Tm AS team,
       ROUND(SUM(PTS), 2) AS total_team_points
FROM `basketball-sql.basketball_sql.season_stats`
WHERE Tm != 'TOT' AND PTS IS NOT NULL
GROUP BY Year, team
ORDER BY total_team_points DESC
LIMIT 10;

WITH decade_stats AS (
  SELECT 
    Tm AS team,
    FLOOR(Year / 10) * 10 AS decade,
    ROUND(SUM(PTS), 2) AS total_points
  FROM `basketball-sql.basketball_sql.season_stats`
  WHERE Tm != 'TOT' AND PTS IS NOT NULL
  GROUP BY team, decade
)
SELECT *
FROM decade_stats
ORDER BY decade, total_points DESC;

WITH team_totals AS (
  SELECT Year, Tm AS team, SUM(PTS) AS team_pts
  FROM `basketball-sql.basketball_sql.season_stats`
  WHERE Tm != 'TOT' AND PTS IS NOT NULL
  GROUP BY Year, team
),
top_scorers AS (
  SELECT Year, Tm AS team, Player, MAX(PTS) AS top_player_pts
  FROM `basketball-sql.basketball_sql.season_stats`
  WHERE Tm != 'TOT' AND PTS IS NOT NULL
  GROUP BY Year, team, Player
),
joined AS (
  SELECT t.Year, t.team, s.Player, s.top_player_pts, t.team_pts,
         ROUND(100 * s.top_player_pts / t.team_pts, 2) AS pct_of_team_score
  FROM team_totals t
  JOIN top_scorers s
    ON t.Year = s.Year AND t.team = s.team
)
SELECT *
FROM joined
ORDER BY pct_of_team_score DESC
LIMIT 10;

WITH team_players AS (
  SELECT Year, Tm AS team, Player, PTS,
         RANK() OVER (PARTITION BY Year, Tm ORDER BY PTS DESC) AS rank
  FROM `basketball-sql.basketball_sql.season_stats`
  WHERE Tm != 'TOT' AND PTS IS NOT NULL
),
filtered AS (
  SELECT Year, team,
         MAX(CASE WHEN rank = 1 THEN PTS END) AS top1,
         MAX(CASE WHEN rank = 2 THEN PTS END) AS top2
  FROM team_players
  WHERE rank <= 2
  GROUP BY Year, team
)
SELECT Year, team, top1, top2,
       ROUND(top1 - top2, 2) AS diff
FROM filtered
WHERE top2 IS NOT NULL
ORDER BY diff ASC
LIMIT 10;

WITH player_pts_stddev AS (
  SELECT Player,
         COUNT(*) AS seasons_played,
         ROUND(AVG(PTS), 2) AS avg_pts,
         ROUND(STDDEV(PTS), 2) AS std_dev_pts
  FROM `basketball-sql.basketball_sql.season_stats`
  WHERE PTS IS NOT NULL
  GROUP BY Player
  HAVING seasons_played >= 5
)
SELECT Player, seasons_played, avg_pts, std_dev_pts,
       ROUND(std_dev_pts / avg_pts, 2) AS consistency_ratio
FROM player_pts_stddev
ORDER BY consistency_ratio ASC
LIMIT 10;

WITH player_year_pts AS (
  SELECT Player, Year, PTS,
         LAG(PTS) OVER (PARTITION BY Player ORDER BY Year) AS prev_pts
  FROM `basketball-sql.basketball_sql.season_stats`
  WHERE PTS IS NOT NULL
),
improvements AS (
  SELECT Player, Year, PTS, prev_pts,
         PTS - prev_pts AS pts_diff
  FROM player_year_pts
  WHERE prev_pts IS NOT NULL
)
SELECT Player, Year, PTS, prev_pts, pts_diff
FROM improvements
ORDER BY pts_diff DESC
LIMIT 10;

WITH player_year_pts AS (
  SELECT Player, Year, PTS,
         LAG(PTS) OVER (PARTITION BY Player ORDER BY Year) AS prev_pts
  FROM `basketball-sql.basketball_sql.season_stats`
  WHERE PTS IS NOT NULL
),
improvements AS (
  SELECT Player, Year, PTS, prev_pts,
         PTS - prev_pts AS pts_diff
  FROM player_year_pts
  WHERE prev_pts IS NOT NULL
)
SELECT Player, Year, PTS, prev_pts, pts_diff
FROM improvements
ORDER BY pts_diff ASC
LIMIT 10;

WITH first_season AS (
  SELECT Player, MIN(Year) AS first_year
  FROM `basketball-sql.basketball_sql.season_stats`
  GROUP BY Player
),
rookie_stats AS (
  SELECT s.Player, s.Year, s.PTS
  FROM `basketball-sql.basketball_sql.season_stats` s
  JOIN first_season f
    ON s.Player = f.Player AND s.Year = f.first_year
  WHERE s.PTS IS NOT NULL
)
SELECT *
FROM rookie_stats
ORDER BY PTS DESC
LIMIT 10;

SELECT Player,
       SUM(PTS) AS total_pts,
       SUM(AST) AS total_ast,
       SUM(TRB) AS total_reb,
       SUM(PTS + AST + TRB) AS total_contribution
FROM `basketball-sql.basketball_sql.season_stats`
WHERE PTS IS NOT NULL AND AST IS NOT NULL AND TRB IS NOT NULL
GROUP BY Player
ORDER BY total_contribution DESC
LIMIT 10;

WITH player_totals AS (
  SELECT Player,
         SUM(PTS) AS total_pts,
         SUM(AST) AS total_ast,
         SUM(TRB) AS total_reb
  FROM `basketball-sql.basketball_sql.season_stats`
  WHERE PTS IS NOT NULL AND AST IS NOT NULL AND TRB IS NOT NULL
  GROUP BY Player
),
typed_players AS (
  SELECT *,
         CASE
           WHEN total_pts >= 20000 AND total_ast < 5000 AND total_reb < 5000 THEN 'Scorer'
           WHEN total_ast >= 6000 AND total_pts < 15000 AND total_reb < 5000 THEN 'Playmaker'
           WHEN total_reb >= 10000 AND total_pts < 15000 AND total_ast < 3000 THEN 'Rebounder'
           WHEN total_pts >= 15000 AND total_ast >= 4000 AND total_reb >= 5000 THEN 'All-Arounder'
           ELSE 'Role Player'
         END AS player_type
  FROM player_totals
)
SELECT player_type, COUNT(*) AS count_players
FROM typed_players
GROUP BY player_type
ORDER BY count_players DESC;

WITH player_totals AS (
  SELECT Player,
         SUM(PTS) AS total_pts,
         SUM(AST) AS total_ast,
         SUM(TRB) AS total_reb
  FROM `basketball-sql.basketball_sql.season_stats`
  WHERE PTS IS NOT NULL AND AST IS NOT NULL AND TRB IS NOT NULL
  GROUP BY Player
),
typed_players AS (
  SELECT *,
         CASE
           WHEN total_pts >= 20000 AND total_ast < 5000 AND total_reb < 5000 THEN 'Scorer'
           WHEN total_ast >= 6000 AND total_pts < 15000 AND total_reb < 5000 THEN 'Playmaker'
           WHEN total_reb >= 10000 AND total_pts < 15000 AND total_ast < 3000 THEN 'Rebounder'
           WHEN total_pts >= 15000 AND total_ast >= 4000 AND total_reb >= 5000 THEN 'All-Arounder'
           ELSE 'Role Player'
         END AS player_type
  FROM player_totals
)
SELECT Player, total_pts, total_ast, total_reb, player_type
FROM typed_players
ORDER BY player_type, total_pts DESC;