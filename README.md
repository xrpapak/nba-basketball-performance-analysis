# Basketball Player & Team Performance Analysis

## Project Overview
This project explores historical basketball data using only SQL to uncover insights about player performance, consistency, team scoring dynamics, and player classification. The objective is to demonstrate both advanced SQL skills and analytical thinking through meaningful patterns and data-driven storytelling.

All queries were executed in **Google BigQuery**, and the dataset includes career and seasonal stats for over 3,900 players.

---

## Dataset Tables
- `players`: Player-level information (height, weight, birth year, etc.)
- `season_stats`: Player performance per season (PTS, AST, TRB, etc.)

---

## Section 1: Basic SQL Exploration

### 1. How many unique players exist in the dataset?
```sql
SELECT COUNT(DISTINCT Player) AS total_players
FROM `basketball-sql.basketball_sql.season_stats`;
```
**Insight:** 3,921 unique players have participated in the league.

---

### 2. Top 10 scoring seasons by average points per player
```sql
SELECT Year, ROUND(AVG(PTS), 2) AS avg_points_per_player
FROM `basketball-sql.basketball_sql.season_stats`
WHERE PTS IS NOT NULL
GROUP BY Year
ORDER BY avg_points_per_player DESC
LIMIT 10;
```
**Insight:** The 1960s were a golden era for scoring, with multiple seasons averaging over 700 points per player.

---

### 3. Most frequent team appearances
```sql
SELECT Tm AS team, COUNT(*) AS appearances
FROM `basketball-sql.basketball_sql.season_stats`
GROUP BY team
ORDER BY appearances DESC
LIMIT 10;
```
**Insight:** Team "TOT" (Total) represents combined stats from players who switched teams mid-season. NYK (Knicks), BOS (Celtics), and DET (Pistons) lead in frequency.

---

### 4. How many players played more than 10 seasons?
```sql
SELECT COUNT(*) AS veteran_players
FROM (
  SELECT Player, COUNT(DISTINCT Year) AS active_seasons
  FROM `basketball-sql.basketball_sql.season_stats`
  GROUP BY Player
  HAVING active_seasons > 10
);
```
**Insight:** 620 players had long careers, proving endurance and impact.

---

### 5. Average age in the first season
```sql
WITH first_appearance AS (
  SELECT Player, MIN(Year) AS first_year
  FROM `basketball-sql.basketball_sql.season_stats`
  GROUP BY Player
),
joined AS (
  SELECT f.Player, f.first_year, p.born
  FROM first_appearance f
  JOIN `basketball-sql.basketball_sql.players` p ON f.Player = p.Player
  WHERE p.born IS NOT NULL
)
SELECT ROUND(AVG(first_year - CAST(born AS INT64)), 2) AS avg_age_first_season
FROM joined;
```
**Insight:** Players debut at an average age of **24.95** years.

---

## Section 2: Player Performance with Window Functions

### 6. Rolling average (last 3 seasons) of player points
```sql
SELECT Player, Year, PTS,
  ROUND(AVG(PTS) OVER (PARTITION BY Player ORDER BY Year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS rolling_avg_pts
FROM `basketball-sql.basketball_sql.season_stats`
WHERE PTS IS NOT NULL
ORDER BY Player, Year;
```
**Insight:** This helps track momentum or decline per player over time.

---

### 7. Top scorers per season (ranked)
```sql
SELECT Year, Player, PTS,
  RANK() OVER (PARTITION BY Year ORDER BY PTS DESC) AS season_rank
FROM `basketball-sql.basketball_sql.season_stats`
WHERE PTS IS NOT NULL
ORDER BY Year, season_rank;
```
**Insight:** Reveals year-by-year dominance.

---

### 8. Career total points per player
```sql
SELECT Player, SUM(PTS) AS total_points
FROM `basketball-sql.basketball_sql.season_stats`
WHERE PTS IS NOT NULL
GROUP BY Player
ORDER BY total_points DESC
LIMIT 10;
```
**Insight:** Kareem Abdul-Jabbar, Karl Malone, and Wilt Chamberlain lead all-time scoring.

---

### 9. Players who ranked top 3 in scoring multiple times
```sql
WITH ranked AS (
  SELECT Year, Player, PTS,
    RANK() OVER (PARTITION BY Year ORDER BY PTS DESC) AS season_rank
  FROM `basketball-sql.basketball_sql.season_stats`
  WHERE PTS IS NOT NULL
)
SELECT Player, COUNT(*) AS top_3_appearances
FROM ranked
WHERE season_rank <= 3
GROUP BY Player
ORDER BY top_3_appearances DESC
LIMIT 10;
```
**Insight:** Michael Jordan and Karl Malone were top 3 scorers in 11 seasons each.

---

## Section 3: Team-Level Scoring Insights

### 10. Highest team scoring totals in a single season
```sql
SELECT Year, Tm AS team, ROUND(SUM(PTS), 2) AS total_team_points
FROM `basketball-sql.basketball_sql.season_stats`
WHERE Tm != 'TOT' AND PTS IS NOT NULL
GROUP BY Year, team
ORDER BY total_team_points DESC
LIMIT 10;
```
**Insight:** The Denver Nuggets in the 1980s had the most explosive scoring seasons.

---

### 11. Team scoring productivity by decade
```sql
WITH decade_stats AS (
  SELECT Tm AS team, FLOOR(Year / 10) * 10 AS decade, ROUND(SUM(PTS), 2) AS total_points
  FROM `basketball-sql.basketball_sql.season_stats`
  WHERE Tm != 'TOT' AND PTS IS NOT NULL
  GROUP BY team, decade
)
SELECT *
FROM decade_stats
ORDER BY decade, total_points DESC;
```
**Insight:** Highlights dynasties and teams that led their eras.

---

### 12. Players who contributed the highest % of team points
```sql
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
  JOIN top_scorers s ON t.Year = s.Year AND t.team = s.team
)
SELECT *
FROM joined
ORDER BY pct_of_team_score DESC
LIMIT 10;
```
**Insight:** Wilt Chamberlain and Michael Jordan carried a historically high scoring load.

---

### 13. Most balanced teams: smallest gap between 1st & 2nd scorers
```sql
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
```
**Insight:** Some teams, like MEM 2013, had scoring duos nearly identical in output.

---

## Section 4: Advanced Analytical Queries

### 14. Most consistent scorers (low standard deviation in PTS)
```sql
WITH player_pts_stddev AS (
  SELECT Player, COUNT(*) AS seasons_played, ROUND(AVG(PTS), 2) AS avg_pts,
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
```
**Insight:** LeBron James is among the most consistent scorers across long careers.

---

### 15. Biggest season-to-season improvement in scoring
```sql
WITH player_year_pts AS (
  SELECT Player, Year, PTS, LAG(PTS) OVER (PARTITION BY Player ORDER BY Year) AS prev_pts
  FROM `basketball-sql.basketball_sql.season_stats`
  WHERE PTS IS NOT NULL
),
improvements AS (
  SELECT Player, Year, PTS, prev_pts, PTS - prev_pts AS pts_diff
  FROM player_year_pts
  WHERE prev_pts IS NOT NULL
)
SELECT Player, Year, PTS, prev_pts, pts_diff
FROM improvements
ORDER BY pts_diff DESC
LIMIT 10;
```
**Insight:** Michael Jordan and Charlie Scott had explosive breakout seasons.

---

### 16. Biggest season-to-season scoring decline
```sql
SELECT Player, Year, PTS, prev_pts, pts_diff
FROM improvements
ORDER BY pts_diff ASC
LIMIT 10;
```
**Insight:** Retirement, injury, or role change sharply affected player output.

---

### 17. Best rookie scoring seasons (first year only)
```sql
WITH first_season AS (
  SELECT Player, MIN(Year) AS first_year
  FROM `basketball-sql.basketball_sql.season_stats`
  GROUP BY Player
),
rookie_stats AS (
  SELECT s.Player, s.Year, s.PTS
  FROM `basketball-sql.basketball_sql.season_stats` s
  JOIN first_season f ON s.Player = f.Player AND s.Year = f.first_year
  WHERE s.PTS IS NOT NULL
)
SELECT *
FROM rookie_stats
ORDER BY PTS DESC
LIMIT 10;
```
**Insight:** Wilt Chamberlain had one of the greatest rookie seasons in history.

---

### 18. All-time impact (PTS + AST + TRB)
```sql
SELECT Player, SUM(PTS) AS total_pts, SUM(AST) AS total_ast, SUM(TRB) AS total_reb,
  SUM(PTS + AST + TRB) AS total_contribution
FROM `basketball-sql.basketball_sql.season_stats`
WHERE PTS IS NOT NULL AND AST IS NOT NULL AND TRB IS NOT NULL
GROUP BY Player
ORDER BY total_contribution DESC
LIMIT 10;
```
**Insight:** Wilt Chamberlain leads in combined contribution across three categories.

---

## Section 5: Player Classification (Type)

### 19. Categorize players into "Scorer", "Playmaker", "Rebounder", "All-Arounder", or "Role Player"
```sql
WITH player_totals AS (
  SELECT Player, SUM(PTS) AS total_pts, SUM(AST) AS total_ast, SUM(TRB) AS total_reb
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
```
**Insight:** Only 33 All-Arounders and 5 pure Scorers emerged from nearly 4,000 players, proving how rare it is to dominate across categories.

---

## Final Thoughts
This project illustrates how pure SQL (when combined with strategic analysis) can powerfully uncover stories hidden in data. It’s also an example of using data contextually: not just querying for numbers, but explaining what those numbers **mean**.

---

## 📅 Author
Christos Papakostas
