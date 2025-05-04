from pyspark.shell import spark
from pyspark.sql import DataFrame

from lab1.common import read_csv, view

def solve() -> DataFrame:
    match = read_csv('match')
    player = read_csv('player')
    player_result = read_csv('player_result')

    view("match", match)
    view("player", player)
    view("player_result", player_result)

    # player_to_match(player_id, player_name, match_id, finished_at, is_winner)
    # player_id, player_name - данные игрока
    # match_id, finished_at - данные матча
    # is_winner - победила ли команда игрока в данном матче
    view("player_to_match", """
        SELECT
            p.player_id AS player_id,
            p.name AS player_name,
            pr.match_id AS match_id,
            m.finished_at AS finished_at,
            (pr.is_radiant = m.radiant_won) AS is_winner
        FROM player p
            JOIN player_result pr ON p.player_id = pr.player_id
            JOIN match m ON pr.match_id = m.match_id
    """)

    # nemesis(player_name, nemesis_name, total_matches, winning_matches, win_rate)
    # player_name - данные игрока
    # nemesis_name - данные второго игрока (немезиды)
    # total_matches - сколько всего матчей проведено с игроком и немезидой,
    #     при условии принадлежности к разным командам
    # winning_matches - в каком числе матчей игрок оказывался в команде, одержавшей победу
    # win_rate - отношение winning_matches / total_matches
    view("nemesis", """
        SELECT
            p.player_name AS player_name,
            n.player_name AS nemesis_name,
            COUNT(*) AS total_matches,
            COUNT(*) FILTER(WHERE p.is_winner) AS winning_matches,
            (COUNT(*) FILTER(WHERE p.is_winner)) / COUNT(*) AS win_rate
        FROM player_to_match p
            JOIN player_to_match n ON (p.match_id = n.match_id AND p.is_winner != n.is_winner)
        GROUP BY p.player_name, n.player_name
        HAVING total_matches >= 5
        ORDER BY win_rate DESC
    """)

    # player_streak_groups(player_id, player_name, match_finished_at, is_winner, streak_group)
    # player_id, player_name - данные игрока
    # match_finished_at - время завершения матча, в котором участвовал игрок
    # is_winner - победила ли команда игрока в данном матче
    # streak_group - технический параметр, указывающий, какой серии побед/поражений
    #     принадлежит данное вхождение. серией считается группа идущих подряд во времени
    #     матчей с одинаковым исходом (победа/поражение)
    view("player_streak_groups", """
        WITH tbl AS(
        SELECT *,
            CASE
                WHEN LAG(is_winner) OVER(PARTITION BY player_id ORDER BY finished_at) = is_winner
                THEN 0
                ELSE 1
            END AS new_group
        FROM player_to_match
        )
        SELECT
            player_id,
            player_name,
            finished_at AS match_finished_at,
            is_winner,
            SUM(new_group) OVER(PARTITION BY player_id ORDER BY finished_at) AS streak_group
        FROM tbl
    """)

    # player_winstreaks(player_id, player_name, winstreak_len)
    # player_id, player_name - данные игрока
    # winstreak_len - длина очередной серии побед
    view("player_winstreaks", """
        SELECT
            player_id,
            player_name,
            COUNT(*) AS winstreak_len
        FROM player_streak_groups
        WHERE is_winner
        GROUP BY player_id, player_name, streak_group
    """)

    # avg_winstreak_len(player_name, avg_winstreak_len)
    # player_name - данные игрока
    # avg_winstreak_len - средняя длина победных серий
    view("avg_winstreak_len", """
        SELECT
            player_name,
            AVG(winstreak_len) AS avg_winstreak_len
        FROM player_winstreaks
        GROUP BY player_name
        ORDER BY avg_winstreak_len DESC
    """)

    nemesis_df = spark.sql("select * from nemesis")
    avg_winstreak_len_df = spark.sql("select * from avg_winstreak_len")
    return nemesis_df, avg_winstreak_len_df
