from pyspark.sql import DataFrame
from lab2.common import SparkContextCommon

def solve(common: SparkContextCommon) -> DataFrame:
    common.view('raw_data', common.read_data())

    df = common.view('data', """
        SELECT solution, posexplode(csharp_prepare(content)) FROM raw_data
    """)

    # k_grams(solution, position, k_gram)
    # solution - название файла
    # position - позиция k-граммы (совпадает с индексом первого элемента k-граммы)
    # k_gram - k-грамма в виде строки
    df = common.view('k_grams', """
        SELECT
            solution,
            pos AS position,
            CONCAT_WS(
                '',
                ARRAY_AGG(col) OVER(
                    PARTITION BY solution
                    ORDER BY pos
                    ROWS BETWEEN CURRENT ROW AND 9 FOLLOWING)
            ) AS k_gram
        FROM data
    """)

    # версия предыдущей таблицы, в которой отбрасываются лишние k-граммы
    df = common.view('k_grams_trunc', """
        WITH tbl(sol, count) AS (
            SELECT solution AS sol, COUNT(*) AS count
            FROM data
            GROUP BY solution
        )
        SELECT * FROM k_grams
        WHERE position == 0 OR position <= ((SELECT FIRST(count) FROM tbl WHERE sol == solution) - 10)
    """)

    # fingerprint_items(solution, window_position, choosen_hash, choosen_hash_position)
    # solution - название файла
    # window_position - позиция начала окна
    # choosen_hash - выбранный в качестве части fingerprint'а хэш
    # choosen_hash_position - позиция выбранного хэша
    df = common.view('fingerprint_items', """
        SELECT DISTINCT
            solution,
            position AS window_position,
            MIN(hash(k_gram)) OVER w AS choosen_hash,
            MIN_BY(position, hash(k_gram)) OVER w AS choosen_hash_position
        FROM k_grams_trunc
        WINDOW w AS (
            PARTITION BY solution
            ORDER BY position
            ROWS BETWEEN CURRENT ROW AND 9 FOLLOWING)
    """)

    # версия предыдущей таблицы, в которой отбрасываются лишние fingerprint'ы
    df = common.view('fingerprint_items_trunc', """
        WITH tbl(sol, count) AS (
            SELECT solution AS sol, COUNT(*) AS count
            FROM k_grams_trunc
            GROUP BY solution
        )
        SELECT DISTINCT solution, choosen_hash, choosen_hash_position
        FROM fingerprint_items
        WHERE window_position == 0 OR window_position <= ((SELECT FIRST(count) FROM tbl WHERE sol == solution) - 10)
        WINDOW w AS (
            PARTITION BY solution
            ORDER BY position
            ROWS BETWEEN CURRENT ROW AND 9 FOLLOWING)
    """)

    df = common.view('moss_val', """
        SELECT
            input1.solution AS inspected,
            input2.solution AS instance,
            ROUND((
                (SELECT COUNT(*) FROM
                    (
                        SELECT choosen_hash FROM fingerprint_items_trunc WHERE solution == input1.solution
                        INTERSECT ALL
                        SELECT choosen_hash FROM fingerprint_items_trunc WHERE solution == input2.solution
                    ) x
                )
                /
                (
                SELECT COUNT(*) FROM fingerprint_items_trunc WHERE solution == input1.solution
                )
            ), 2) AS plagiarism_measure
        FROM raw_data input1 JOIN raw_data input2 ON input1.solution != input2.solution
        ORDER BY plagiarism_measure DESC
    """)

    df.show()


