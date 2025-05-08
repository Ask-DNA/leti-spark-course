from pyspark.sql import DataFrame
from pyspark.sql.functions import posexplode
from lab2.common import SparkContextCommon

def solve(common: SparkContextCommon) -> DataFrame:
    common.view('raw_data', common.read_data())



    # Решение через UDF



    df = common.view('moss_val', """
        SELECT
            input1.solution AS inspected,
            input2.solution AS instance,
            round(
                moss(
                    csharp_prepare(input1.content), -- Исследуемый на предмет плагиата код
                    csharp_prepare(input2.content), -- Образец для анализа сходства
                    10,                             -- Размерность k-грамм
                    10                              -- Размерность окна для winnowing
                ),
                2
            ) AS plagiarism_measure
        FROM raw_data input1 JOIN raw_data input2 ON input1.solution != input2.solution
        ORDER BY plagiarism_measure DESC
    """)

    df.show()



    # Решение через SQL



    df = common.view('data', """
        SELECT solution, posexplode(csharp_prepare_2(content)) FROM raw_data
    """)

    # k_grams(solution, position, k_gram)
    # solution - название файла
    # position - позиция k-граммы (совпадает с индексом первого элемента k-граммы)
    # k_gram - k-грамма в виде массива строк (k = 10)
    df = common.view('k_grams', """
        WITH tbl(sol, count) AS (
            SELECT solution AS sol, COUNT(*) AS count
            FROM data
            GROUP BY solution
        )
        SELECT
            solution,
            pos AS position,
            ARRAY_AGG(col) OVER(
                PARTITION BY solution
                ORDER BY pos
                ROWS BETWEEN CURRENT ROW AND 9 FOLLOWING) AS k_gram
        FROM data
        WHERE pos == 0 OR pos <= ((SELECT FIRST(count) FROM tbl WHERE sol == solution) - 10)
    """)

    # fingerprint_items(solution, choosen_hash, choosen_hash_position)
    # solution - название файла
    # choosen_hash - выбранный в качестве части fingerprint'а хэш
    # choosen_hash_position - позиция выбранного хэша
    df = common.view('fingerprint_items', """
        WITH tbl(sol, count) AS (
            SELECT solution AS sol, COUNT(*) AS count
            FROM k_grams
            GROUP BY solution
        )
        SELECT DISTINCT
            solution,
            MIN(hash(k_gram)) OVER w AS choosen_hash,
            MIN_BY(position, hash(k_gram)) OVER w AS choosen_hash_position
        FROM k_grams
        WHERE position == 0 OR position <= ((SELECT FIRST(count) FROM tbl WHERE sol == solution) - 10)
        WINDOW w AS (
            PARTITION BY solution
            ORDER BY position
            ROWS BETWEEN CURRENT ROW AND 9 FOLLOWING)
    """)

    # fingerprints(solution, fingerprint)
    # solution - название файла
    # fingerprint - массив выбранных хэшей
    df = common.view('fingerprints', """
        SELECT
            solution,
            ARRAY_AGG(choosen_hash) AS fingerprint
        FROM fingerprint_items
        GROUP BY solution
    """)

    df = common.view('moss_val_2', """
        SELECT
            input1.solution AS inspected,
            input2.solution AS instance,
            ROUND(
                (ARRAY_SIZE(input1.fingerprint)
                - ARRAY_SIZE(ARRAY_EXCEPT(input1.fingerprint, input2.fingerprint)))
                / ARRAY_SIZE(input1.fingerprint),
                2
            ) AS plagiarism_measure
        FROM fingerprints input1 JOIN fingerprints input2 ON input1.solution != input2.solution
        ORDER BY plagiarism_measure DESC
    """)

    df.show()