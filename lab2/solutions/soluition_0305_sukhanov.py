from pyspark.sql import DataFrame
from lab2.common import SparkContextCommon

def solve(common: SparkContextCommon) -> DataFrame:
    common.view('raw_data', common.read_data())
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
    """)
    df.show()
