import importlib
from os import listdir
from os.path import isfile, join

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, ArrayType

import re

from lab2.config import data_path


def clear_solution_name(path: str) -> str:
    return path.split('/')[-1].split('.cs')[0]

clear_solution_name_udf = udf(lambda path: clear_solution_name(path), StringType())


# Алгоритм подготовки кода к анализу
def csharp_prepare(source_code: str):
    # Удаление однострочных и многострочных комментариев
    result = re.sub(
        r'(?x)( ""(?> (?<=@.) (?>[^""]+|"""")*  | (?> [^""\\]+ | \\. )* ) ""| \' (?> [^\'\\]+ | \\. )* \')| // .* | /\* (?s: .*? \*/ )',
        '',
        source_code)
    # Удаление последовательностей пробельных символов
    result = re.sub(r'\s+', ' ', result)
    return result


class SparkContextCommon:
    def __init__(self, spark):
        self.spark = spark
        self.spark.udf.register('csharp_prepare', lambda x: list(csharp_prepare(x)), ArrayType(StringType()))

    def view(self, name: str, target: DataFrame | str) -> DataFrame:
        df = target if isinstance(target, DataFrame) else self.spark.sql(target)
        df.createOrReplaceTempView(name)
        return df

    def read_data(self) -> DataFrame:
        sc = self.spark.sparkContext
        rdd = sc.wholeTextFiles(data_path)
        columns_mapping = {
            '_1': 'path',
            '_2': 'content',
        }
        df = (self.spark.createDataFrame(rdd)
              .withColumnsRenamed(columns_mapping)
              .withColumn('solution', clear_solution_name_udf(col('path')))
              .drop('path'))
        return df


def list_solutions(path):
    files = [f for f in listdir(path) if isfile(join(path, f))]
    return [f.replace('.py', '') for f in files if 'template' not in f]


def run_solution(name):
    spark = SparkSession.builder.master("local").getOrCreate()
    common = SparkContextCommon(spark)
    module = importlib.import_module(f'lab2.solutions.{name}')
    return getattr(module, 'solve')(common)


def list_solutions(path):
    files = [f for f in listdir(path) if isfile(join(path, f))]
    return [f.replace('.py', '') for f in files if 'template' not in f]
