import importlib
from os import listdir
from os.path import isfile, join

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, DoubleType, ArrayType, CharType

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

# Реализация MOSS с использованием алгоритма winnowing
# inspected - исследуемая на предмет плагиата строка
# instance - образец для анализа сходства
# k - размерность k-грамм
# l - размерность окна для winnowing
def moss_winnowing(inspected: str, instance: str, k: int, l: int):
    if (len(inspected) < 1):
        return 0
    if (len(instance) < 1):
        return 1
    k = k if k <= len(inspected) and k <= len(instance) else min(len(inspected), len(instance))
    tmp = min(len(inspected) + 1 - k, len(instance) + 1 - k)
    l = l if l <= tmp else tmp

    inspected_fingerprint = winnowing(get_k_gram_hashes(inspected, k), l)
    instance_fingerprint = winnowing(get_k_gram_hashes(instance, k), l)

    return compare_fingerprints(inspected_fingerprint, instance_fingerprint)

# Алгоритм формирования k-грамм с вычислением их хэш-кодов
def get_k_gram_hashes(input: str, k: int):
    k_gram_hashes = []
    for i, x in enumerate(input):
        if i <= len(input) - k:
            k_gram_hashes.append(hash(input[i:i+k]))
    return k_gram_hashes

# Алгоритм формирования fingerprint'а
def winnowing(input: list[int], l: int):
    fingerprint_indices = []
    for i, x in enumerate(input):
        if i <= len(input) - l:
            min = input[i]
            index = i
            for j, y in enumerate(input[i+1:i+l]):
                if (y < min):
                    min = y
                    index = j + i + 1
            fingerprint_indices.append(index)
    fingerprint_indices = list(set(fingerprint_indices))
    fingerprint_indices.sort()
    return list(map(lambda x: input[x], fingerprint_indices))

# Алгоритм вычисления меры сходства
def compare_fingerprints(inspected_fingerprint: list[int], instance_fingerprint: list[int]):
    count = 0
    for x in inspected_fingerprint:
        if x in instance_fingerprint:
            count += 1
    return count / len(inspected_fingerprint)



class SparkContextCommon:
    def __init__(self, spark):
        self.spark = spark
        self.spark.udf.register('csharp_prepare', lambda x: csharp_prepare(x), StringType())
        self.spark.udf.register('csharp_prepare_2', lambda x: list(csharp_prepare(x)), ArrayType(StringType()))
        self.spark.udf.register('moss', lambda a, b, k, l: moss_winnowing(a, b, k, l), DoubleType())

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
