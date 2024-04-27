import logging
from typing import Union
from warnings import simplefilter

import pandas as pd
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

logger = logging.getLogger(__name__)


def get_spark_session(
    shuffle_partitions: int = 200,
) -> Union[SparkContext, SparkSession]:
    """This function return the spark context and spark session

    :return: Description of return value

    """
    conf = SparkConf()

    conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
    conf.set("spark.sql.jsonGenerator.ignoreNullFields", "False")

    sc = SparkContext.getOrCreate(SparkContext(conf=conf))

    spark = SparkSession.builder.getOrCreate()

    return sc, spark
