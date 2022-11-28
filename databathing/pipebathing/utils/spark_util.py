from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import os

from databathing.pipebathing.config import spark_conf_setting


def init_spark(project_name, env='root'):
    """init the spark.
    Args:
        None.

    Returns:
        Spark.
    """
    global spark

    # if env != 'root':
    #     os.environ['PYSPARK_SUBMIT_ARGS'] = """--jars gcs-connector-hadoop2-latest.jar pyspark-shell"""
    spark_conf = SparkConf().setAppName(project_name).setAll(spark_conf_setting)
    sc = SparkContext(conf=spark_conf)

    if env != 'root':
        sc._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        sc._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.enable", "true")
        sc._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.gs.impl",
                                          "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

    spark = SparkSession.builder. \
        config(conf=sc.getConf()). \
        enableHiveSupport(). \
        getOrCreate()

    return spark


def get_spark():
    return spark
