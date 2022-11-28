spark_conf_setting = [
    ("hive.exec.dynamic.partition", "true"),
    ("hive.exec.dynamic.partition.mode", "nonstrict"),
    ("spark.executor.memory", "10g"),
    ("spark.driver.memory", "10g"),
    ("spark.executor.cores", "3"),
    ("spark.executor.instances", "20"),
    ("spark.sql.shuffle.partitions", "300"),
    ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
    ("spark.sql.orc.filterPushdown", "true"),
    ("spark.sql.orc.char.enabled", "true"),
    ("spark.sql.hive.convertMetastoreOrc", "true"),
    ("spark.sql.orc.enabled", "true"),
    ("spark.sql.debug.maxToStringFields", "2000"),
    ("spark.debug.maxToStringFields", "2000"),
    ("spark.kryoserializer.buffer.max", "1g")
]
