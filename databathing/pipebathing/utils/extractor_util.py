

def generate_extractor_stmt(sources):
    extractors_str = ""

    for source in sources:
        if source["type"] == "orc":
            extractors_str += "global {}\n".format(source["name"])
            extractors_str += "{} = spark_global.read.{}(\"{}\")\n".format(source["name"], source["type"], source["path"])
        elif source["type"] == "bigquery":
            print("bigquery: have not yet implemented, will do soon!!!")
        elif source["type"] == "mssql":
            print("mssql: have not yet implemented, will do soon!!!")
        elif source["type"] == "mysql":
            print("mysql: have not yet implemented, will do soon!!!")
        elif source["type"] == "mft":
            print("mft: have not yet implemented, will do soon!!!")
        elif source["type"] == "mongo":
            print("mongo: have not yet implemented, will do soon!!!")
        elif source["type"] == "elastic search":
            print("elastic search: have not yet implemented, will do soon!!!")
        elif source["type"] == "cassandra":
            print("cassandra: have not yet implemented, will do soon!!!")
        elif source["type"] == "oracle":
            print("oracle: have not yet implemented, will do soon!!!")
        else:
            print("{}: have not yet implemented, will do soon!!!".format(source["type"]))

    return extractors_str
    