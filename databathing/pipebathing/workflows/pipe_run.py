import pandas as pd
import sys
import yaml

from databathing.pipebathing.config import df_storage
from databathing.pipebathing.utils import generate_extractor_stmt, generate_loader_stmt, Pipeline, get_log, get_spark

from pyspark.sql.functions import sum,avg,max,min,mean,count
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

from databathing.querybathing import querybathing
 
LOG = get_log(__name__)

pipeline = Pipeline()

@pipeline.task()
def extractor():
    LOG.info(f"databathing_workflow: extractor")
    try:
        extractors_stream = open(df_storage.yaml_storage["extracts"], 'r')
        extractors_dict = yaml.safe_load(extractors_stream)
        sources = extractors_dict["sources"]
        extractors_str = generate_extractor_stmt(sources)
        exec(extractors_str)

    except Exception as error:
        LOG.exception(f"databathing_workflow: extractor failure. {error}")
        sys.exit(1)


@pipeline.task(depends_on=extractor)
def transform(flag):
    LOG.info(f"databathing_workflow: transform")
    try:
        # exec("editor.show()")
        transforms_stream = open(df_storage.yaml_storage["transforms"], 'r')
        transforms_dict = yaml.safe_load(transforms_stream)

        steps = transforms_dict["steps"]
        final_ans = ""

        for step in steps:
            query = step["query"]
            share_dfs = step["share_df"]
            share_dfs_str = ""
            name = step["name"]

            for share_df in share_dfs:
                share_dfs_str += "global {}\n".format(share_df)

            ans = querybathing(query).parse()
            final_ans += share_dfs_str + ans + "{} = final_df\n\n".format(name)

        exec(final_ans)
    except Exception as error:
        LOG.exception(f"databathing_workflow: transform failure. {error}")
        sys.exit(1)



@pipeline.task(depends_on=transform)
def loader(flag):
    LOG.info(f"databathing_workflow: loader")
    try:
        # exec("fact_dim_df.show()")
        # exec("agg_df.show()")

        loaders_stream = open(df_storage.yaml_storage["loaders"], 'r')
        loaders_dict = yaml.safe_load(loaders_stream)
        targets = loaders_dict["targets"]
        targets_str = generate_loader_stmt(targets)
        exec(targets_str)
        
    except Exception as error:
        LOG.exception(f"databathing_workflow: loader failure. {error}")
        sys.exit(1)



def pipe_run():
    global spark_global
    spark_global = get_spark()
    global start_time

    LOG.info(f"test_databathing_workflow: start")
    start_time = pd.Timestamp.now()
    pipeline.run()
    end_time = pd.Timestamp.now()
    LOG.info(f"test_databathing_workflow total use {end_time - start_time}")
