#! /bin/bash

TABLE=haloish_cpu_output

rm -rf spark-warehouse/${TABLE}

  spark-submit \
    --master "local[32]" \
    --conf spark.driver.maxResultSize=2gb \
    --conf spark.driver.memory=64g \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=/tmp/spark-events \
    --packages io.delta:delta-spark_2.12:3.3.1 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    local_haloish_delta_write.py \
    --table default.${TABLE}

# --conf spark.rapids.flameGraph.pathPrefix=/home/jihoons/Projects/spark-rapids-works/deeply-nested-schema/fgs/cpu \