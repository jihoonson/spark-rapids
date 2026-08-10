#! /bin/bash

TABLE=haloish_output

rm -rf spark-warehouse/${TABLE}

export SPARK_RAPIDS_PLUGIN_JAR=/home/jihoons/Local/opt/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12-spark355-deep-schema.jar
# export SPARK_RAPIDS_PLUGIN_JAR=/home/jihoons/Local/opt/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12-spark355-deep-schema-cudf-mem-limit.jar

  spark-submit \
    --master "local[16]" \
    --conf spark.driver.maxResultSize=2gb \
    --conf spark.driver.memory=16g \
    --conf spark.sql.files.maxPartitionBytes=2gb \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --conf spark.rapids.memory.host.spillStorageSize=16G \
    --conf spark.rapids.memory.pinnedPool.size=8g \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=/tmp/spark-events \
    --conf spark.rapids.sql.batchSizeBytes=1073741823 \
    --conf spark.rapids.sql.enabled=true \
    --conf spark.driver.extraClassPath=$SPARK_RAPIDS_PLUGIN_JAR \
    --packages io.delta:delta-spark_2.12:3.3.1 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.driver.extraJavaOptions=-Dai.rapids.cudf.nvtx.enabled=true \
    --conf spark.rapids.sql.schemaEvolution.fromScalarAllNullNested.enabled=true \
    --conf spark.rapids.sql.schemaEvolution.copyAllNullNested.enabled=true \
    --conf spark.sql.cache.serializer=com.nvidia.spark.ParquetCachedBatchSerializer \
    local_haloish_delta_write.py \
    --table default.${TABLE}

    # --conf spark.rapids.flameGraph.pathPrefix=/home/jihoons/Projects/spark-rapids-works/deeply-nested-schema/fgs/gpu_skip_all_write/ \
    # --conf spark.rapids.flameGraph.stageEpochInterval=2 \
    # --conf spark.shuffle.manager=com.nvidia.spark.rapids.spark355.RapidsShuffleManager \
    # --conf spark.rapids.shuffle.mode=MULTITHREADED \
    # --conf spark.sql.files.minPartitionNum=1 \