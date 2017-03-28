#!/bin/bash

#退出spark进程
ps -ef | grep spark | grep -v grep | grep -v spark_ketang.sh | cut -c 9-15 | xargs kill -s 9

#删除管理员转发相关信息
rm message_file

#启动spark程序
/home/hadoop/spark-1.4.0/bin/spark-submit \
--master spark://10.3.250.101:7077 \
--total-executor-cores 10 \
--executor-memory 3G \
/home/liweihua/ketang_statis/ketang_main/chuqin.py &

/home/hadoop/spark-1.4.0/bin/spark-submit \
--master spark://10.3.250.101:7077 \
--total-executor-cores 10 \
--executor-memory 3G \
/home/liweihua/ketang_statis/ketang_main/rank.py &

/home/hadoop/spark-1.4.0/bin/spark-submit \
--master spark://10.3.250.101:7077 \
--total-executor-cores 10 \
--executor-memory 3G \
/home/liweihua/ketang_statis/ketang_main/zhuanfa.py &


wait
