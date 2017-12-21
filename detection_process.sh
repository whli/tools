#!/bin/bash

# 根据不同的项目脚本名称需要有特殊标识，否则会牵扯过多
# 根据项目的启动时间及项目大概运行的时长，设置监测程序启动的时间

# 主控脚本监控
process_name="run_test.sh"
info=`ps -fe | grep "$process_name" | grep -v grep | grep -v cd`
if [ $? -eq 0 ];then
	process_num=`echo $info | cut -d' ' -f 2`
	echo $process_num
	#kill $process_num
fi

# 计算脚本监控
process_name="test_process.py"
info=`ps -fe | grep "$process_name" | grep -v grep | grep -v cd`
if [ $? -eq 0 ];then
    process_num=`echo $info | cut -d' ' -f 2`
	echo $process_num
    #kill $process_num
fi

# spark程序监测
process_list=("Spark" "topic")
show_result="running app list is : "
for process in ${process_list[@]}
do
	info=`yarn application -list | grep "$process"`
	if [ $? -eq 0 ];then
		show_result=$show_result$process" ; "
		process_num=`echo $info | cut -d' ' -f 1`
		echo $process_num
		#yarn application -kill $process_num
	fi
done

echo $show_result
