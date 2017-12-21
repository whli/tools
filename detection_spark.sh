#!/bin/bash

###
# spark应用项目监控，每10分钟运行一次，运行超时的项目会被kill,并发送报警邮件
# 应用说明：在app_list中添加appName和运行结束时间
###

Python="/usr/bin/python"

# 数据格式：spark_appname;运行结束时间
# spark_appname: appname中不能出现空格
app_list=(
			"PersonalRank_India;09:00:00"
			"Beta_Statis_for_Show;02:00:00"
			"app_statis;02:30:00"
)

now_time=`date +%H:%M:%S`
for item in ${app_list[@]}
do
	IFS=';' arr=($item)
	appname=${arr[0]}
	endtime=${arr[1]}
	info=`yarn application -list | grep "$appname"`
	if [ $? -eq 0 ];then
		if [[ $now_time > $endtime ]];then
			process_num=`echo $info | cut -d' ' -f 1`
			#echo $process_num
			#echo $appname
			#echo $endtime
			yarn application -kill $process_num
			$Python warning_mail.py "spark app $appname is overtime and has been killed!" "spark_app_monitor"
		fi
	fi
done
