#!/usr/bin

######################################################
#
# File Name:  
#
# Function:   
#
# Usage:  
#
# Author:
#
# Create Time:    
#
######################################################


PYTHON="/usr/local/python2.7/bin/python"

# 已回溯到160901
start_day='2016-12-25'
end_day='2016-12-23'
	
i=0

while 1>0
do 

                s_day=`date -d "$start_day - $i day" +%Y-%m-%d`
		#next_day=`date -d "$s_day + $1 day" +%Y-%m-%d`
		start_time=${s_day}" 00:00:00"
		end_time=${s_day}" 23:59:59" 
		start_unix=`date -d "$start_time" +%s`
		end_unix=`date -d "$end_time" +%s`

		$PYTHON ./class_question.py $start_unix $end_unix
		
                echo $s_day

		i=$(($i+1))
		if [ $s_day = $end_day ];then
			exit
		fi

done
