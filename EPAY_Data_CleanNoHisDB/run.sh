#!/bin/bash

function LOG
{
   if [ "x`uname -s`" = "xLinux" ]; then
       ECHO_CMD="echo -e "
   else
       ECHO_CMD="echo "
   fi

   typeset log_level=$1
   typeset log_message=$2
   typeset log_date=`date +"%Y-%m-%d %H:%M:%S"`
   ${ECHO_CMD} "[${log_date}][${log_level}][PID:$$] ${log_message}" 
}

#####default founction lib, call it when success
function task_success
{
   LOG "INFO" ">>>Task Success:[$*]<<<"
   exit 0
}

#####default founction lib, call it when failed
function task_fail
{
   LOG "INFO" ">>>Task fail:[$*]<<<"
   exit 1
}

#####main######
LOG "INFO" ">>>Task Start<<<"

######可在此处开始编写您的脚本逻辑代码
source ~/.profile
current_path=$(cd `dirname $0`; pwd)
sh $current_path/1_dataclean_front_user.sh
#sh $current_path/2_dataclean_front_user.sh
#sh $current_path/3_dataclean_front_user.sh
######执行脚本成功和失败的标准只取决于脚本最后一条执行语句的返回值
######如果返回值为0，则认为此脚本执行成功，如果非0，则认为脚本执行失败



