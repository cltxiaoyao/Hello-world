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

######���ڴ˴���ʼ��д���Ľű��߼�����
source ~/.profile
current_path=$(cd `dirname $0`; pwd)
sh $current_path/1_dataclean_front_user.sh
#sh $current_path/2_dataclean_front_user.sh
#sh $current_path/3_dataclean_front_user.sh
######ִ�нű��ɹ���ʧ�ܵı�׼ֻȡ���ڽű����һ��ִ�����ķ���ֵ
######�������ֵΪ0������Ϊ�˽ű�ִ�гɹ��������0������Ϊ�ű�ִ��ʧ��



