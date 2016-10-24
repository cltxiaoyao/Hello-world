#!/bin/bash

SCRIPT="$0"
# SCRIPT may be an arbitrarily deep series of symlinks. Loop until we have the concrete path.
while [ -h "$SCRIPT" ] ; 
do
    ls=`ls -ld "$SCRIPT"`
    # Drop everything prior to ->  
    link=`expr "$ls" : '.*-> \(.*\)$'`  
    if expr "$link" : '/.*' > /dev/null; then
        SCRIPT="$link"
    else
        SCRIPT=`dirname "$SCRIPT"`/"$link"
        fidoneSCRIPT_DIR=`dirname $SCRIPT`
        SCRIPT_DIR=`cd $SCRIPT_DIR && pwd`
    fi
done
SCRIPT_DIR=`dirname $SCRIPT`
SCRIPT_DIR=`cd $SCRIPT_DIR && pwd`

APP_PATH=`cd ${SCRIPT_DIR}/.. && pwd`
BIN_PATH=${SCRIPT_DIR}

fileName="dbMigration.sh"

#Query The Current Execute User
Current_Execute_User=`whoami`
Parameters_Numbers="$#"

FILE_NAME="dbMigration.sh"
if [ ${Parameters_Numbers} -ne 3 ];then
    echo "Error" " ${fileName} Parameters shoule be three"
fi

DataSOURCE_FILE=${APP_PATH}/conf/datasource.properties

DB_SOURCE_USER="$1"
BATCH_NUM_TEMP="$2"
EXECUTE_RESULT_DIR="$3"

DB_SOURCE_TNS=`cat ${DataSOURCE_FILE} |grep -Ewi "^[[:space:]]*${DB_SOURCE_USER}_database_tnsname" |head -1 |cut -d = -f 2- |sed 's/^[[:space:]]*//g' |sed 's/[[:space:]]*$//g'`

BATCH_NUM=`echo ${BATCH_NUM_TEMP}|awk -F "BATCH_NUM" '{print $2}'`


EXECUTE_EXP_LOGS_DIR="${EXECUTE_RESULT_DIR}/logs/ExecuteExpLogs/${DB_SOURCE_USER}"
EXECUTE_IMP_LOGS_DIR="${EXECUTE_RESULT_DIR}/logs/ExecuteImpLogs/${DB_SOURCE_USER}"
EXECUTE_SQL_LOGS_DIR="${EXECUTE_RESULT_DIR}/logs/ExecuteSqlLogs/${DB_SOURCE_USER}"
EXECUTE_EXP_DIR="${EXECUTE_RESULT_DIR}/expPars/${DB_SOURCE_USER}"
EXECUTE_IMP_DIR="${EXECUTE_RESULT_DIR}/impPars/${DB_SOURCE_USER}"
EXECUTE_SQL_DIR="${EXECUTE_RESULT_DIR}/sqls/${DB_SOURCE_USER}"
EXECUTE_DUMP_DIR="${EXECUTE_RESULT_DIR}/dumps/${DB_SOURCE_USER}"
LOG_FILE=${EXECUTE_RESULT_DIR}/migration${BATCH_NUM}.log

    


function printLog
{
    typeset localTime=`date "+%F %T"`
    if [ $# -ne 2 ]; then
        echo "[${localTime}]" "$*" |tee -a "${LOG_FILE}"
    else
        # Log Level: INFO/WARN/ERROR
        typeset logLevel=`echo "$1" |tr "a-z" "A-Z"`
        # logLevel=$(printf "%5s" "${logLevel}")
        
        typeset logMessage="$2"
        echo "[${localTime}] [${logLevel}] ${logMessage}" |tee -a "${LOG_FILE}"
    fi
}

function checkUserConnect 
{ 
	typeset funcName="checkUserConnect" 
	[[ $# -ne 1 ]] && printLog "ERROR" "Input parameters numbers must be one, ${funcName} dbtnsname" && exit 1 
	typeset tnsname="$1" 
	typeset result=$(sqlplus /@"${tnsname}" <<EOF 
	set heading off;
	set echo off;
	set feedback off;
	select 1 from dual;
	exit EOF)

	typeset connectStatus=`echo "${result}"|grep -w "Connected" |wc -l` 
	if [ ${connectStatus} -ne 1 ]
		then 
			printLog "ERROR" "Connect database failed2 by TNSNAME:${tnsname}, please check it." 
			return 1 
	fi 
		return 0 
}


function replaceSpecStrWithNewStrInFile 
{ 
	typeset funcName="replaceSpecStrWithNewStrInFile" 
	if [ $# -ne 3 ] 
		then 
			printLog "ERROR" "Parameter Number Error. The parameter number must be there." 
			return 1 
	fi 
	typeset oldString="$1" 
	typeset newString="$2" 
	typeset fileName="$3" 
	if [ ! -f "${fileName}" ] 
		then printLog "ERROR" "${funcName}" "File ${fileName} does not exist." 
		return 1 
	fi 
	typeset sedOldString=`echo ${oldString} |sed 's/\//\\\\\//g'` 
	typeset sedNewString=`echo ${newString} |sed 's/\//\\\\\//g'` 
	sed -i "s/${sedOldString}/${sedNewString}/g" "${fileName}" 
	if [ $? -ne 0 ] 
		then printLog "ERROR" "${funcName}" "Replace string in ${fileName} failed." 
		return 1 
	fi 
	return 0 
}



function prepareEnv
{
    
	
	if [ ! -d ${EXECUTE_RESULT_DIR} ]
		then 
			mkdir -p ${EXECUTE_RESULT_DIR}
	fi
	
	
	if [ ! -d ${EXECUTE_EXP_LOGS_DIR} ]
		then 
			mkdir -p ${EXECUTE_EXP_LOGS_DIR}
	fi
	
	
	if [ ! -d ${EXECUTE_IMP_LOGS_DIR} ]
		then 
			mkdir -p ${EXECUTE_IMP_LOGS_DIR}
	fi
	
	
	if [ ! -d ${EXECUTE_SQL_LOGS_DIR} ]
		then 
			mkdir -p ${EXECUTE_SQL_LOGS_DIR}
	fi
	
	
	if [ ! -d ${EXECUTE_EXP_DIR} ]
		then 
			mkdir -p ${EXECUTE_EXP_DIR}
	fi
	
	
	if [ ! -d ${EXECUTE_IMP_DIR} ]
		then 
			mkdir -p ${EXECUTE_IMP_DIR}
	fi
	
	
	if [ ! -d ${EXECUTE_SQL_DIR} ]
		then 
			mkdir -p ${EXECUTE_SQL_DIR}
	fi
	
	if [ ! -d ${EXECUTE_DUMP_DIR} ]
		then 
			mkdir -p ${EXECUTE_DUMP_DIR}
	fi
	

    [ ! -f ${LOG_FILE} ] && touch ${LOG_FILE}
}

function prepareMigrationScript
{
    typeset funcName="prepareMigrationScript"
    
	typeset expParFileNumber=`find ${EXECUTE_EXP_DIR} -type f -name "*.par" |wc -l`
	
	printLog "Info" "Now preparing to execute batch task ${BATCH_NUM}..."
	
	printLog "Info" "Source schema: ${DB_SOURCE_USER}"
	
	printLog "Info" "Dest schema: ${DB_DEST_USER}"
	
	
    if [ $expParFileNumber -ne 1 ]
		then
			#printLog "Error" "Generate files failed, exp file number is not correct."
			printLog "info" "Generate files null, the table or parition is not exsit."
			exit 1
	fi
	exp_file=`ls -l ${EXECUTE_EXP_DIR} | grep '^-' | grep 'par' | awk -F " " '{print $NF}'`
	exp_log=`cat ${EXECUTE_EXP_DIR}/${exp_file} | grep "^log" | cut -d "=" -f 2`
	exp_dump=`cat ${EXECUTE_EXP_DIR}/${exp_file} | grep "^file" | cut -d "=" -f 2`
	
	typeset sqlFileNumber=`find ${EXECUTE_SQL_DIR} -type f -name "*.sql" |wc -l`	
	if [ $sqlFileNumber -ne 1 ]
		then
			#printLog "Error" "Generate files failed, sql file number is not correct."
			printLog "info" "Generate files null, the table or parition is not exsit."
			exit 1
	fi
	sql_file=`ls -l ${EXECUTE_SQL_DIR} | grep '^-' | grep 'sql' | awk -F " " '{print $NF}'`
	
	
	
	checkUserConnect "${DB_SOURCE_TNS}"
	
	if [ $? -ne 0 ]
		then 
			printLog "Error" "Connect oracle failed."
			exit 1
	fi
	
	if [ $? -ne 0 ]
		then 
			printLog "Error" "Connect oracle failed."
			exit 1
	fi
	
	replaceSpecStrWithNewStrInFile "userid=${DB_SOURCE_USER}" "userid=/@${DB_SOURCE_TNS}" ${EXECUTE_EXP_DIR}/${exp_file}
	replaceSpecStrWithNewStrInFile "${exp_dump}" "${EXECUTE_DUMP_DIR}/${exp_dump}" ${EXECUTE_EXP_DIR}/${exp_file}
	replaceSpecStrWithNewStrInFile "${exp_log}" "${EXECUTE_EXP_LOGS_DIR}/${exp_log}" ${EXECUTE_EXP_DIR}/${exp_file}

	
	return 0
	
}


function execMigrationScript
{
    typeset funcName="execMigrationScript"
    
	#exp data
	exp_flag=`cat ${EXECUTE_EXP_DIR}/${exp_file} | grep "tables" | cut -d "=" -f 2`
	
	if [ ! -z "${exp_flag}" ]
		then
			printLog "Info" "Begin export ${DB_SOURCE_USER}'s data..."
			exp parfile=${EXECUTE_EXP_DIR}/${exp_file} 
			checkLog ${EXECUTE_EXP_LOGS_DIR}/${exp_log} "Export terminated successfully without warnings."
	
			if [ $? -ne 0 ]
				then 
					printLog "ERROR" "Export failed, please check logfile: ${EXECUTE_EXP_LOGS_DIR}/${exp_log}"
					exit 1
			fi
			printLog "Info" "End export ${DB_SOURCE_USER}'s data successfully..."
			printLog "Info" "Details..."
			cat ${EXECUTE_EXP_LOGS_DIR}/${exp_log} | grep "^. ." >>${LOG_FILE}
	fi
	
	checkSqlLog
	
	if [ $? -ne 0 ]
		then 
			exit 1
	fi
	
	
	return 0
	
	
}


function checkLog
{
	typeset funcName="execMigrationScript"
	
	if [ $# -ne 2 ]
		then
			printLog "ERROR" "Parameters number is invalid"
			exit 1
	fi
	
	logfile="$1"
	success_flag="$2"
	
    success=`cat ${logfile}|grep "${success_flag}"`
    if [ ! -z "${success}" ] 
		then
			printLog "Info" "${success_flag}"
			return 0
	fi
	
	return 1
}

function checkSqlLog
{
	typeset existORAInfo=$(cat ${EXECUTE_SQL_LOGS_DIR}/${sql_file}.log |grep -Ew "^ORA-[0-9]+:")
	
	if [ ! -z "${existORAInfo}" ]
		then
			printLog "Error" "Delete failed, to see details: ${EXECUTE_SQL_LOGS_DIR}/${sql_file}.log..."
			exit 1
	else
		printLog "Info" "Delete successfully..."
		return 0
	fi
}

function main
{
	typeset funcName="main"
    printLog "Info" "Begin to execute data migration task: batchnum ${BATCH_NUM}..."
    prepareEnv
    prepareMigrationScript
	
	if [ $? != 0 ]
		then 
			printLog "ERROR" "prepareMigrationScript failed..."
			exit 1;
	fi
	
    execMigrationScript
	
	if [ $? != 0 ]
		then 
			printLog "ERROR" "execMigrationScript failed..."
			exit 1;
	fi
	
	printLog "Info" "End to execute data migration task: batchnum ${BATCH_NUM} successfully..."
}
main




