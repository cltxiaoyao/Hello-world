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


fileName="run.sh"

BATCH_NUM=3
CLEAN_RESULT="true"
#Query The Current Execute User
Current_Execute_User=`whoami`
Current_Execute_UserHome=$(cat /etc/passwd |grep -Ew "^${Current_Execute_User}" |awk -F: '{print $6}')
if [ -f "${Current_Execute_UserHome}/.profile" ]; then 
    . ${Current_Execute_UserHome}/.profile
else
    echo "File ${Current_Execute_UserHome}/.profile does not exist."
fi

Parameters_Numbers="$#"

DataSOURCE_FILE=${SCRIPT_DIR}/conf/datasource.properties
TableConfig_FILE=${SCRIPT_DIR}/conf/datacleanTableConfig.xml


EXECUTE_BASE_DIR="${SCRIPT_DIR}/ExecuteResult"
EXECUTE_RESULT_DIR="${SCRIPT_DIR}/ExecuteResult/${BATCH_NUM}"
EXECUTE_EXP_LOGS_DIR="${EXECUTE_RESULT_DIR}/logs/ExecuteExpLogs"
EXECUTE_IMP_LOGS_DIR="${EXECUTE_RESULT_DIR}/logs/ExecuteImpLogs"
EXECUTE_SQL_LOGS_DIR="${EXECUTE_RESULT_DIR}/logs/ExecuteSqlLogs"
EXECUTE_ERROR_LOGS_DIR="${EXECUTE_RESULT_DIR}/logs/errorLogs"
UNEXECUTE_SCRIPT_DIR="${EXECUTE_RESULT_DIR}/unexecuteFile"
EXECUTE_EXP_DIR="${EXECUTE_RESULT_DIR}/expPars"
EXECUTE_IMP_DIR="${EXECUTE_RESULT_DIR}/impPars"
EXECUTE_SQL_DIR="${EXECUTE_RESULT_DIR}/sqls"
EXECUTE_DUMP_DIR="${EXECUTE_RESULT_DIR}/dumps"
LOG_FILE=${EXECUTE_RESULT_DIR}/run${BATCH_NUM}.log
MIGRATION_LOG_FILE=${EXECUTE_RESULT_DIR}/migration${BATCH_NUM}.log


function printLog
{
    typeset localTime=`date "+%F %T"`
    if [ $# -ne 2 ]; then
        echo "[${localTime}]" "$*" |tee -a "${LOG_FILE}"
    else
        # Log Level: INFO/WARN/ERROR
        typeset logLevel=`echo "$1" |tr "a-z" "A-Z"`
        
        typeset logMessage="$2"
        echo "[${localTime}] [${logLevel}] ${logMessage}" |tee -a  "${LOG_FILE}"
    fi
}

function prepareEnv
{
   if [ ! -f ${DataSOURCE_FILE} -o ! -f ${TableConfig_FILE} ];then
       echo "[ERROR] ${DataSOURCE_FILE} or ${TableConfig_FILE} may do not exist!"
       exit 1
   fi
      
   #includeBatch number
    EXECUTE_RESULT_DIR="${SCRIPT_DIR}/ExecuteResult/${BATCH_NUM}"
    if [ -d "${EXECUTE_RESULT_DIR}" ]; then
        find ${EXECUTE_RESULT_DIR} -type f |xargs rm -f >/dev/null 2>&1
    fi
    rm -rf ${EXECUTE_RESULT_DIR} >/dev/null 2>&1
    mkdir -p ${EXECUTE_RESULT_DIR}

    EXECUTE_LOGS_DIR="${EXECUTE_RESULT_DIR}/ExecuteLogs"
    rm -rf ${EXECUTE_LOGS_DIR}
    mkdir -p ${EXECUTE_LOGS_DIR}
    
    
    EXECUTE_ERROR_LOGS_DIR="${EXECUTE_RESULT_DIR}/ErrorLogs"
    rm -rf ${EXECUTE_ERROR_LOGS_DIR}
    mkdir -p ${EXECUTE_ERROR_LOGS_DIR}
    
    UNEXECUTE_SCRIPT_DIR="${EXECUTE_RESULT_DIR}/ErrorSqls"
    rm -rf ${UNEXECUTE_SCRIPT_DIR}
    mkdir -p ${UNEXECUTE_SCRIPT_DIR}
   
    EXECUTE_SQLS_DIR="${EXECUTE_RESULT_DIR}/SQLFiles"
    mkdir -p ${EXECUTE_SQLS_DIR}
    
    LOG_FILE=${EXECUTE_RESULT_DIR}/run${BATCH_NUM}.log
    [  -f ${LOG_FILE} ] && rm -rf ${LOG_FILE} && touch ${LOG_FILE}
    
    LOG_DIR=${SCRIPT_DIR}/logs
    [ ! -d  ${LOG_DIR} ] && mkdir -p ${LOG_DIR}
    
    LOG_DIR_CLEAN_RESULTS_LOG_DIR=${LOG_DIR}/cleanResultsLogs/${BATCH_NUM}
    [ ! -d  ${LOG_DIR_CLEAN_RESULTS_LOG_DIR} ] && mkdir -p ${LOG_DIR_CLEAN_RESULTS_LOG_DIR}
    
    LOG_DIR_RUN_LOG_DIR=${LOG_DIR}/runLogs/${BATCH_NUM}
    [ ! -d  ${LOG_DIR_RUN_LOG_DIR} ] && mkdir -p ${LOG_DIR_RUN_LOG_DIR} 
    
}

#invoke python script to generate migration files:par and sqlfile
function generateMigrationFiles
{
    typeset funcName="generateMigrationFiles"
    typeset DATA_CLEAN_PY=${SCRIPT_DIR}/bin/python/dataMigration.py

    printLog "Info" "Begin to invoke dataMigration.py to generate par and sql files."
   
	DB_SOURCE_TNS=`cat ${DataSOURCE_FILE} | grep "_database_tnsname" |head -1 | awk -F "=" '{print $2}'`
    python "${DATA_CLEAN_PY}"  "${EXECUTE_BASE_DIR}" "${BATCH_NUM}" "${DB_SOURCE_TNS}"
        
    if [ $? -ne 0 ]; then
        printLog "Error"  "Generate par and sql files failed."
        exit 1
    else
        printLog "Info"  "Generate par and sql files successfully."
        
    fi
    
	typeset expParFileNumber=`find ${EXECUTE_EXP_DIR} -type f -name "*.par" |wc -l`
    
    echo ${EXECUTE_EXP_DIR};
    echo "find ${EXECUTE_EXP_DIR} -type f -name \"*.par\" |wc -l";
    echo $expParFileNumber;
    if [ $expParFileNumber -ne 1 ];then
			printLog "Error"  "Generate migration files failed, exp file number is not correct."
			#printLog "info" "Generate files null, the table or parition is not exsit."
			#exit 1
	fi
	
	typeset impParFileNumber=`find ${EXECUTE_IMP_DIR} -type f -name "*.par" |wc -l`
	if [ $impParFileNumber -ne 1 ];then
			printLog "Error"  "Generate migration files failed, imp file number is not correct."
			#printLog "info" "Generate files null, the table or parition is not exsit."
			#exit 1
	fi
	
	#typeset sqlFileNumber=`find ${EXECUTE_SQL_DIR} -type f -name "*.sql" |wc -l`
	#if [ $sqlFileNumber -ne 1 ];then
	#		printLog "Error"  "Generate migration files failed, sql file number is not correct."
			#printLog "info" "Generate files null, the table or parition is not exsit."
			#exit 1
	#fi
	
    return 0
}

function batchMigration
{
    typeset funcName="batchMigration"
    
    DB_MIGRATION_SCRIPT="${SCRIPT_DIR}/bin/dbMigration.sh"
    
	dbuser=`ls -l ${EXECUTE_EXP_DIR}|grep '^d'|awk -F " " '{print $NF}'`
    printLog "Info" "The migration scripts of follow user will be executed: ${dbuser}"
    
    typeset startTimer=$(date "+%s")
    printLog "============================================================="
    printLog "INFO" "Begin to execute ${dbuser}..."
    
    nohup sh ${DB_MIGRATION_SCRIPT} "${dbuser}"  "BATCH_NUM${BATCH_NUM}" "${EXECUTE_RESULT_DIR}" >/dev/null 2>&1 &

    
    typeset countTime=1
  
    typeset parallelExecFinished=$(ps -ef |grep -w "${DB_MIGRATION_SCRIPT}" |grep -w "BATCH_NUM${BATCH_NUM}"|grep -v "grep" |wc -l)
    
    while [ ${parallelExecFinished} -ne 0 ] 
    do
        typeset printLogFlag=$(expr ${countTime} \% 10)
        if [ ${printLogFlag} -eq 0 ]; then
            typeset executingModuleList=$(ps -ef |grep -w "${DB_MIGRATION_SCRIPT}" |grep -w "BATCH_NUM${BATCH_NUM}"|grep -v "grep" |awk '{print $(NF-2)}')
            typeset tempExecutingModuleList=`echo ${executingModuleList}`
            printLog "INFO" "Executing [${tempExecutingModuleList}] ..."
        fi
        sleep 1
        countTime=$(expr ${countTime} + 1)
        parallelExecFinished=$(ps -ef |grep -w "${DB_MIGRATION_SCRIPT}"|grep -w "BATCH_NUM${BATCH_NUM}" |grep -v "grep" |wc -l)
    done
    
    typeset endTimer=$(date "+%s")
    typeset usedTime=$(expr ${endTimer} - ${startTimer})
    #printLog "INFO" "Executing batch migration clean  task finished, costs ${usedTime} seconds."
    printLog "============================================================="
    #printLog ""	
	
}

#invoke python script to generate sql files
function generateSQLFiles
{
    typeset funcName="generateSQLFiles"
    typeset DATA_CLEAN_PY=${SCRIPT_DIR}/bin/python/dataClean.py

    printLog "Info"  "Begin to invoke dataClean.py to generate sqlfiles."

    python "${DATA_CLEAN_PY}"  "${EXECUTE_SQLS_DIR}" "${BATCH_NUM}" 
        
    if [ $? -ne 0 ]; then
        printLog "Error"  "Generate sql files failed."
        exit 1
    else
        printLog "Info" "Generate sql files successfully."        
    fi
    return 0
}

function batchClean
{
    typeset funcName="batchClean"
    
    DB_BATCH_EXEC_SCRIPT="${SCRIPT_DIR}/bin/dbBatchExec.sh"
    [ ! -f ${DB_BATCH_EXEC_SCRIPT} ] && printLog "Error" "The scripts does not exist! ${DB_BATCH_EXEC_SCRIPT}" && exit 1
    
    dbuser=`ls -l ${EXECUTE_SQLS_DIR}|grep '^d'|awk -F " " '{print $NF}'`
    printLog "Info" "The scripts of follow user will be executed: ${dbuser}"
    
    typeset startTimer=$(date "+%s")
    printLog "============================================================="
    printLog "INFO" "Begin to execute ${dbuser}..."
    
    nohup sh ${DB_BATCH_EXEC_SCRIPT} "${dbuser}" "BATCH_NUM${BATCH_NUM}" "${EXECUTE_SQLS_DIR}"  "${EXECUTE_LOGS_DIR}" "${EXECUTE_ERROR_LOGS_DIR}" "${UNEXECUTE_SCRIPT_DIR}" >/dev/null 2>&1 &
    
    typeset countTime=1
  
    typeset parallelExecFinished=$(ps -ef |grep -w "${DB_BATCH_EXEC_SCRIPT}" |grep -w "BATCH_NUM${BATCH_NUM}"|grep -v "grep" |wc -l)
    
    while [ ${parallelExecFinished} -ne 0 ] 
    do
        typeset printLogFlag=$(expr ${countTime} \% 10)
        if [ ${printLogFlag} -eq 0 ]; then
            typeset executingModuleList=$(ps -ef |grep -w "${DB_BATCH_EXEC_SCRIPT}" |grep -w "BATCH_NUM${BATCH_NUM}"|grep -v "grep" |awk '{print $(NF-5)}')
            typeset tempExecutingModuleList=`echo ${executingModuleList}`
            printLog "INFO" "Executing [${tempExecutingModuleList}] ..."
        fi
        sleep 1
        countTime=$(expr ${countTime} + 1)
        parallelExecFinished=$(ps -ef |grep -w "${DB_BATCH_EXEC_SCRIPT}"|grep -w "BATCH_NUM${BATCH_NUM}" |grep -v "grep" |wc -l)
    done
    
    typeset endTimer=$(date "+%s")
    typeset usedTime=$(expr ${endTimer} - ${startTimer})
    printLog "INFO" "Executing batch clean finished, costs ${usedTime} seconds."
    printLog "============================================================="
    printLog ""
}
function cleanBackUpfiles
{
    typeset funcName="cleanBackUpfiles"

    fileNum=`ls -lrt ${LOG_DIR_CLEAN_RESULTS_LOG_DIR} |grep -E "dataclean_${BATCH_NUM}.*.log"|wc -l`
    if [ ${fileNum} -gt 19 ];then
        typeset -i deleteFileNum=`expr ${fileNum} \- 19`
        fileList=`ls -lrt ${LOG_DIR_CLEAN_RESULTS_LOG_DIR}|grep -E "dataclean_${BATCH_NUM}.*.log"|head -${deleteFileNum}|awk -F " " '{print $NF}'`
        
        for fileDelete in ${fileList}
        do
            rm -f ${LOG_DIR_CLEAN_RESULTS_LOG_DIR}/${fileDelete}
        done     
    
    fi
    
    
}

function cleanBackUpDir
{
    typeset funcName="cleanBackUpDir"

    dirNum=`ls -lrt ${LOG_DIR_RUN_LOG_DIR} |grep '^d'|wc -l`
    if [ ${dirNum} -gt 19 ];then
        typeset -i deleteFileNum=`expr ${dirNum} \- 19`
        dirList=`ls -lrt ${LOG_DIR_RUN_LOG_DIR}|grep '^d'|head -${deleteFileNum}|awk -F " " '{print $NF}'`
        
        for dirDelete in ${dirList}
        do
            rm -rf ${LOG_DIR_RUN_LOG_DIR}/${dirDelete}
        done     
    
    fi
}

function outputResult
{
     printLog "=======================Clean Result========================="
     
     Module_EXECUTE_ERROR_LOGS_DIR="${EXECUTE_ERROR_LOGS_DIR}/${dbuser}"
    
     typeset dbExecLogFile=$(ls -l ${Module_EXECUTE_ERROR_LOGS_DIR} |grep -E "*.log$" |awk '{print $NF}')
    
    if [ ! -z "${dbExecLogFile}" ];then
        
        typeset existORAInfo=$(cat ${Module_EXECUTE_ERROR_LOGS_DIR}/${dbExecLogFile} |grep -Ew "^ORA-[0-9]+:")
        if [ ! -z "${existORAInfo}" ];then
            printLog "Error" "Executing dbExecLogFile failed."
            CLEAN_RESULT="false"
        fi
        cat ${Module_EXECUTE_ERROR_LOGS_DIR}/${dbExecLogFile}
    else
        printLog "Error" "There is no log file,the task may failed." 
        CLEAN_RESULT="false"        
    fi
    
    cleanBackUpfiles    
    dateStamp=`date "+%Y%m%d%H%M%S%N"`
    fileDirNum=`echo $dateStamp|cut -b -17`
    
    cp ${Module_EXECUTE_ERROR_LOGS_DIR}/${dbExecLogFile} ${LOG_DIR_CLEAN_RESULTS_LOG_DIR}/dataclean_${BATCH_NUM}_${fileDirNum}.log
    
    printLog "Info" "The log will be saved in ${LOG_DIR_CLEAN_RESULTS_LOG_DIR}/dataclean_${BATCH_NUM}_${fileDirNum}.log"
    
    cleanBackUpDir
    mkdir -p ${LOG_DIR_RUN_LOG_DIR}/${fileDirNum}
    [ $? -ne 0 ] && printLog "Error" "Create file path ${LOG_DIR_RUN_LOG_DIR}/${fileDirNum} failed."
    
    cp -R ${EXECUTE_RESULT_DIR}/* ${LOG_DIR_RUN_LOG_DIR}/${fileDirNum}
     
    printLog "Info" "The process file will be saved in ${LOG_DIR_RUN_LOG_DIR}/${fileDirNum}"
}



function main
{
    prepareEnv
    printLog "=========[NO.${BATCH_NUM}]Batch Clean Task Begin========="
	
	generateMigrationFiles
	batchMigration
    generateSQLFiles
    batchClean
    outputResult
    
    [ "X${CLEAN_RESULT}" != "Xtrue" ] && printLog "Error" "Execute data clean script failed." && exit 1
    printLog "=========[NO.${BATCH_NUM}]Batch Clean Task End========="
}
main




