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
fileName="dbBatchExec.sh"

#Query The Current Execute User
Current_Execute_User=`whoami`
Parameters_Numbers="$#"

echo Current_Execute_User$Current_Execute_User
FILE_NAME="dbBatchExec.sh"
if [ ${Parameters_Numbers} -ne 6 ];then
    echo "Error" " ${fileName} Parameters shoule be six"
fi

DataSOURCE_FILE=${APP_PATH}/conf/datasource.properties

DB_USER="$1"
BATCH_NUM_TEMP="$2"
EXCUTE_SQLS_DIR="$3"
EXCUTE_LOGS_DIR="$4"
EXCUTE_ERROR_LOGS_DIR="$5"
UNEXCUTE_SCRIPT_DIR="$6"

BATCH_NUM=`echo ${BATCH_NUM_TEMP}|awk -F "BATCH_NUM" '{print $2}'`
fileName="dbBatchExec.sh"

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

function prepareEnv
{
    SUCCESS_NUM=0
    EXCEPTION_NUM=0
    


    Module_EXCUTE_SQL_DIR="${EXCUTE_SQLS_DIR}/${DB_USER}"
    
    Module_EXCUTE_LOGS_DIR="${EXCUTE_LOGS_DIR}/${DB_USER}"
    mkdir -p ${Module_EXCUTE_LOGS_DIR}
    
    [ $? -ne 0 ] && printLog "Info" "${fileName}" "Create ${Module_EXCUTE_LOGS_DIR} failed" && exit 1
    
    LOG_FILE="${Module_EXCUTE_LOGS_DIR}/${FILE_NAME}_${DB_USER}.log"
    echo "" > ${LOG_FILE}
    
    Module_EXCUTE_ERROR_LOGS_DIR="${EXCUTE_ERROR_LOGS_DIR}/${DB_USER}"
    mkdir -p ${Module_EXCUTE_ERROR_LOGS_DIR}
    
    Module_UNEXCUTE_SCRIPT_DIR="${UNEXCUTE_SCRIPT_DIR}/${DB_USER}"
    mkdir -p ${Module_UNEXCUTE_SCRIPT_DIR}
      
    
}

function prepareCleanScript
{
    typeset funcName="prepareCleanScript"
    
    typeset sqlFileNums=`find ${Module_EXCUTE_SQL_DIR} -name "*.sql" |wc -l`
    
    printLog "Info" "${funcName}" "${DB_USER}:There are ${sqlFileNums} sql files need to be executed"
    
    #Module_BATCH_EXEC_SQL="${Module_EXCUTE_LOGS_DIR}/dfx_batch_${DB_USER}.sql"
    #echo > ${Module_BATCH_EXEC_SQL}
    #
    #typeset allSqlFiles=`ls -l ${Module_EXCUTE_SQL_DIR} |grep -E "*.sql$" |awk '{print $NF}'`
    #for sqlFile in $allSqlFiles
    #do
    #    printLog "INFO" "Add ${sqlFile} to ${Module_BATCH_EXEC_SQL}..."
    #    echo "spool ${Module_EXCUTE_ERROR_LOGS_DIR}/${sqlFile}.log" >> ${Module_BATCH_EXEC_SQL}
    #    echo "@@${Module_EXCUTE_SQL_DIR}/${sqlFile}" >> ${Module_BATCH_EXEC_SQL}
    #    echo "spool off;" >> ${Module_BATCH_EXEC_SQL}
    #done
}

function checkUserConnect
{
    typeset funcName="checkUserConnect"
    [[ $# -ne 1 ]] && printLog "ERROR" "dbBatchExec.sh:Input parameters numbers must be one, ${funcName} dbtnsname: $1" && exit 1
    
    typeset tnsname="$1"
    
    typeset result=$(sqlplus /@"${tnsname}" <<EOF
    set echo on
    set feedback off
    set heading off
    exit;
EOF)

    typeset connectStatus=`echo "${result}"|grep -w "Connected" |wc -l`  

    if [ ${connectStatus} -ne 1 ];then
        printLog "ERROR" "Connect database failed by TNSNAME:${tnsname}, please check it."        
        return 1
    fi
     printLog "Info" "Connect database successfully by TNSNAME:${tnsname}."
    return 0
}

function decryptKeyTool
{
    typeset funcName="decryptKeyTool"    
    
    typeset passwd="$1"
    cd ${APP_PATH}/lib
    typeset pwdTemp=`java -cp BesDecryptKeyTool.jar com.huawei.bes.common.encryption.Execute "0" "${passwd}"`
    database_db_user_password=`echo "${pwdTemp}"|sed 's/^[[:space:]]*//g' |sed 's/[[:space:]]*$//g'`
    
    [  -z "${database_db_user_password}" ] && printLog "ERROR" "Decrypt passwd failed." && return 1
    
    return 0
}

function execCleanScript
{
    typeset funcName="execCleanScript"
    
    typeset database_tnsname=`cat ${DataSOURCE_FILE} |grep -Ewi "^[[:space:]]*${DB_USER}_database_tnsname" |head -1 |cut -d = -f 2- |sed 's/^[[:space:]]*//g' |sed 's/[[:space:]]*$//g'`
    
    
    if [ -z "${database_tnsname}" ];then
            printLog "ERROR" "The information of database tns is empty."
            find ${Module_EXCUTE_SQL_DIR} -name "*.sql" |xargs -i cp {} ${Module_UNEXCUTE_SCRIPT_DIR}
            return 1
    fi
    
    checkUserConnect "${database_tnsname}"
    
    if [ $? -ne 0 ];then
        printLog "ERROR" "TNSNAME [${database_tnsname}] can not connect to database,scripts can not be executed."
        find ${Module_EXCUTE_SQL_DIR} -name "*.sql" |xargs -i cp {} ${Module_UNEXCUTE_SCRIPT_DIR}
        exit 1
    fi

    
    typeset sqlFile=`ls -l ${Module_EXCUTE_SQL_DIR} |grep -E "*.sql$" |awk '{print $NF}'`
    #for sqlFile in $allSqlFiles
    #do
    #    printLog "INFO" "Add ${sqlFile} to ${Module_BATCH_EXEC_SQL}..."
    #    echo "spool ${Module_EXCUTE_ERROR_LOGS_DIR}/${sqlFile}.log" >> ${Module_BATCH_EXEC_SQL}
    #    echo "@@${Module_EXCUTE_SQL_DIR}/${sqlFile}" >> ${Module_BATCH_EXEC_SQL}
    #    echo "spool off;" >> ${Module_BATCH_EXEC_SQL}
    #done
    
    sqlplus -s /@"${database_tnsname}" >${Module_EXCUTE_ERROR_LOGS_DIR}/${sqlFile}.log 2>&1 <<EOF
        set echo on
        @${Module_EXCUTE_SQL_DIR}/${sqlFile}
        exit;
EOF
}

function checkExecDBResult
{
    typeset funcName="checkExecDBResult"
    
    typeset totalFilesNum=$(find ${Module_EXCUTE_SQL_DIR} -type f -name "*.sql"|wc -l)

    typeset dbExecLogFiles=$(ls -l ${Module_EXCUTE_ERROR_LOGS_DIR} |grep -E "*.log$" |awk '{print $NF}')
    
    if [ ! -z "${dbExecLogFiles}" ];then
        typeset dbExecLog=""
        for dbExecLog in ${dbExecLogFiles}
        do
            typeset existORAInfo=$(cat ${Module_EXCUTE_ERROR_LOGS_DIR}/${dbExecLog} |grep -Ew "^ORA-[0-9]+:")
            if [ ! -z "${existORAInfo}" ];then
                EXCEPTION_NUM=$(expr ${EXCEPTION_NUM} + 1)
            else
                SUCCESS_NUM=$(expr ${SUCCESS_NUM} + 1) 
                #rm -f ${Module_EXCUTE_ERROR_LOGS_DIR}/${dbExecLog}              
            fi
        done
    fi
    
}

function main
{
    
    prepareEnv
    prepareCleanScript
    execCleanScript

    #checkExecDBResult
}
main




