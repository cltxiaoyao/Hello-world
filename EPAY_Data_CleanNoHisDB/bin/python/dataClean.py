# -*- coding:utf-8 -*-
import os
import xmlUtil
import sys
def data_clean(path, batch_num):
    if not os.path.isdir(path):
        return False
    configPath = GetConfigFile(path)
    task_list = xmlUtil.get_xml_data(configPath)
    assemble_sql(task_list, path, batch_num)
    
def assemble_sql(task_list, path, batch_num):
    for task in task_list:
        batchnum = task['batchNum']
        if not int(batchnum) == int(batch_num):
            continue
        dbUser = task['dbUser']
        file_name = dbUser + "_1_" + batchnum + ".sql"
        dir_path = os.path.join(path, dbUser)
        create_dir(dir_path)
        file_path = os.path.join(dir_path, file_name)
        create_file(file_path, task)

        
def create_dir(dir_path):
    if not os.path.isdir(dir_path):
        try:
            os.mkdir(dir_path)
        except:
            print "mkdir error."
            sys.exit(1)
        
def create_file(file_path, task):
    tables = task['tables']
    if tables == None:
        return False
    try: 
        fpCreate = False   
        for table in tables:
            if fpCreate == False:
                fp = open(file_path, 'w')
                fp.write("set serveroutput on\n")
                fpCreate = True
            tableName = table['tableName']
            tableNameHis = table['dstTaleName']
            commitTotalNum = table['commitTotalNum']
            commitSingleNum = table['commitSingleNum']
            condition = get_condition(table)
            sql = "DECLARE\n  type cursor_type is ref cursor;\n  cur cursor_type;\n  type rowid_table_type is \
table of rowid index by pls_integer;\n  v_rowid         rowid_table_type;\n  v_sql_getrowid  varchar2(2000);\
\n  v_sql_insert_his  varchar2(4000);\n  nums integer := 0;\n  startTime varchar2(100);\n  endTime varchar2(100);\nBEGIN\n  \
startTime := to_char(sysdate,'YYYY-MM-DD HH:MI:SS');\n  v_sql_getrowid   := 'SELECT ROWID FROM " + \
tableName + " WHERE " + condition + " AND rownum <= " + commitTotalNum + "';\n  v_sql_insert_his := 'begin DELETE FROM "\
  + tableName + " WHERE rowid = :1;end;';\n  open cur for v_sql_getrowid;\n  loop\n    fetch cur bulk collect\n      into v_rowid \
  limit " + commitSingleNum + ";\n    nums := v_rowid.count + nums;\n    exit when  v_rowid.count = 0;\n    forall i in v_rowid.first .. v_rowid.last\n\
        execute immediate v_sql_insert_his using v_rowid(i);\n   commit;\n  end loop;\n  close cur;\n  endTime := to_char(sysdate,'YYYY-MM-DD HH:MI:SS'\
);\nDBMS_OUTPUT.PUT_LINE('-----task " + task['batchNum'] + ": " + task['dbUser'] + " data clean-------');\nDBMS_OUTPUT.PUT_LINE('startTime:' || startTime ||', endTime:' || \
 endTime || ' sourceTable:" + tableName + ", TargetTable:" + tableNameHis + ", transferRows:' || nums);\nEND;\n/\n"
            fp.write(sql)
        if fpCreate == True:
            fp.close()
    except IOError:
        print "Create file error."
        sys.exit(1)
    except:
        print "Method create_file(file_path, tables) error."
        sys.exit(1)
    
def get_condition(table):
    statusField = table['statusField']
    status = table['status']
    expDateField = table['expDateField']
    expDate = table['expDate']
    dateUnit = table['dateUnit']
    if expDateField == None or expDateField=="" or expDate == None or expDate =="":
        print "expDateField,expDate config error"
        sys.exit(1)
        
    if (dateUnit == "Y"):
        dateSql = "add_months(sysdate,-" + expDate + "*12)"
    elif dateUnit == "M":
        dateSql = "add_months(sysdate,-" + expDate + ")"
    elif dateUnit == "D":
        dateSql = "sysdate - " + expDate
    elif dateUnit == "H":
        dateSql = "sysdate - " + expDate + "/24"
    else:
        print "dateUnit config error"
        sys.exit(1)
    if not(statusField is None or statusField == "") and not(status is None or status == ""):
        condition = statusField + " = " + status + " and " + expDateField + " <= " + dateSql
    else:
        condition = expDateField + " <= " + dateSql
    return condition

def GetConfigFile(strPath):  
    if not strPath:
        print "strPath is None"
        sys.exit(1) 
    try:    
        lsPath = os.path.split(strPath);  
        lsPath = os.path.split(lsPath[0]); 
        lsPath = os.path.split(lsPath[0]); 
        lsPath = os.path.join(lsPath[0],"conf")
        return os.path.join(lsPath,"datacleanTableConfig.xml"); 
    except:
        print "Method GetConfigPath(%s) error"%strPath
        sys.exit(1)

if __name__ == "__main__":
    #data_clean("d:\\",2)
    data_clean(sys.argv[1], sys.argv[2])
