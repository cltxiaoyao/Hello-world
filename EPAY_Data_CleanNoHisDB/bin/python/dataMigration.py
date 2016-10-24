# -*- coding:utf-8 -*-
import os
import xmlUtil
import sys
import datetime
import calendar 
def data_migration(path, batch_num,tns_name):
    if not os.path.isdir(path):
        return False
    configPath = GetConfigFile(path)
    task_list = xmlUtil.get_xml_data(configPath)
    assemble_file(task_list, path, batch_num,tns_name)

    
def assemble_file(task_list, path, batch_num,tns_name):
    sysdate = datetime.datetime.now()
    for task in task_list:
        batchnum = task['batchNum']
        if not int(batchnum) == int(batch_num):            
            continue
        tables = task['tables']
        dbUser = task['dbUser']
        file_name_exp = dbUser + "_exp" + "_2_" + batchnum + ".par"
        
        file_name_imp = dbUser + "_imp" + "_2_" + batchnum + ".par"
        
        file_name_sql = dbUser + "_clean" + "_2_" + batchnum + ".sql"
        
        dir_path = os.path.join(path, str(batch_num))
        
        create_dir(dir_path)
        
        dir_path_exp = os.path.join(dir_path, "expPars")
        
        dir_path_imp = os.path.join(dir_path, "impPars")
        
        dir_path_sql = os.path.join(dir_path, "sqls")
        
        create_dir(dir_path_exp)
        create_dir(dir_path_imp)
        create_dir(dir_path_sql)
        
        dir_path_exp_user = os.path.join(dir_path_exp, dbUser)
        dir_path_imp_user = os.path.join(dir_path_imp, dbUser)
        dir_path_sql_user = os.path.join(dir_path_sql, dbUser)
        
        create_dir(dir_path_exp_user)
        create_dir(dir_path_imp_user)
        create_dir(dir_path_sql_user)
        
        
        file_path_exp = os.path.join(dir_path_exp_user, file_name_exp)
        file_path_imp = os.path.join(dir_path_imp_user, file_name_imp)
        file_path_sql = os.path.join(dir_path_sql_user, file_name_sql)
        
        create_file(file_path_exp, create_exp_batch_file(task_list, batch_num, sysdate,tns_name))
        create_file(file_path_imp, create_imp_batch_file(task_list, batch_num, sysdate))
        create_file(file_path_sql, create_clean_batch_sql(tables, sysdate))

        

        
def create_dir(dir_path):
    if not os.path.isdir(dir_path):
        try:
            os.mkdir(dir_path)
        except:
            print "mkdir error."
            sys.exit(1)
        
def create_file(file_path, content):
    try:        
        if content == "":
            print "File content is null, not need create file."
            return
        fp = open(file_path, 'w')
        fp.write(content)
        fp.close()
    except IOError:
        print "Create file error."
    except:
        print "Method create_file() error."

def is_useful_partition(user, tableName, partitionName):
    cmd = "sqlplus -S /@" + user + " <<!\n select  PARTITION_NAME \
from USER_TAB_PARTITIONS where TABLE_NAME = '" + tableName + "' and  \
PARTITION_NAME = '" + partitionName + "';\nexit;\n!"
    result = os.popen(cmd).read()
    
    if "no rows selected" in result:
        return False
    return True
    

def create_exp_batch_file(task_list, batch_num, sysdate,tns_name):
    content = "";
    for task in task_list:
        batchnum = task['batchNum']
        if not int(batchnum) == int(batch_num):
            continue
        flag = 0
        tablesContent = ""
        taskList = list(task['tables'])
        
        condition = "";
        for table in taskList:
            if table['cleanType'] == "2":
                if not table['needToMigrate'] is None and table['needToMigrate'] == "no":
                    continue
                flag = flag + 1  
                
                if table['partitionPrefix'] != "": 
                    partitionName = getDateFormat(sysdate, int(table['expDate']), table['dateUnit'], table['partitionPrefix'])
                    if partitionName == "":
                        return content
                    else:
                        if not is_useful_partition(tns_name, table['tableName'], partitionName):
                            task['tables'].remove(table)
                            continue
                        if tablesContent == "":
                            tablesContent = table['tableName'] + ":" + partitionName
                        else:
                            tablesContent = tablesContent + "," + table['tableName'] + ":" + partitionName
                else:
                    condition = get_condition(table);  
                    if condition == "":
                        return content
                    else:
                        tablesContent = table['tableName']
                       

        if tablesContent != "" and flag > 0:
            content = "userid=" + task['dbUser'] + "\n"
            content = content + "log=exp_batchnum_" + batchnum + ".log\n"
            content = content + "file=exp_batchnum_" + batchnum + ".dump\n"
            content = content + "tables=" + tablesContent + "\n"
            if condition != "":
                content = content + "query=\"where " + condition + "\"\n"
            content = content + "buffer=1024000\n"
            content = content + "statistics=none\n"
        return content
    return content

def create_imp_batch_file(task_list, batch_num, sysdate):
    content = "";
    for task in task_list:
        batchnum = task['batchNum']
        if not int(batchnum) == int(batch_num):
            continue
        flag = 0
        tablesContent = "";
        
        for table in task['tables']:
            if table['cleanType'] != "2":
                continue
            if not table['needToMigrate'] is None and table['needToMigrate'] == "no":
                continue
            flag = flag + 1   
            
            if table['partitionPrefix'] != "":  
                partitionName = getDateFormat(sysdate, int(table['expDate']), table['dateUnit'], table['partitionPrefix'])
                
                if partitionName == "":
                    return ""
                else:
                    if tablesContent == "":
                        tablesContent = table['tableName'] + ":" + partitionName
                    else:
                        tablesContent = tablesContent + "," + table['tableName'] + ":" + partitionName 
            else:
                tablesContent = table['tableName']                           
        if tablesContent != "" and flag > 0:
            content = "userid=" + task['dbUser'] + "\n"
            content = content + "fromuser=" + task['dbUser'] + "\n"
            content = content + "touser=" + task['dbUser'] + "\n"
            content = content + "log=imp_batchnum_" + batchnum + ".log\n"
            content = content + "file=exp_batchnum_" + batchnum + ".dump\n"
            content = content + "tables=" + tablesContent + "\n"
            content = content + "buffer=1024000\n"
            content = content + "ignore=y\n"
            content = content + "commit=y\n"
        return content;
    return content;

def create_clean_batch_sql(tables, sysdate):
    content = ""
    if tables == None:
        return content
    for table in tables:
            if int(table['cleanType']) != 2:
                continue
            if table['partitionPrefix'] != "":
                partitionName = getDateFormat(sysdate, int(table['expDate']), table['dateUnit'], table['partitionPrefix'])
                if partitionName == "":
                    return content
                
                content = content + "alter table " + table['tableName'] + " truncate partition " + partitionName + ";\n"
            else:
                content = content + "select sysdate from dual;"
 
    return content   

def GetConfigFile(strPath):
    if not strPath:
        print "strPath is None"
        sys.exit(1)
    try:
        lsPath = os.path.split(strPath);
        #lsPath = os.path.split(lsPath[0]);
        lsPath = os.path.join(lsPath[0], "conf")
        return os.path.join(lsPath, "datacleanTableConfig.xml");
    except:
        print "Method GetConfigPath(%s) error" % strPath
        sys.exit(1)

    

def getDateFormat(sysdate, expireDate, dateUnit, partitionFormat):
    if partitionFormat == "":
        return ""
    
    partition_prefix = partitionFormat.split('[')[0]
    partition_template = (partitionFormat.split('[')[1]).split(']')[0]
    
    todaymonths = get_today_months(-expireDate - 1);
    
    if dateUnit == "M":
        year_full = int(todaymonths[0:4])
        month = int(todaymonths[5:7])
        
        if month < 10:
            result_full = str(year_full) + '0' + str(month)
            result = str(year_full)[2:4] + '0' + str(month)
        else:
            result_full = str(year_full) + str(month)
            result = str(year_full)[2:4] + str(month)
        
        if partition_template == "YYYYMM":
            return partition_prefix + result_full
        if partition_template == "YYMM":
            return partition_prefix + result
        return ""   
        
    elif dateUnit == "D":
        sysdate_before = sysdate + datetime.timedelta(days= -expireDate - 1)
        year_full = str(sysdate_before)[0:4]
        year = str(sysdate_before)[2:4]
        month = str(sysdate_before)[5:7]
        day = str(sysdate_before)[8:10]
        
        if partition_template == "YYYYMMDD":
            return partition_prefix + year_full + month + day
        
        if partition_template == "YYMMDD":
            return partition_prefix + year + month + day
        
        if partition_template == "MMDD":
            return partition_prefix + month + day
        
        return ""
    else:
        print "Expire date unit is invalid."
        return ""


def get_condition(table):
    expDateField = table['expDateField']
    expDate = table['expDate']
    dateUnit = table['dateUnit']
    if (dateUnit == "Y"):
        dateSql = "add_months(sysdate,-" + expDate + "*12)"
    elif dateUnit == "M":
        dateSql = "add_months(sysdate,-" + expDate + ")"
    elif dateUnit == "D":
        dateSql = "sysdate - " + expDate
    elif dateUnit == "H":
        dateSql = "sysdate - " + expDate + "/24"
    else:
        return None
    
    condition = expDateField + " <= " + dateSql
    
    return condition
def get_now_time():
    now = datetime.datetime.now()
    thisyear = int(now.year)
    thismon = int(now.month)
    thisday = int(now.day)
    thishour = int(now.hour)
    thisminute = int(now.minute)
    thissecond = int(now.second)
    return thisyear, thismon, thisday, thishour, thisminute, thissecond

def get_year_and_month(n=0): 
    now = datetime.datetime.now()
    thisyear, thismon, thisday, thishour, thisminute, thissecond = get_now_time()
    totalmon = thismon + n

    if(n >= 0): 
        if(totalmon <= 12): 
            days = str(get_days_of_month(thisyear, totalmon)) 
            totalmon = add_zero(totalmon) 
            return (thisyear, totalmon, days, thishour, thisminute, thissecond, thisday) 
        else: 
            i = totalmon / 12 
            j = totalmon % 12 
            if(j == 0): 
                i -= 1 
                j = 12 
            thisyear += i 
            days = str(get_days_of_month(thisyear, j)) 
            j = add_zero(j) 
            return (str(thisyear), str(j), days, thishour, thisminute, thissecond, thisday) 
    else: 
        if((totalmon > 0) and (totalmon < 12)): 
            days = str(get_days_of_month(thisyear, totalmon)) 
            totalmon = add_zero(totalmon) 
            return (thisyear, totalmon, days, thishour, thisminute, thissecond, thisday) 
        else: 
            i = totalmon / 12 
            j = totalmon % 12 
            if(j == 0): 
                i -= 1 
                j = 12 
            thisyear += i 
            days = str(get_days_of_month(thisyear, j)) 
            j = add_zero(j) 
            return (str(thisyear), str(j), days, thishour, thisminute, thissecond, thisday) 

def get_days_of_month(year, mon): 
    return calendar.monthrange(year, mon)[1] 

def add_zero(n): 
    nabs = abs(int(n)) 
    if(nabs < 10): 
        return "0" + str(nabs) 
    else: 
        return nabs

def get_today_months(n=0): 
    year, mon, d, hour, minute, second, day = get_year_and_month(n)
    arr = (year, mon, d, hour, minute, second, day)
    
    if(int(day) < int(d)):
        arr = (year, mon, day, hour, minute, second)
    return "-".join("%s" % i for i in arr)

if __name__ == "__main__":
    #data_migration("d:\\", 2,"")
    data_migration(sys.argv[1], sys.argv[2],sys.argv[3] )

    
