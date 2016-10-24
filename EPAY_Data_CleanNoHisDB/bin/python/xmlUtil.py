# -*- coding:utf-8 -*-
"""
* author: 
* Date: 11-11-9
* Time: 13：20
* Desc:
""" 
from  xml.dom import  minidom
import sys

def get_attrvalue(node, attrname):
    return node.getAttribute(attrname) if node else ''

def get_nodevalue(node, index = 0):
    return node.childNodes[index].nodeValue if node else ''

def get_xmlnode(node,name):
    return node.getElementsByTagName(name) if node else []

def xml_to_string(filename='datacleanTableConfig.xml'):
    doc = minidom.parse(filename)
    print(doc)
    return doc.toxml('UTF-8')

def get_xml_data(filename):
    try:
        doc = minidom.parse(filename) 
        root = doc.documentElement

        task_nodes = get_xmlnode(root,'task')
        task_list=[]
        for taskNode in task_nodes: 
            batchNum = get_attrvalue(taskNode,'batchNum') 
            dbUser = get_attrvalue(taskNode,'dbUser')
            task = {}
            task['batchNum'] , task['dbUser'], task['tables'] = (
                                                                 batchNum, dbUser,[])
            table_nodes = get_xmlnode(taskNode,'table')
            for tableNode in table_nodes:
                tableName = get_attrvalue(tableNode,'tableName')
                cleanType = get_attrvalue(tableNode,'cleanType')
                statusField = get_attrvalue(tableNode,'statusField')
                status = get_attrvalue(tableNode,'status')
                expDateField = get_attrvalue(tableNode,'expDateField')
                expDate = get_attrvalue(tableNode,'expDate')
                dateUnit = get_attrvalue(tableNode,'dateUnit')
                commitTotalNum = get_attrvalue(tableNode,'commitTotalNum')
                commitSingleNum = get_attrvalue(tableNode,'commitSingleNum')
                dstTaleName = get_attrvalue(tableNode,'dstTaleName')
                srcCol = get_attrvalue(tableNode,'srcCol')
                dstCol = get_attrvalue(tableNode,'dstCol')
                partitionPrefix = get_attrvalue(tableNode,'partitionPrefix')
                needToMigrate = get_attrvalue(tableNode,'needToMigrate')
                table = {}
                table['tableName'],table['dstTaleName'],table['cleanType'],table['statusField'],table['status'],\
                table['expDateField'],table['expDate'],table['dateUnit'],table['commitTotalNum'],\
                table['commitSingleNum'], table['srcCol'], table['dstCol'], table['partitionPrefix'], table['needToMigrate'] = (tableName,dstTaleName,\
                                     cleanType, statusField, status, expDateField, \
                                     expDate, dateUnit, commitTotalNum, commitSingleNum, srcCol, dstCol, partitionPrefix, needToMigrate)
                task["tables"].append(table)
            task_list.append(task)
        
        return task_list

    except:
            print "get_xml_data() error,config file path: %s"%filename
            sys.exit(1)

