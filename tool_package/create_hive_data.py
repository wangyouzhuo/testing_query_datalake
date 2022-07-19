import random
#from pyhive import hive
from random import choice
import numpy as np
import re
from random import seed, randint
from datetime import datetime
import time
from conf.CONFIG import *
import string



# HIVE_DATATYPE = ['INT','BIGINT',
#                  'TIMESTAMP',
#                   'STRING',
#                   'VARCHAR(20)',
#                   'CHAR(20)',
#                  'DOUBLE','FLOAT']
#
# HIVE_PARTITION_DATETYPE = ['INT','BIGINT','STRING']
#
# FILE_FORMAT = ['ORC','CSV','PARQUET','TEXTFILE']
#
# STRING_SET = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']

# def connect_hive():
#     try:
#         conn = hive.connect(host="127.0.0.1", port=9083, database="default", username="root")
#         cursor = conn.cursor()
#         cursor.execute("show databases")
#         res = cursor.fetchall()
#         conn.close()
#         for item in res:
#             print(item)
#     except Exception:
#         print('excepion happen')

#----------------------------------------- tool -------------------------------------------------------------

def generate_row_value(datatype_list):
    """

    generate the raw value based on the datatype_list
    For example, input the [int,int,int],it will output the string likw   1,2,3
    then itcan be directly used in INSERT INTO STATEMENT

    :param datatype_list:
    :return:
    """
    new_row = [get_random_value(datatype) for datatype in datatype_list]
    result_str = ''
    for item in datatype_list:
        result_str = result_str + str(get_random_value(item)) + ','
    return result_str.rstrip(','),new_row

def randomDate(start, end):
    """

    :param start:
    :param end:
    :return:
    """
    frmt = '%d-%m-%Y %H:%M:%S'
    stime = time.mktime(time.strptime(start, frmt))
    etime = time.mktime(time.strptime(end, frmt))
    ptime = stime + random.random() * (etime - stime)
    dt = datetime.fromtimestamp(time.mktime(time.localtime(ptime)))
    return dt


def clean(a):
    return re.sub(u"\\(.*?\\)|\\{.*?}|\\[.*?]", "", a)

def random_string_generator(str_size, allowed_chars):
    return ''.join(random.choice(allowed_chars) for x in range(str_size))

def get_random_value(datatype):
    """
    generate any random value for any randome datatype
    :param datatype:
    :return:
    """
    #  ['INT','BIGINT','TIMESTAMP','STRING','VARCHAR(20)','CHAR(20)','DOUBLE','FLOAT']
    if datatype == 'INT':
        return randint(0, 214748364)
    elif datatype == 'BIGINT':
        return randint(0, 214748364)
    elif datatype == 'TIMESTAMP':
        return '"'+str(randomDate("20-01-2018 13:30:00", "24-01-2018 04:50:34"))+'"'
    elif datatype == 'STRING':
        return '"'+random_string_generator(random.randint(1,40),STRING_SET)+'"'
    elif datatype == 'VARCHAR(20)':
        return  '"'+random_string_generator(random.randint(1,20),STRING_SET)+'"'
    elif datatype == 'CHAR(20)':
        return  '"'+random_string_generator(random.randint(1,20),STRING_SET)+'"'
    elif datatype == 'DOUBLE':
        return  random.uniform(1.0, 100.0)
    else:
        return random.uniform(1.0, 100.0)

#----------------------------------------- sql generator -------------------------------------------------------------

def generate_sample_hive_tbl_schema(column_count,random_enabled=True):
    """
    generate the table schema without any partition

    :param column_count: table's column count
    :param random_enabled:
        if enableds,the schema result will not be displayed again;
        else you can generate the same schema based on the same column_count
    :return:
    """
    if random_enabled == True:
        datatype_list = np.random.choice(HIVE_DATATYPE,column_count,replace=True)
        column_name_list = [clean(item)+"_COLUMN_"+str(index) for index,item in enumerate(datatype_list)]
        scheme_str = ''
        for datatype,column_name in zip(datatype_list,column_name_list):
            scheme_str = scheme_str +  str(column_name)  + " " + str(datatype) + ',\n'
        scheme_str = scheme_str.rstrip('\n,')
        return scheme_str,datatype_list
    else:
        np.random.seed(0)
        datatype_list = np.random.choice(HIVE_DATATYPE,column_count,replace=True)
        column_name_list = [clean(item)+"_COLUMN_"+str(index) for index,item in enumerate(datatype_list)]
        scheme_str = ''
        for datatype,column_name in zip(datatype_list,column_name_list):
            scheme_str = scheme_str +  str(column_name)  + " " + str(datatype) + ',\n'
        scheme_str = scheme_str.rstrip('\n,')
        return scheme_str,datatype_list

def generate_partiion_schema(partition_level,random_enabled=True):
    """
    random generate the partition schema

    :param partition_level: partition_level is K , that means it has K parttion column
    :param random_enabled:
        if enableds,the partition schema result will not be displayed again;
        else you can generate the same partition schema based on the same partition_level
    :return:
    """
    if random_enabled == True:
        PARTITION_DATATYPE_LIST = np.random.choice(HIVE_PARTITION_DATETYPE, partition_level, replace=True)
        PARTITION_COLUMN_NAME_LIST = [clean(item)+"_PARTITION_COLUMN_"+str(index) for index,item in enumerate(PARTITION_DATATYPE_LIST)]
        scheme_str = ''
        for datatype, column_name in zip(PARTITION_DATATYPE_LIST, PARTITION_COLUMN_NAME_LIST):
            scheme_str = scheme_str + str(column_name) + " " + str(datatype) + ',\n'
        scheme_str = scheme_str.rstrip(',,\n')
        return scheme_str,PARTITION_DATATYPE_LIST,PARTITION_COLUMN_NAME_LIST
    else:
        np.random.seed(0)
        PARTITION_DATATYPE_LIST = np.random.choice(HIVE_PARTITION_DATETYPE, partition_level, replace=True)
        PARTITION_COLUMN_NAME_LIST = [clean(item)+"_PARTITION_COLUMN_"+str(index) for index,item in enumerate(PARTITION_DATATYPE_LIST)]
        scheme_str = ''
        for datatype, column_name in zip(PARTITION_DATATYPE_LIST, PARTITION_COLUMN_NAME_LIST):
            scheme_str = scheme_str + str(column_name) + " " + str(datatype) + ',\n'
        scheme_str = scheme_str.rstrip(',,\n')
        return scheme_str,PARTITION_DATATYPE_LIST,PARTITION_COLUMN_NAME_LIST


def generate_create_hive_tbl_statement(column_count,partition_level,tbl_name,file_format,comment,random_enabled):
    """

    generate the CREATE TABLE STATEMENT,both support the none-partition-table and table-with-partition

    :param column_count: base table's column count,decide how flat a table is
    :param partition_level: partition's column count,decide how flat a partition is
    :param tbl_name: base_table_name
    :param file_format: file_format of tale
    :param comment:
    :param random_enabled: whether random the schema of base table and partition
    :return:
    """
    base_table_schema_str,base_datatype = generate_sample_hive_tbl_schema(column_count,random_enabled)
    partition_schem_str,partition_datatype,partition_namelist = generate_partiion_schema(partition_level,random_enabled)
    if int(partition_level) >= 1:
        ddl = 'CREATE TABLE IF NOT EXISTS %s\n(\n%s\n)\nCOMMENT "%s"\nPARTITIONED BY\n(\n%s\n)\nSTORED AS %s;'%\
          (tbl_name,base_table_schema_str,comment,partition_schem_str,file_format)
    else:
        ddl = 'CREATE TABLE IF NOT EXISTS %s\n(\n%s\n)\nCOMMENT "%s"\nSTORED AS %s;' % \
              (tbl_name, base_table_schema_str, comment, file_format)
        partition_namelist = None

    return ddl,base_datatype,partition_datatype,partition_namelist


def generate_insert_into_hive_value_statement(tbl_name,base_datatype,partiton_datatype,rows_count,partition_namelist):
    """

    generate the 'INSERT INTO UNION ALL...',both support the none-partition-table and table-with-partition

    :param tbl_name:
    :param base_datatype:
        when you want to generate the INSERT INTO STATEMENT,you need to input the basetable's datatype
    :param partiton_datatype:
        when you want to generate the INSERT INTO STATEMENT,you need to input the partition's datatype
    :param rows_count:
        how many rows you want to insert
    :param partition_namelist:
        when you want to generate the INSERT INTO STATEMENT,you need to input the base table's partition column name list
    :return:
    """
    if len(partiton_datatype) >= 1:
        # INSERT INTO TABLE XXX PARTITION(dt='first_dt') VALUES ('new_data','new_user_id','new_scene');
        for i in range(rows_count):
            partition_value_str,partition_value_list = generate_row_value(partiton_datatype)
            partition_sql_str_raw = [ '%s=%s'%(i,j) for i,j in zip(partition_namelist,partition_value_list)]
            partiion_str = ''
            for i,j in zip(partition_namelist,partition_value_list):
                partiion_str = partiion_str + str(i) + '=' + str(j) + ',\n'
            partiion_str = partiion_str.rstrip(',\n')
        BASE_SQL = "INSERT INTO TABLE %s" % tbl_name
        BASE_SQL = BASE_SQL + '\nPARTITION(\n%s\n)' % partiion_str
        for i in range(rows_count):
            str_tmp,item_list = generate_row_value(base_datatype)
            BASE_SQL = BASE_SQL + "\nSELECT %s UNION ALL" % str_tmp
        return BASE_SQL.rstrip('UNION ALL') + ';'
    else:
        # INSERT INTO TABLE XXX VALUES ('new_data','new_user_id','new_scene');
        BASE_SQL = "INSERT INTO TABLE %s"%tbl_name
        for i in range(rows_count):
            str_tmp, item_list = generate_row_value(base_datatype)
            BASE_SQL = BASE_SQL + "\nSELECT %s UNION ALL"%str_tmp
        BASE_SQL = BASE_SQL.rstrip('UNION ALL') + ';'
        return BASE_SQL


if __name__ == "__main__":
    #connect_hive()

    tbl_name = 'oh_shit_10'

    resutl = generate_sample_hive_tbl_schema(20)

    ddl,base_datatype,partition_datatype,partition_namelist = generate_create_hive_tbl_statement(10,2,tbl_name,'PARQUET','THIS IS COMMENT',random_enabled=False)

    print("----------------------------------THIS IS THE CREATE TABLE STATEMENT----------------------------------------\n")

    print(ddl)

    print("\n---------------------------------THIS IS THE INSERT INTO STATAMENT-----------------------------------------------\n")

    print(generate_insert_into_hive_value_statement(tbl_name,base_datatype,partition_datatype,50,partition_namelist))