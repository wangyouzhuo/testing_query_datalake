
from conf.CONFIG import *

'''

[Apache HIVE DOC]
https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-AlterTable/Partition/Column

 1.给hive表添加字段

    例1：alter table table_name add columns (now_time string comment '当前时间');
    例2：alter table table_name add columns (now_time varchar(300) comment '当前时间');

        input: table_name

        process: 随机创建一个新的列 data_type随机采样/column_name为 'COLUMN_ADD_RANDOMSTRING'/COMMENT 'ADD COLUMN DATATYPE'

 2.给hive表添加字段，然后移动到某个特定列的后面

    alter table table_name add columns (c_time string comment '当前时间'); -- 正确，添加在最后
    alter table table_name change c_time c_time string after address ;  -- 正确，移动到指定位置,address字段的后面

 3.修改列的名称

    ALTER TABLE table_name CHANGE 旧列名 新列名 字段类型;
    例1：alter table DWD_ORC_ENTRY_XF change id taskid varchar(300);

        input: table_name

        process: 随机选择column_list中的其中一个，生成新的名称，然后执行修改


 4.修改hive表的名称

    alter table name rename to new_Name;

 5.修改表的备注

    ALTER TABLE 数据库名.表名 SET TBLPROPERTIES('comment' = '新的表备注');

 6.删除hive表的字段

     ALTER TABLE test REPLACE COLUMNS (
    a STRING,
    b BIGINT,
    c STRING,
    d STRING,
    e BIGINT
    );

 7.添加hive表分区

    alter table 表名 add if not exists partitions(dt='2021-11-11');

        input: table_name

        process: 基于当前表的分区结构，生成新的若干个partition，然后执行 add partiiton

 8.删除分区

     alter table 表名 drop if exists partition(dt='2022-05-06');

        input: table_name

        process: 基于当前表的分区结构，找出其中的若干个partition，然后执行 drop partiiton

 9.列数据类型转换



'''
import random


def get_schema_from_hive_statement(tbl_name):
    sql_str = 'DESC %s'%tbl_name
    return sql_str


def get_statment_add_column(tbl_name,column_name):
    target_datatype = random.sample(HIVE_DATATYPE,1)
    base_str = 'alter table %s add columns(%s %s comment "this column is just added to table")'%(tbl_name,target_datatype,column_name)
    return base_str


def get_statement_add_partition(tbl_name):

    partiion_key_value_str = ''
    base_str = 'alter table %s add if not exists partition(%s)'%(tbl_name,partiion_key_value_str)
    return base_str




