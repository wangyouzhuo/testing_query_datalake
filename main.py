
from tool_package.connect_source import *
from tool_package.create_hive_data import *
from tool_package.general import *
from tool_package.schema_change import *





if __name__ == '__main__':

    # where you want to create these tables
    target_database = 'ACCEPTANCE_DB_5'

    # how many tables you wang to create
    TABLE_COUNT = 5

    # for each table, how flat they are and how deep the partition is ,and how many rows count you want to insert into
    COLUMN_COUNT,PARTITION_LEVEL,ROWS_COUNT = 5,2,10

    # connect to your target hive with jdbc
    curs = build_hive_connecton(host='172.26.194.238',port="10000",database="default",username="root",password='123456',auth='CUSTOM')

    # create db
    perform_sql(curs,'CREATE DATABASE %s'%target_database)

    # use db
    perform_sql(curs,'USE %s'%target_database)


    # stage 1：create table and insert rows data into them

    get_partition_datatype = dict()
    get_partition_namelist = dict()
    get_base_datatype = dict()
    get_base_namelist = dict()

    for i in range(TABLE_COUNT):
        tbl_name = 'ACCEPTANCE_TBL_%s'%str(i)
        print(tbl_name)
        # 建表
        # ddl,base_datatype,base_columnname_lisy,partition_datatype,partition_namelist
        ddl,base_datatype,base_columnname_list,partition_datatype,partition_namelist = generate_create_hive_tbl_statement(
            column_count=COLUMN_COUNT,partition_level=PARTITION_LEVEL,tbl_name=tbl_name,file_format='PARQUET',comment='acceptance hive',random_enabled = False)
        perform_sql(curs,ddl)
        # 插入数据
        INSERT_SQL = generate_insert_into_hive_value_statement(tbl_name=tbl_name,base_datatype=base_datatype,partiton_datatype=partition_datatype
                                                  ,rows_count=60,partition_namelist=partition_namelist)
        perform_sql(curs, INSERT_SQL)

        get_partition_datatype[tbl_name] = partition_datatype
        get_partition_namelist[tbl_name] = partition_namelist
        get_base_datatype[tbl_name] = base_datatype
        get_base_namelist[tbl_name] = base_columnname_list



    print("数据准备完毕，总计：%s 张表，每张表：%s 列，初始化：%s 行数据，%s 个分区"%(TABLE_COUNT,COLUMN_COUNT,ROWS_COUNT,PARTITION_LEVEL))

    message = input("请前往客户端对目标数据库：%s下的数据表，进行查询。确认其分区数量和Schema，无误后按回车：")%target_database

    print("准备进行表结构变更，并向变更后的表写入数据")


    '''
    stage2 ：schema change, and insert new data into the new data(after change)
    '''

    for i in range(TABLE_COUNT):
        # 进行加列
        #
        tbl_name = 'ACCEPTANCE_TBL_%s' % str(i)
        # target_datatype = random.sample(HIVE_DATATYPE, 1)
        # base_str = 'alter table %s add columns %s %s comment "this column is just added to table")'% (tbl_name,'new_column',target_datatype,)
        # print(base_str)
        # perform_sql(curs,base_str)
        # get_base_datatype[tbl_name] = get_base_datatype[tbl_name].append(target_datatype)
        # get_base_namelist[tbl_name] = get_base_namelist[tbl_name].append('new_add_column')
        # INSERT_SQL = generate_insert_into_hive_value_statement(tbl_name=tbl_name,
        #                                                        base_datatype=get_base_datatype[tbl_name],
        #                                                        partiton_datatype=get_partition_datatype[tbl_name],
        #                                                        partition_namelist=get_partition_namelist[tbl_name],
        #                                                        rows_count=20)
        # perform_sql(curs, INSERT_SQL)
        get_statement_add_partition()




    print("变更完毕")






