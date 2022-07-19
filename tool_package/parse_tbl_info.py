from tool_package.connect_source import build_hive_connecton


"""
"""


def get_info(cursor,table_name):
    """
    generate the desc statement
    :param cursor:
    :param table_name:
    :return: sql statement like 'DESC TABLE_NAME'
    """
    desc_statement = 'desc %s'%table_name
    cursor.execute(desc_statement)
    res = cursor.fetchall()
    return res


def get_partitions(cursor,table_name):
    """
    generate the show partitions statements
    :param cursor:
    :param table_name:
    :return:
    """
    base_str = 'SHOW PARTITIONS %s'%table_name
    cursor.execute("base_str")
    res = cursor.fetchall()
    return res


if __name__ == '__main__':

    connection= build_hive_connecton()
    cursor = connection.cursor()
    # cursor.execute("select * from table_name")
    # res = cursor.fetchall()
    # conn.close()
    # for item in res:
    #    print(item)