from pyhive import hive


def build_hive_connecton(host,password,port=10000, auth="LDAP", database="default",username="root"):
    """
    build the connection with the target hive sources
    :param host: hive host
    :param password: hive jdbc password
    :param port: hive jdbc port,default is 10000
    :param auth:
    :param database: connect to the target database,default is DEFAULT
    :param username: hive username，default is 'root'
    :return: cursor
    """
    conn = hive.connect(host=host,port=port, auth=auth, database=database,username=username,password=password)
    cursor = conn.cursor()
    return cursor

if __name__ == '__main__':
    conn = build_hive_connecton(host='172.26.194.238',port="10000",database="default",username="root",password='123456',auth='CUSTOM')
    cursor = conn.cursor()
    cursor.execute("show databases;")
    res = cursor.fetchall()
    print(res)

