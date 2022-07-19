

# when generate base table, it can random sample datatype from this list
HIVE_DATATYPE = ['INT','BIGINT',
                 'TIMESTAMP',
                  'STRING',
                  'VARCHAR(20)',
                  'CHAR(20)',
                 'DOUBLE','FLOAT']

# when generate table partition, the partition column's datatype will random sample from list
HIVE_PARTITION_DATETYPE = ['INT','BIGINT','STRING']

# when generate table schema, the file format of table will random sample from list
FILE_FORMAT = ['ORC','CSV','PARQUET','TEXTFILE']



# when generate random string data for string columns, the string will random sample from list
STRING_SET = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']