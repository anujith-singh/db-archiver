import mysql.connector
from mysql.connector import Error
from config_loader import database_config
import re


MYSQL_CONNECTION_STRING = 'mysql://{user}:{password}@{host}'

db_user = database_config.get('user')
db_password = database_config.get('password')
db_host = database_config.get('host')

mysql_connection = mysql.connector.connect(host=db_host, user=db_user, password=db_password)
mysql_cursor = mysql_connection.cursor(dictionary=True)


def create_archive_database(db_name, archive_db_name):
    mysql_cursor.execute(f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{archive_db_name}'")
    result = mysql_cursor.fetchone()

    if result is None:
        mysql_cursor.execute(f'SHOW CREATE DATABASE {db_name}')
        create_db_query = mysql_cursor.fetchone()['Create Database']
        create_archive_db_query = re.sub(r'(?s)(CREATE DATABASE )(`.*?)(`)', r'\1IF NOT EXISTS `' + archive_db_name + '`', create_db_query, count=1)
        mysql_cursor.execute(create_archive_db_query)


def create_archive_table(db_name, table_name, archive_db_name, archive_table_name):
    mysql_cursor.execute(f'USE {db_name}')
    mysql_cursor.execute(f'SHOW CREATE TABLE {table_name}')
    create_table_query = mysql_cursor.fetchone()['Create Table']

    create_archive_table_query_list = []
    # create_archive_table_query_str = ''
    for line in create_table_query.splitlines():
        if 'CREATE TABLE' in line:
            # replacing table_name with table_name_archive in CREATE TABLE query
            create_archive_table_query_list.append(re.sub(r'(?s)(CREATE TABLE )(`.*?)(`)', r'\1`' + archive_table_name + '`', line, count=1))
            # create_archive_table_query_str += re.sub(r'(?s)(CREATE TABLE )(`.*?)(`)', r'\1`' + archive_table_name + '`', line, count=1)
            continue
        if not re.search('CONSTRAINT(.*)FOREIGN KEY(.*)REFERENCES', line):
            create_archive_table_query_list.append(line)
            # create_archive_table_query_str += line

    line_count = len(create_archive_table_query_list)
    remove_comma_line_no = line_count - 2
    remove_comma_line = create_archive_table_query_list[remove_comma_line_no]
    remove_comma_line = remove_comma_line.rstrip(',')
    create_archive_table_query_list[remove_comma_line_no] = remove_comma_line
    create_archive_table_query = ' '.join(create_archive_table_query_list)

    mysql_cursor.execute(f'USE {archive_db_name}')
    mysql_cursor.execute(create_archive_table_query)


def drop_archive_table(archive_db_name, archive_table_name):
    mysql_cursor.execute(f'USE {archive_db_name}')
    mysql_cursor.execute(f'DROP TABLE {archive_table_name}')


def get_count_of_rows_archived(archive_db_name, archive_table_name):
    mysql_cursor.execute(f'SELECT count(*) as count FROM {archive_db_name}.{archive_table_name}')

    return mysql_cursor.fetchone()['count']


def get_file_names(db_name, table_name, archive_db_name, archive_table_name, column_to_add_in_s3_filename, where_clause):
    column_name = column_to_add_in_s3_filename

    mysql_cursor.execute(f'SELECT {column_name} as first_value FROM {archive_db_name}.{archive_table_name} ORDER BY {column_name} LIMIT 1')
    first_value = mysql_cursor.fetchone()['first_value']
    first_value = str(first_value)

    mysql_cursor.execute(f'SELECT {column_name} as last_value FROM {archive_db_name}.{archive_table_name} ORDER BY {column_name} DESC LIMIT 1')
    last_value = mysql_cursor.fetchone()['last_value']
    last_value = str(last_value)

    data_part_name = 'from_(' + first_value + ')_to_(' + last_value + ')'
    s3_path = db_name + '/' + table_name + '/' + data_part_name + '_where_(' + where_clause + ').csv'
    local_file_name = table_name + '_' + data_part_name + '.csv'

    return local_file_name, s3_path
