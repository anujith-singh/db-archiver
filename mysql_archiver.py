from config_loader import archive_configs, database_config
from mysql.connector.errors import ProgrammingError
import db_utils
import archive_utils
import s3_utils


def start_archival():
    for archive_config in archive_configs:
        db_name = database_config.get('database')
        transaction_size = database_config.get('transaction_size')
        archive(archive_config, db_name, transaction_size)


def archive(archive_config, db_name, transaction_size):
    table_name = archive_config.get('table')
    where_clause = archive_config.get('where')
    column_to_add_in_s3_filename = archive_config.get('column_to_add_in_s3_filename')

    archive_db_name = db_name + '_archive'
    archive_table_name = table_name + '_archive'

    db_utils.create_archive_database(db_name, archive_db_name)
    try:
        db_utils.create_archive_table(db_name, table_name, archive_db_name, archive_table_name)
    except ProgrammingError as e:
        if e.errno == 1050:
            # ToDo: archive the table, upload to s3, drop archive table, start with creating archive table again
            pass
        else:
            raise e

    local_file_name, s3_path = db_utils.get_file_names(db_name, table_name, archive_db_name, archive_table_name, column_to_add_in_s3_filename, where_clause)

    archive_utils.archive_to_db(db_name, table_name, archive_db_name, archive_table_name, where_clause, transaction_size)
    archive_utils.archive_to_file(db_name, table_name, archive_db_name, archive_table_name, where_clause, transaction_size, local_file_name)
    s3_utils.upload_to_s3(local_file_name, s3_path)

    db_utils.drop_archive_table(archive_db_name, archive_table_name)


if __name__ == '__main__':
    start_archival()
