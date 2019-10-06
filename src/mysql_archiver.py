import argparse
import gzip
import logging
import os
import sentry_sdk

import archive_utils
import db_utils
import s3_utils
from config_loader import database_config, sentry_dsn
from mysql.connector.errors import ProgrammingError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s'
)


def start_archival():
    parser = argparse.ArgumentParser(description='MySQL DB Archiver')

    parser.add_argument(
        '--table',
        '-t',
        dest='table',
        type=str,
        required=True,
        help='Table to be archived')

    parser.add_argument(
        '--where',
        '-w',
        dest='where',
        type=str,
        required=True,
        help='Where clause for archiving table, this will also be appended to archive file name')

    parser.add_argument(
        '--column_name_to_log',
        '-c',
        dest='column_name_to_log',
        required=True,
        help='Smallest and largest values from this column will be part of the archiver file name')

    parser.add_argument('--optimize', dest='optimize', action='store_true')

    args = parser.parse_args()
    table_name = args.table
    where_clause = args.where
    column_name_to_log_in_file = args.column_name_to_log
    optimize = args.optimize

    if not table_name or not where_clause or not column_name_to_log_in_file:
        raise ValueError(
            f'table: {table_name} | where: {where_clause} | column_name_to_log: {column_name_to_log_in_file},'
            f' These are mandatory values.'
        )

    db_name = database_config.get('database')
    transaction_size = database_config.get('transaction_size')
    logging.info('Starting archive...')
    archive(db_name, table_name, where_clause, column_name_to_log_in_file, transaction_size, optimize)


def archive(db_name, table_name, where_clause, column_name_to_log_in_file,
            transaction_size, optimize):
    logging.info(
        f'\n\n------------- archiving {db_name}.{table_name} -------------')

    archive_db_name = db_name + '_archive'
    archive_table_name = table_name + '_archive'

    db_utils.create_archive_database(db_name, archive_db_name)

    try:
        db_utils.create_archive_table(
            db_name, table_name, archive_db_name, archive_table_name)
    except ProgrammingError as e:
        if e.errno == 1050:
            logging.info(
                f'Archive table {archive_db_name}.{archive_table_name} exists,'
                f' archiving older rows'
            )

            fetch_archived_data_upload_to_s3_and_delete(
                db_name, table_name, archive_db_name, archive_table_name,
                column_name_to_log_in_file, transaction_size, '')

            archive(db_name, table_name, where_clause,
                    column_name_to_log_in_file, transaction_size, optimize)

            return None
        else:
            raise e

    archive_utils.archive_to_db(
        db_name, table_name, archive_db_name, archive_table_name, where_clause,
        transaction_size, optimize)

    fetch_archived_data_upload_to_s3_and_delete(
        db_name, table_name, archive_db_name, archive_table_name,
        column_name_to_log_in_file, transaction_size, where_clause)


def fetch_archived_data_upload_to_s3_and_delete(
    db_name, table_name, archive_db_name, archive_table_name,
    column_name_to_log_in_file, transaction_size, where_clause):
    no_of_rows_archived = db_utils.get_count_of_rows_archived(
        archive_db_name, archive_table_name)
    if not no_of_rows_archived:
        logging.info(
            f'Archive table {archive_db_name}.{archive_table_name} '
            f'had no rows, dropping archive table')
        db_utils.drop_archive_table(archive_db_name, archive_table_name)

        return None

    local_file_name, s3_path = db_utils.get_file_names(
        db_name, table_name, archive_db_name, archive_table_name,
        column_name_to_log_in_file, where_clause)

    archive_utils.archive_to_file(
        archive_db_name, archive_table_name, transaction_size, local_file_name)

    gzip_file_name = compress_to_gzip(local_file_name)
    gzip_s3_path = f'{s3_path}.gz'

    # s3_utils.upload_to_s3(local_file_name, s3_path)
    s3_utils.upload_to_s3(gzip_file_name, gzip_s3_path)
    logging.info(f'Deleting local file: {local_file_name}')
    os.remove(local_file_name)
    os.remove(gzip_file_name)

    db_utils.drop_archive_table(archive_db_name, archive_table_name)

    return None


def compress_to_gzip(local_file_name):
    gzip_file_name = f'{local_file_name}.gz'
    fp = open(local_file_name,'rb')
    with gzip.open(gzip_file_name, 'wb') as gz_fp:
        gz_fp.write(bytearray(fp.read()))

    return gzip_file_name


if __name__ == '__main__':
    sentry_sdk.init(dsn=sentry_dsn)
    try:
        start_archival()
    except Exception as e:
        sentry_sdk.capture_exception(e)

        raise e
