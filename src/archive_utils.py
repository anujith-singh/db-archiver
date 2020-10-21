import logging
import subprocess

db_to_db_archive_command = """
pt-archiver \
    --config pt-archiver-base.conf \
    --source {source_dsn} \
    --dest h={archive_host},D={archive_db_name},t={archive_table_name} \
    --where "{where_clause}" \
    --limit {transaction_size} \
    --txn-size {transaction_size} \
    --bulk-delete \
    --no-check-charset \
    {optimize_str}
"""

db_to_file_archive_command = """
pt-archiver \
    --config pt-archiver-base.conf \
    --source h={archive_host},D={archive_db_name},t={archive_table_name} \
    --where "true" \
    --no-delete \
    --limit={transaction_size} \
    --header \
    --file="{archive_file_name}" \
    --output-format=csv \
    --no-safe-auto-increment \
    --no-check-charset
"""


def archive_to_db(host, archive_host, db_name, table_name, archive_db_name, archive_table_name,
                  where_clause, transaction_size, optimize):
    optimize_str = ''
    if optimize:
        optimize_str = ' --optimize=s'

    source_dsn = 'h={host},D={db_name},t={table_name}'.format(
        host=host,
        db_name=db_name,
        table_name=table_name,
    )

    archive_command = db_to_db_archive_command.format(
        source_dsn=source_dsn,
        archive_db_name=archive_db_name,
        archive_table_name=archive_table_name,
        where_clause=where_clause,
        transaction_size=transaction_size,
        optimize_str=optimize_str,
        archive_host=archive_host
    )
    archive_command = ' '.join(archive_command.split())

    logging.info('')
    logging.info('')
    logging.info('Archiving from DB to archive DB')
    logging.info(f'Executing: {archive_command}')

    execute_shell_command(archive_command)


def archive_to_file(archive_host, archive_db_name, archive_table_name, transaction_size,
                    local_file_name):
    archive_command = db_to_file_archive_command.format(
        archive_db_name=archive_db_name,
        archive_table_name=archive_table_name,
        transaction_size=transaction_size,
        archive_file_name=local_file_name,
        archive_host=archive_host
    )
    archive_command = ' '.join(archive_command.split())

    logging.info('')
    logging.info('')
    logging.info('Archiving from archive DB to file')
    logging.info(f'Executing: {archive_command}')

    execute_shell_command(archive_command)


def execute_shell_command(archive_command):
    result = subprocess.run(archive_command, shell=True, stderr=subprocess.PIPE)

    if result.returncode != 0:
        exception_msg = f"Return code: {result.returncode}, Error: {result.stderr.decode('utf-8')}"
        raise Exception(exception_msg)
