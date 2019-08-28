import subprocess
from db_utils import mysql_connection


db_to_db_archive_command = """
pt-archiver \
  --source D={db_name},t={table_name} \
  --dest D={archive_db_name},t={archive_table_name} \
  --where "{where_clause}" \
  --limit {transaction_size} \
  --txn-size {transaction_size} \
  --bulk-delete \
  --no-check-charset
"""

db_to_file_archive_command = """
pt-archiver \
  --source D={archive_db_name},t={archive_table_name} \
  --where "true" \
  --no-delete \
  --limit={transaction_size} \
  --header \
  --file="{archive_file_name}" \
  --output-format=csv \
  --no-safe-auto-increment \
  --no-check-charset
"""


def archive_to_db(db_name, table_name, archive_db_name, archive_table_name, where_clause, transaction_size):
    archive_command = db_to_db_archive_command.format(
        db_name=db_name,
        table_name=table_name,
        archive_db_name=archive_db_name,
        archive_table_name=archive_table_name,
        where_clause=where_clause,
        transaction_size=transaction_size
    )
    subprocess.run(archive_command, shell=True, check=True)


def archive_to_file(db_name, table_name, archive_db_name, archive_table_name, where_clause, transaction_size, local_file_name):
    archive_command = db_to_file_archive_command.format(
        archive_db_name=archive_db_name,
        archive_table_name=archive_table_name,
        transaction_size=transaction_size,
        archive_file_name=local_file_name
    )
    subprocess.run(archive_command, shell=True, check=True)
