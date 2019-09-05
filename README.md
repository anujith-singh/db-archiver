##### Configuration:
* Copy config template to actual config file
```cp db-archiver-config.yml.sample db-archiver-config.yml```
* Modify config file with actual credentials
```vi db-archiver-config.yml```


##### Example usage:
```python src/mysql_archiver.py --table my_table_name --where "created < now() - interval 6 month" --column_name_to_log id```


##### To create a mysql user with privileges just enough for this tool
```
CREATE USER 'db_archiver' IDENTIFIED BY 'somepassword';
GRANT SELECT, DELETE ON `<db_to_archive>`.`<table_to_archive>` TO 'db_archiver';
GRANT CREATE, INSERT, SELECT, DROP ON `<db_to_archive>`_archive.`<table_to_archive>`_archive TO 'db_archiver';
```
