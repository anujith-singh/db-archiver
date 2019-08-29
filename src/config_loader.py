import yaml


with open('mysql-archiver-config.yml', 'r') as stream:
    config = yaml.safe_load(stream)
    database_config = config.get('database_config')
    archive_configs = config.get('archive')
    s3_config = config.get('s3_config')

    with open('pt_archiver_config.template', 'r') as file:
        pt_archiver_config_template = file.read()

        pt_archiver_base_config = pt_archiver_config_template.format(
            user=database_config.get('user'),
            password=database_config.get('password'),
            host=database_config.get('host')
        )

        f = open('pt-archiver-base.conf', 'w')
        f.write(pt_archiver_base_config)
        f.close()
