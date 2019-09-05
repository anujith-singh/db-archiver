import yaml


with open('db-archiver-config.yml', 'r') as stream:
    config = yaml.safe_load(stream)
    database_config = config.get('database_config')
    s3_config = config.get('s3_config')
    sentry_dsn = config.get('sentry_dsn')

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
