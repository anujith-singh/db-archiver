from config_loader import s3_config
from botocore.exceptions import ClientError


def upload_to_s3(local_file, s3_path):
    bucket_name = s3_config.get('bucket_name')
    s3_path = get_usable_s3_path(bucket_name, s3_path)
    s3_client.upload_fileobj(
        local_file,
        bucket_name,
        s3_path
    )


def get_usable_s3_path(bucket_name, s3_path, incrementor=0):
    exists = check_if_s3_file_exists(bucket_name, s3_path)
    if exists:
        s3_path = os.path.splitext(s3_path)[0]
        if incrementor != 0:
            remove_char_count = len(str(incrementor)) + 1
            s3_path = s3_path[:-remove_char_count]
        incrementor += 1
        s3_path = s3_path + '_' + str(incrementor) + '.csv'
        if incrementor > 10:
            raise Exception('10 variants of the same file exists in S3')
        return get_usable_s3_path(bucket_name, s3_path, incrementor)

    return s3_path


def check_if_s3_file_exists(bucket_name, s3_path):
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_path)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise e
    return True
