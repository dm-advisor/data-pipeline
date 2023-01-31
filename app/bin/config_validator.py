import os
import boto3
import argparse
import logging
import datetime
import json
import watchtower

from botocore.exceptions import ClientError
from jsonschema import validate
from ucop_util import stack_info

"""
This Python program validates a JSON configuration file against its schema and
reports any invalid entries.
"""
PGM_NAME = 'config_validator.py'
MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
current_date = datetime.datetime.now()
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger(
    PGM_NAME + '_' + current_date.strftime('%Y-%m-%d-%H-%M-%S'))


def main():
    """
    Main function (entry point)
    """
    parser = argparse.ArgumentParser(
        description='A program to validate the JSON configuration files.')
    parser.add_argument(
        'product',
        help='Product for which to manage Lake Formation permissions (e.g., rdms).',
        type=str)
    parser.add_argument(
        'bucket_label',
        help='S3 Bucket label for the bucket in which the configuration JSON files are stored (e.g., app).',
        type=str)
    parser.add_argument(
        'schema_file',
        help='Full path/name of the schema for the JSON configuration file in S3 that you want to validate'
             ' (e.g., config/perm_config_schema.json).',
        type=str)
    parser.add_argument(
        'config_file',
        help='Full path/name of the configuration JSON file in S3 that you want to validate'
             ' (e.g., config/erm_reporting_config.json).',
        type=str)
    parser.add_argument(
        '-r',
        '--region',
        help='AWS region in which to execute this program.',
        choices=['us-west-1', 'us-east-1', 'us-east-2'])
    parser.add_argument(
        '-l',
        '--logger_level',
        help='Desired level of logging.',
        choices=['debug', 'error', 'critical', 'info'],
        default='debug')
    args = parser.parse_args()
    product = args.product.lower()
    environment = os.getenv('branchEnv')
    if environment is None:
        raise Exception(
            "Unable to determine environment based on os.getenv('branchEnv'). Set the variable using "
            "'export branchEnv=<env>' where <env> is one of dev, qa, uat or prod")

    config_bucket_label = args.bucket_label
    schema_file = args.schema_file
    config_file = args.config_file
    if args.region is not None:
        region = args.region.lower()
    else:
        region = 'us-west-2'
    boto3.setup_default_session(region_name=region)

    if args.logger_level is None:
        logger_level = 'DEBUG'
    else:
        logger_level = args.logger_level.upper()

    if logger_level == 'INFO':
        logger.setLevel(logging.INFO)
    elif logger_level == 'ERROR':
        logger.setLevel(logging.ERROR)
    elif logger_level == 'CRITICAL':
        logger.setLevel(logging.CRITICAL)
    else:
        logger.setLevel(logging.DEBUG)

    logger.addHandler(
        watchtower.CloudWatchLogHandler(
            log_group='/' + product + '/' + environment + '/log',
            create_log_group=True))

    logger.info(
        'About to execute program={}'.format(PGM_NAME))
    logger.info(
        'Positional arguments set to: product={}, config_bucket_label={}, schema_file={}, config_file={}'
        .format(product, config_bucket_label, schema_file, config_file))
    logger.info(
        'Optional/default arguments set to: region={}, and logger_level={}'
        .format(region, logger_level))

    s3_resource = boto3.resource('s3')

    # Obtain the bucket name where config JSON file is located from
    # CloudFormation stack.

    bucket_name = stack_info(logger_level=logger_level).get_bucket_name_by_label(
        product, environment, config_bucket_label)

    # Load the schema file.
    try:
        schema_obj = s3_resource.Object(bucket_name, schema_file)
        schema_file_body = schema_obj.get()['Body']
        json_schema = json.load(schema_file_body)

    except ClientError as err2:
        if err2.response['Error']['Code'] == 'NoSuchKey':
            logger.exception(
                "Unable to locate the specified schema file '{}' in the specified "
                "bucket '{}'!".format(schema_file, bucket_name))
            raise
        else:
            logger.exception(err2)
            raise

    # Load the configuration JSON file.
    try:
        data_obj = s3_resource.Object(bucket_name, config_file)
        data_file_body = data_obj.get()['Body']
        json_data = json.load(data_file_body)

    except ClientError as err2:
        if err2.response['Error']['Code'] == 'NoSuchKey':
            logger.exception(
                "Unable to locate the specified configuration file '{}' in the specified "
                "bucket '{}'!".format(config_file, bucket_name))
            raise
        else:
            logger.exception(err2)
            raise

    # If no exception is raised by validate(), the instance is valid.
    validate(instance=json_data, schema=json_schema)

    logger.info('{} file in {} is valid.'.format(config_file, bucket_name))


if __name__ == '__main__':
    main()
