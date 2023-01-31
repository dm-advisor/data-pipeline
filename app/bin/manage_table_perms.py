import os
import boto3
import logging
import argparse
import watchtower
import datetime

from ucop_util.lf_perms_helper import lf_perms_helper
"""
This Python program grants Lake Formation permissions on Athena tables to a set of predefined
IAM roles based on information in a JSON configuration file (e.g., config_perm.json).
This program is executed as part of each CloudFormation deployment to an environment.
"""
PGM_NAME = 'manage_table_perms.py'
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
        description='A program to manage Lake Formation permissions on Athena tables. Before executing this program, '
                    "be sure to 'export branchEnv=<env>' where <env> is the target environment "
                    '(e.g., dev, qa, uat or prod).')
    parser.add_argument(
        'mode',
        help='Mode in which you want to manage Lake Formation permissions (e.g., grant or revoke).',
        type=str,
        choices=['grant', 'revoke'])
    parser.add_argument(
        'product',
        help='Product for which to manage Lake Formation permissions (e.g., sdap).',
        type=str)
    parser.add_argument(
        'bucket_label',
        help='S3 Bucket label for the bucket in which the configuration JSON files are stored (e.g., app).',
        type=str)
    parser.add_argument(
        'app_config_file',
        help='Full path/name of the application configuration JSON file in S3 (e.g., config/config.json).',
        type=str)
    parser.add_argument(
        'perm_config_file',
        help='Full path/name of the permissions configuration JSON file in S3 (e.g., config/config_perm.json).',
        type=str)
    parser.add_argument(
        '-p',
        '--prefix',
        help='Prefix for use in calls to lf_perms_helper.get_table_name() method to limit the list of table names'
             'returned to a set that begins with the provided prefix.',
        type=str)
    parser.add_argument(
        '-s',
        '--suffix',
        help='Suffix for use in calls to lf_perms_helper.get_table_name() method to limit the list of table names '
             'returned to a set that ends with the provided suffix.',
        type=str)
    parser.add_argument(
        '-sor',
        '--sso_org_roles_config_file',
        help='Full path/name of the configuration JSON file in S3 that contains the SSO roles that '
             'are managed at the AWS organization level (e.g., config/sso_roles_config.json).',
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
    mode = args.mode
    product = args.product.lower()
    environment = os.getenv('branchEnv')
    if environment is None or len(environment) == 0:
        raise Exception(
            "Unable to determine environment based on os.getenv('branchEnv'). Be sure to 'export branchEnv=<env>'"
            ' where <env> is one of dev, qa, uat or prod')

    config_bucket_label = args.bucket_label
    app_config_file = args.app_config_file
    perm_config_file = args.perm_config_file
    prefix = args.prefix
    suffix = args.suffix
    if args.sso_org_roles_config_file is not None:
        sso_org_roles_config_file = args.sso_org_roles_config_file
    else:
        sso_org_roles_config_file = 'config/sso_roles_config.json'

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
        'Positional arguments set to: mode={}, product={}, config_bucket_label={}, app_config_file={} and '
        'perm_config_file={}'
        .format(mode, product, config_bucket_label, app_config_file, perm_config_file))
    logger.info(
        'Optional/default arguments set to: prefix={}, suffix={}, region={}, and logger_level={}'
        .format(prefix, suffix, region, logger_level))
    logger.info('branchEnv={}'.format(environment))

    try:
        lfph_obj = lf_perms_helper(mode, product, environment,
                                   config_bucket_label, app_config_file,
                                   perm_config_file, sso_org_roles_config_file, region, logger_level)

        table_names_list = lfph_obj.get_table_names(prefix, suffix)
        if len(table_names_list) == 0:
            raise Exception(
                'Call to get_table_names({}, {}) returned an empty list.'.
                format(prefix, suffix))

        lfph_obj.handle_permissions(table_names_list)
        logger.info('Permissions were granted successfully!')
    except Exception as ex:
        logger.exception(ex)
        raise


if __name__ == '__main__':
    main()
