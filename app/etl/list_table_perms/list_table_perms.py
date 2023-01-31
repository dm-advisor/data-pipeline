import os
import boto3
import logging
import json
import sys

from ucop_util.stack_info import stack_info
from ucop_util.util_exception import ValueNotFoundError

"""
This Lambda function is used to populate the underlying data for the role_permission Athena table in sec_reporting
database. The table provides the base data for the various Athena views (e.g., claims_ep_grant_role_metadata_view) 
that are used by Cognos Analytics reports.
This Lambda function is executed each night using the following event bridge rule:
rdms-{env}-list-table-perms-lambda-scheduled-role.
"""

PGM_NAME = __name__
MSG_FORMAT = '%(asctime)s %(levelname)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

product = os.getenv('product')
environment = os.getenv('environment')
region = os.getenv('region_name')
logger_level = os.getenv('logger_level_value')
out_bucket_label = os.getenv('out_bucket_label')
out_bucket_key = os.getenv('out_bucket_key')

logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger(PGM_NAME)

if logger_level == 'DEBUG':
    logger.setLevel(logging.DEBUG)
elif logger_level == 'ERROR':
    logger.setLevel(logging.ERROR)
elif logger_level == 'INFO':
    logger.setLevel(logging.INFO)
elif logger_level == 'CRITICAL':
    logger.setLevel(logging.CRITICAL)


logger.info('Loading {} function'.format(PGM_NAME))

lf_client = boto3.client('lakeformation', region_name=region)
s3_resource = boto3.resource('s3')


def lambda_handler(event, context):
    """
    Handler is a Python function that AWS Lambda invokes. The handler is the entry
    point into the Lambda function. The purpose of this particular lambda function
    is to list all principals and their policies associated with Athena tables across
    all databases.

    Parameters
    ----------
    event : list
            Data related to the event that triggered the Lambda function.
    context : LambdaContext
            Context object that provides the runtime information to the handler.
    Returns
    -------
    None
    Exceptions
    ----------
    None
    """
    logger.debug('Now in {}...'.format(sys._getframe().f_code.co_name))
    logger.info('product={}'.format(product))
    logger.info('environment={}'.format(environment))
    logger.info('region={}'.format(region))
    logger.info('logger_level={}'.format(logger_level))
    logger.info('out_bucket_label={}'.format(out_bucket_label))
    logger.info('out_bucket_key={}'.format(out_bucket_key))
    logger.debug('event={}'.format(event))
    logger.debug('context={}'.format(context))
    logger.debug('context object env={}'.format(context.client_context))

    try:
        out_bucket_name = stack_info().get_bucket_name_by_label(
            product, environment, out_bucket_label)
        logger.debug('out_bucket_name={}'.format(out_bucket_name))

        list_table_permissions(out_bucket_name, out_bucket_key)

    except ValueNotFoundError as vnfe:
        logger.error('Unable to obtain S3 bucket names from the stack.')
        logger.exception(vnfe)
        raise vnfe
    except Exception as ex:
        logger.error(
            'An exception caused the processing to fail. Please investigate and remediate!'
        )
        logger.exception(ex)
        raise ex


def list_table_permissions(out_bucket_name, out_bucket_key):
    """
    Uses Lake Formation boto3 API to get all principles and their policies associated with each
    table across all databases.

    Parameters
    ----------
    out_bucket_name: string
            The name of S3 bucket in which the output should be stored.
    out_bucket_key: string
            The path and file name for the output.
    Returns
    -------
    None
    Exceptions
    ----------
    None
    """
    logger.debug('Now in {}...'.format(sys._getframe().f_code.co_name))

    output_content = ""

    response = lf_client.list_permissions()
    perm_info = response['PrincipalResourcePermissions']

    while 'NextToken' in response.keys():
        response = lf_client.list_permissions(NextToken=response["NextToken"])
        perm_info.extend(response['PrincipalResourcePermissions'])

    for principal in perm_info:
        output_content += json.dumps(principal) + '\n'

    s3_resource.Object(out_bucket_name, out_bucket_key).put(Body=output_content)
