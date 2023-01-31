import os
import re

import boto3
import json
import logging
import watchtower
import datetime
import time
import argparse
from deepdiff import DeepDiff

from botocore.exceptions import ClientError
from ucop_util.stack_info import stack_info

"""
This Python program will deploy the following AWS resources for a given project and environment.
    1. Creates a database based on the information provided in the application configuration JSON file.
    2. Creates the required folders under each S3 bucket based on the information provided in the application
       configuration JSON file.
    3. Creates Athena objects (tables/view) based on the information provided in the application configuration 
       JSON file. The program drops and recreates any existing Athena table that has undergone changes. It detects 
       changes by parsing and comparing the various segments of the CREATE EXTERNAL TABLE statement in the DDL 
       to the table's metadata information that it obtains by using get_tables Glue boto3 API call (see note #3 below).
       The program detects changes by parsing the following segment of the CREATE TABLE statement in the 
       order that is specified below:
       a) Column changes, including changes to column comments
       b) Table comment changes
       c) Partitioned by changes, including changes to comments
       d) Clustered by changes, including changes to number of buckets
       e) Row format changes
       f) Stored as (file format) changes
       g) Location changes
       h) Table properties changes

       The program automatically recreates a table upon detecting the first change and does not check 
       for any other changes. It also reloads all partitions as part of recreating a table, if the table
       is partitioned.

       Note 1: The program recreates all existing views each time, because the cost of detecting changes to 
       a view is higher than simply recreating the view each time.

       Note 2: For the program to detect changes correctly, all values in the DDL, such as comments, row format  
       values in the ROW FORMAT clause, file format values in STORED AS clause, etc. MUST be enclosed in single  
       quotes as opposed to double quotes. The reason is that the program parses and segments text using single 
       quotes as markers strictly.

       Note 3: Using the command below, you can obtain metadata info on a given table. The JSON structure that the
       command returns is similar to the response from get_tables Glue boto3 API call that is mentioned in item #3 
       above.

       aws glue get-tables --database-name <database-name> | jq -r '.TableList[] | 
           select (.Name | endswith("<table-name>"))'

       Example: aws glue get-tables --database-name perf_test | jq -r '.TableList[] | 
           select (.Name | endswith("user_query"))'

        Note 4: The program expects the DDL to be syntactically correct and as such would produce 
        unpredictable results if DDL's syntax is incorrect.

Known Issues: 
    1. Existence of any escaped character, other than an escaped single quote (\'), in any column or 
       table comment in the DDL causes the metadata and DDL not to match! To include a single quote
       in a comment without any issues, you can escape it using a backslash (\').
    2. The program is unable to detect changes to any TBLPROPERTIES other than the following default properties:
      'classification', 'has_encrypted_data', 'orc.compress', 'parquet.compress', 'write.compression', 
      'projection.*', 'skip.header.line.count', 'storage.location.template'
    3. Because comparison of DDL columns list with metadata columns list is done after converting both sets to
       lowercase, differences between upper- and lower-case letters in columns names, data types or comments are 
       not identified as changes.

"""
PGM_NAME = 'app.py'
MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
current_date = datetime.datetime.now()
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger(PGM_NAME + '_' + current_date.strftime('%Y-%m-%d-%H-%M-%S'))

s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')
athena_client = boto3.client('athena')
glue_client = boto3.client('glue')

# Each CREATE EXTERNAL TABLE statement defined in a DDL text file can be segmented based
# on a set of standard clauses as declared in the list below.
# Note that, with the exception of 'location', all other clauses are optional and may not exist
# in the DDL text.
ddl_clauses_list = [' comment \'', ' partitioned by ', ' clustered by ', ' row format ', ' stored as ',
                    ' location \'s3:', ' tblproperties ']

# The ROW FORMAT clause in a DDL may further have additional sub-clauses as declared in the list below.
# ROW FORMAT DELIMITED and ROW FORMAT SERDE are mutually exclusive.
ddl_row_format_subclauses_list = [' fields terminated by \'', ' escaped by \'',
                                  ' collection items terminated by \'',
                                  ' map keys terminated by \'',
                                  ' lines terminated by \'',
                                  ' null defined as \'',
                                  ' serde \'', ' with serdeproperties (',
                                  ' delimited ']

NOT_FOUND = -1
AVRO_STORAGE_FORMAT = 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
ORC_STORAGE_FORMAT = 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
PARQUET_STORAGE_FORMAT = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
RCFILE_STORAGE_FORMAT = 'org.apache.hadoop.hive.ql.io.RCFileInputFormat'
SEQUENCEFILE_STORAGE_FORMAT = 'org.apache.hadoop.mapred.SequenceFileInputFormat'
TEXTFILE_STORAGE_FORMAT = 'org.apache.hadoop.mapred.TextInputFormat'
ROW_FORMAT_WITH_SERDEPROPERTIES = 'with serdeproperties'
ROW_FORMAT_SERDE = 'serde'
ROW_FORMAT_DELIMITED = 'delimited'


def create_folders(config_dict, stack_info_obj, product_name, environment_name):
    """
    Create folders within S3 buckets based on the information provided in
    the application configuration JSON file.

    Parameters
    ----------
        config_dict: dictionary
            The dictionary reflecting the application configuration JSON file.
        stack_info_obj: object
            Reference to the stack_info object.
        product_name: str
            Name of the application for which to create the folders.
        environment_name: str
            Name of the environment for which to create the folders.
    Returns
    -------
        None
    Exceptions
    ----------
        None
    """
    # Create subfolders for various buckets
    logger.info('Creating non-existing bucket folders...')
    for config in config_dict['folders']:
        folder_name = config['folder_name']
        bucket_label = config['label']
        bucket_name = stack_info_obj.get_bucket_name_by_label(product_name, environment_name, bucket_label)
        logger.debug('Creating folder: {}'.format(bucket_name + '/' + folder_name))
        s3.put_object(Bucket=bucket_name, Key=(folder_name + '/'))


def create_database(product_name, db_name, db_location, output_bucket_name):
    """
    Based on the information provided in the application configuration JSON file create the
    Athena database, if it does not already exist.

    Parameters
    ----------
        db_name: str
            The name of Athena database.
        db_location: str
            The S3 path of the database location.
        output_bucket_name: str
            Athena output bucket name.
    Returns
    -------
        None
    Exceptions
    ----------
        None
    """
    db_list = list_existing_databases()

    if db_name in db_list:
        logger.info("Database: {} already exists -- skipping database creation".format(db_name))
    else:
        logger.info('Creating database: {}'.format(db_name))
        if db_location is None:
            query_string = 'CREATE DATABASE IF NOT EXISTS {}'.format(db_name)
        else:
            query_string = 'CREATE DATABASE IF NOT EXISTS {} {}'.format(db_name, db_location)

        execute_query(product_name, query_string, 's3://' + output_bucket_name + '/_temporary/tables/')


def list_existing_databases():
    """
    Construct a list of existing Athena databases. The list is then used to determine if a database already
    exists and does not need to be created.

    Parameters
    ----------
        N/A
    Returns
    -------
        db_list: list
            A list of existing database names.
    Exceptions
    ----------
        None
    """
    starting_token = None
    paginator = athena_client.get_paginator('list_databases')
    response_iterator = paginator.paginate(
        CatalogName='AwsDataCatalog',
        PaginationConfig={
            'StartingToken': starting_token
        }
    )

    db_list = list()
    for page in response_iterator:
        for table_name in page['DatabaseList']:
            db_list.append(table_name['Name'])

    return db_list


def process_athena_tables(config_dict, output_bucket_name, app_bucket_name, db_name,
                          stack_info_obj, product_name, environment_name):
    """
    Create tables/views based on the information provided in the application configuration JSON file.

    Parameters
    ----------
        config_dict: dictionary
            The dictionary reflecting the application configuration JSON file.
        output_bucket_name: str
            Name of the bucket where query execution output should be stored.
        app_bucket_name: str
            Name of the bucket where create table/view DDL templates are stored.
        db_name: str
            Name of the database in which the table's/view's existence should be checked.
        stack_info_obj: object
            Reference to the stack_info object.
        product_name: str
            Name of the application for which to create the folders.
        environment_name: str
            Name of the environment for which to create the folders.
    Returns
    -------
        None
    Exceptions
    ----------
        None
    """
    logger.info('Processing Athena tables/views...')

    tables_metadata_list = construct_tables_metadata_list(db_name)

    for table_config in config_dict['athena_tables']:
        # Use list comprehension to find the corresponding metadata for the table in the above list.
        table_metadata = [element for element in tables_metadata_list if element['Name'] == table_config['table_name']]
        ddl_text = str(prep_ddl_script(table_config, output_bucket_name, app_bucket_name, db_name,
                                       stack_info_obj, product_name, environment_name))

        if len(table_metadata) == 0:
            logger.debug('Table: {} does not exist'.format(table_config['table_name']))
            logger.debug('table_metadata={}'.format(table_metadata))
            create_table(table_config, ddl_text, db_name, stack_info_obj, product_name, environment_name)
        else:
            # Due to complexities detecting changes in the views, we replace all views each time regardless.
            # Note that for a view to be recreated properly, the DDL for the view must include a
            # "CREATE OR REPLACE VIEW" clause.
            if table_metadata[0]['TableType'] == 'VIRTUAL_VIEW':
                logger.debug('Creating or replacing view: {}'.format(table_config['table_name']))
                create_table(table_config, ddl_text, db_name, stack_info_obj, product_name, environment_name)
            else:
                logger.debug('Table: {} already exists'.format(table_config['table_name']))
                logger.debug('table_metadata={}'.format(table_metadata))
                detect_table_changes(table_metadata[0], table_config, ddl_text, db_name,
                                     stack_info_obj, product_name, environment_name)


def construct_tables_metadata_list(db_name):
    """
    Construct a list of metadata dictionaries for all existing tables/views in the target database.
    The list is then used to determine if the specified table/view already exists and can be skipped,
    as opposed to creating the table/view. For existing tables, the metadata is also used to detect schema changes.

    Parameters
    ----------
        db_name: str
            Name of the database in which the table's existence should be checked.
    Returns
    -------
        tables_metadata_list: list
            A list of tables metadata dictionaries for existing table/views in the target database.
    Exceptions
    ----------
        None
    """
    starting_token = None
    # paginator = athena_client.get_paginator('list_table_metadata')
    paginator = glue_client.get_paginator('get_tables')
    response_iterator = paginator.paginate(
        DatabaseName=db_name,
        PaginationConfig={
            'StartingToken': starting_token
        }
    )

    tables_metadata_list = list()
    for page in response_iterator:
        for element in page['TableList']:
            tables_metadata_list.append(element)

    return tables_metadata_list


def prep_ddl_script(table_config, output_bucket_name, app_bucket_name, db_name,
                    stack_info_obj, product_name, environment_name):
    """
    Obtain the DDL script for each Athena table from S3, substitute DDL parameters (signified by enclosing
    %% characters) with actual values and write out the revised DDL to S3.

    Parameters
    ----------
        table_config: dictionary
            The dictionary reflecting the table configuration from application JSON file.
        output_bucket_name: str
            Name of the bucket where query execution output should be stored.
        app_bucket_name: str
            Name of the bucket where table create DDL templates are stored.
        db_name: str
            Name of the database in which the table's existence should be checked.
        stack_info_obj: object
            Reference to the stack_info object.
        product_name: str
            Name of the application for which to create the folders.
        environment_name: str
            Name of the environment for which to create the folders.
    Returns
    -------
        The original DDL text with parameters (signified by enclosing %% characters) substituted with actual values.
    Exceptions
    ----------
        None
    """
    logger.info('Processing table={}'.format(table_config['table_name']))
    logger.debug('DDL Script_name: {}'.format(table_config['script_name']))
    logger.debug('Table folder: {}'.format(table_config['table_folder']))
    logger.debug('DDL output location before parameter substitution: {}'.format('s3://' + app_bucket_name + '/' +
                                                                                table_config['sql_folder1']))
    logger.debug('DDL output location after parameter substitution: {}'.format('s3://' +
                                                                               stack_info_obj.get_bucket_name_by_label(
                                                                                   product_name, environment_name,
                                                                                   'output') + '/' +
                                                                               table_config['sql_folder2']))

    location_clause = 's3://' + stack_info_obj.get_bucket_name_by_label(
        product_name, environment_name,
        table_config['label']) + table_config['location_dir'] + '/' + table_config['table_name']

    # Obtain DDL script with embedded parameters from sql_folder1
    obj = s3_resource.Object(app_bucket_name,
                             table_config['sql_folder1'] + '/' + table_config['script_name'])
    body = obj.get()['Body'].read().decode('utf-8')

    # Replace %%LOCATION%% (not applicable to views) and %%DATABASE%% parameters in DDL script with actual values,
    # strip any comments (signified by --) and put the output file in the target S3 bucket.
    body = body.replace('%%LOCATION%%', location_clause).replace('%%DATABASE%%', db_name)
    body = strip_comments(body)

    s3_resource.Object(output_bucket_name, table_config['sql_folder2'] + '/' +
                       table_config['script_name']).put(Body=body)
    return str(body)


def strip_comments(adhoc_text):
    """
    A general function that accepts ASCII source text and returns a copy of the text that is stripped
    of all SQL comments starting with "--".

    Parameters
    ----------
        adhoc_text: str
            Any text from which comments (signified by --) need to be removed, in this case the DDL text.
    Returns
    -------
        The original text with all comments removed.
    Exceptions
    ----------
        None
    """
    # regex explanation:
    # \s    any whitespace character
    # *     zero or more consecutive spaces
    # --    matches two dashes that signify an SQL comment
    # .     matches any character (except for line terminators)
    # \n    matches a line-feed (newline) character (ASCII 10)
    # ?     matches the previous token between zero and one times, as many times as possible,
    #       giving back as needed (greedy)
    return re.sub('\s*--.*\n?', '', adhoc_text, flags=re.MULTILINE)


def detect_table_changes(table_metadata, table_config, ddl_text, db_name,
                         stack_info_obj, product_name, environment_name):
    """
    Compare the CREATE EXTERNAL TABLE statement in the DDL against the table's metadata to determine
    if table structure and/or any table properties have changed, requiring the table to be recreated.

    Parameters
    ----------
        table_metadata: list
            Dictionaries of metadata for existing tables/views.
        table_config: dictionary
            The dictionary reflecting the table configuration from application JSON file.
        ddl_text: str
            Full text contained in the DDL script that is read from S3.
        db_name: str
            Name of the database in which the table/view changes should be detected.
        stack_info_obj: object
            Reference to the stack_info object.
        product_name: str
            Name of the application for which to create the folders.
        environment_name: str
            Name of the environment for which to create the folders.
    Returns
    -------
        None
    Exceptions
    ----------
        None
    """
    logger.debug("Now attempting to detect changes to the existing table: {}".format(table_config['table_name']))

    # To facilitate DDL text parsing, remove line-feeds, carriage-returns, back tics, semi colons
    # and any single space before closing parentheses. Also, replace tabs with a single space.
    normalized_ddl_text = ddl_text.replace('\n', ' ').replace('\r', '').replace('`', '').replace(';', ''). \
        replace('\t', ' ').replace(' )', ')').lower()

    # Replace multiple spaces in the DDL text with a single space to facilitate DDL text parsing.
    while '  ' in normalized_ddl_text:
        normalized_ddl_text = normalized_ddl_text.replace('  ', ' ').replace(' ,', ',')

    # DDL can be coded using single quotes, double quotes or both. To facilitate parsing the DDL, convert
    # all enclosing double quotes to single quotes, but preserve the non-enclosing (embedded) double quotes.
    # For example, we want to convert:
    # fields terminated by "\"" escaped by "\""
    # to:
    # fields terminated by '\"' escaped by '\"'
    # However, due to complexities with properly converting only surrounding double quotes to single
    # quotes this feature has not been implemented. The current version of the program expects values
    # to be surrounded in single quotes only.
    # Tried using regex to convert enclosing double quotes to single quotes, but could not formulate
    # the correct regex, neither could devise a Python function. The following regex pattern does not
    # preserve the escaped double quotes, as shown in the above example.
    # normalized_ddl_text = re.sub('"([^\"]*(?:\\.[^\\\"]*)*)"', r"'\1'", normalized_ddl_text)
    #
    #  For some unknown reason the embedded double quotes become single quotes using the replaces below.
    #  Example: fields terminated by "\"" escaped by "\"" --becomes--> fields terminated by '\'' escaped by '\''
    # normalized_ddl_text = normalized_ddl_text.replace('"', "'").replace("'''", "'\\\"'").replace("\\\'", "'")

    '''
    # Attempted the following loop to convert the enclosing double quotes to single quotes, but
    # the logic does not convert the closing (right most) double quotes.
    ddl_reformatted_text = ''
    for i in range(len(normalized_ddl_text)):
        logger.debug("normalized_ddl_text[{}]={}".format(i, normalized_ddl_text[i]))
        if normalized_ddl_text[i] == '"':
            if normalized_ddl_text[i-1] != '\\' and normalized_ddl_text[i-1] != "'":
                ddl_reformatted_text += "'"
            else:
                ddl_reformatted_text += normalized_ddl_text[i]
        else:
            ddl_reformatted_text += normalized_ddl_text[i]

    logger.debug('ddl_reformatted_text={}'.format(ddl_reformatted_text))
    '''

    if has_table_structure_changed(normalized_ddl_text, table_metadata) is True:
        logger.info("Structure of the existing table={} has changed. Recreating the table.".
                    format(table_config['table_name']))

        drop_table(db_name, table_config['table_name'], table_metadata['TableType'], table_config['temp_folder'],
                   stack_info_obj, product_name, environment_name)
        create_table(table_config, ddl_text, db_name, stack_info_obj, product_name, environment_name)
    else:
        logger.info("Structure of the existing table={} has not changed. Not recreating the table.".
                    format(table_config['table_name']))


def has_table_structure_changed(ddl_text, table_metadata):
    """
    Determine if the Athena table's structure has changed.

    Parameters
    ----------
        ddl_text: str
            Text of the CREATE EXTERNAL TABLE statement in the table's DDL.
        table_metadata: Dictionary
            Athena table's metadata in the form of JSON that is obtained by calling get_tables Glue boto3 API.

    Returns
    -------
        Boolean True when changes are detected or False when no changes are detected.
    Exceptions
    ----------
        None
    """
    # To facilitate parsing of the DDL, split the columns list segment of the DDL
    # from the other segments.
    ddl_columns_segment, ddl_other_segments = split_ddl_columns_segment(ddl_text)
    ddl_other_segments = ddl_other_segments.strip()
    logger.debug("ddl_columns_segment=<{}>".format(ddl_columns_segment))
    logger.debug("ddl_other_segments=<{}>".format(ddl_other_segments))

    ddl_clause_list_sorted = index_ddl_clauses(ddl_other_segments, ddl_clauses_list)
    logger.debug("ddl_clause_list_sorted={}".format(ddl_clause_list_sorted))

    if have_columns_changed(table_metadata, ddl_columns_segment) is True:
        return True

    # Create a list of DDL segments other than the columns list based on clauses in the DDL text.
    ddl_segments = segmentize_ddl_by_clause(ddl_other_segments, ddl_clause_list_sorted)
    logger.debug("ddl_segments={}".format(ddl_segments))

    if has_table_comment_changed(table_metadata, ddl_segments) is True:
        return True

    if has_partitioning_changed(table_metadata, ddl_segments) is True:
        return True

    if has_clustering_changed(table_metadata, ddl_segments) is True:
        return True

    if has_row_format_changed(table_metadata, ddl_segments) is True:
        return True

    if has_file_format_changed(table_metadata, ddl_segments) is True:
        return True

    if has_table_location_changed(table_metadata, ddl_segments) is True:
        return True

    if have_tblproperties_changed(table_metadata, ddl_segments) is True:
        return True

    return False


def split_ddl_columns_segment(ddl_text):
    """
    To facilitate parsing of the DDL, split the columns list segment of the DDL from the other segments.

    Parameters
    ----------
        ddl_text: str
            Text of the CREATE EXTERNAL TABLE statement in the table's DDL.
    Returns
    -------
        ddl_columns_segment: str
            Segment of the DDL that includes the columns list exclusively.
        DDL segments other than the columns list: str
    Exceptions
    ----------
        Raised if the parenthesis that surround the columns list and the nested parenthesis contained
        in the list are not balanced.
    """
    # Obtain the list of columns in the DDL based on the outermost opening and closing parenthesis that surround
    # the list of columns, their data types and optional comments. Note that there may be nested parenthesis in
    # the columns list for some data types such as decimal(precision, scale).
    stack = []  # A python stack of indices signifying the position of the opening parenthesis
    index_dict = {}  # A dictionary of indexes of open/close parenthesis pairs

    start_pos = 0
    end_pos = 0
    p_count = 0
    for i, c in enumerate(ddl_text):
        if c == '(':
            stack.append(i)
            if start_pos == 0:
                start_pos = i
            p_count += 1
        elif c == ')':
            try:
                index_dict[stack.pop()] = i
                p_count -= 1
                if p_count == 0:
                    end_pos = i
                    break
            except IndexError:
                logger.error('Encountered unbalanced parentheses while parsing the columns list in DDL -- '
                             'Check the DDL for syntax errors!')
                raise IndexError

    if stack:  # check if stack is empty afterwards; typically signifies mismatched opening & closing parenthesis.
        raise Exception('Encountered unbalanced parentheses while parsing the columns list in DDL -- '
                        'Check the DDL for syntax errors!')

    # Obtain the position of parenthesis that surround the columns list in DDL.
    columns_list_start_end = list(index_dict.items())[-1]

    # Obtain the columns list based on the position of the surrounding parenthesis. See example DDL below.
    # CREATE EXTERNAL TABLE IF NOT EXISTS db_name.table_name ( columns_list )
    #                                                        ^              ^
    ddl_columns_segment = ddl_text[columns_list_start_end[0] + 1: columns_list_start_end[1]].strip()

    return ddl_columns_segment, ddl_text[columns_list_start_end[1] + 1:]


def index_ddl_clauses(ddl_segment_text, clauses_list):
    """
    Capture the starting position (i.e., index) of each clause that was found in the CREATE EXTERNAL TABLE
    statement in the DDL.

    Parameters
    ----------
        ddl_segment_text: str
            Text of the CREATE EXTERNAL TABLE statement in the table's DDL.
        clauses_list: List
            A list of clauses that were found in the DDL.
    Returns
    -------
        ddl_clause_list_sorted:
            A list of clauses found in the DDL that are sorted based on their starting position.
    Exceptions
    ----------
        None
    """
    ddl_clause_dict = dict()
    ddl_clause_list = list()

    # Based on the DDL clauses list (refer to ddl_clauses_list and ddl_row_format_subclauses_list),
    # build a dictionary of the standard DDL segments that are present in the ddl_segment_text and
    # their starting position (i.e., Index). Clauses not present in the DDL get an index of
    # NOT_FOUND (i.e., -1).
    for clause in clauses_list:
        ddl_clause_dict['Clause'] = clause.strip()
        clause_index = ddl_segment_text.find(ddl_clause_dict['Clause'])
        # Must differentiate between the optional table COMMENT segment and any column comment in the
        # PARTITIONED BY segment. In a syntactically correct DDL, if a table COMMENT segment
        # exists it must appear immediately after the columns list (i.e., clause index of table
        # COMMENT if exists will always be zero). The if statement below avoids column comments
        # in the PARTITIONED BY segment being treated as table COMMENT.
        if ddl_clause_dict['Clause'] == "comment '":
            logger.debug("ddl_clause_dict['Clause']={}".format(ddl_clause_dict['Clause']))
            logger.debug("clause_index={}".format(clause_index))
            if clause_index == 0:
                ddl_clause_dict['Index'] = clause_index
            else:
                ddl_clause_dict['Index'] = NOT_FOUND
        else:
            if clause_index >= 0:
                ddl_clause_dict['Index'] = clause_index
            else:
                ddl_clause_dict['Index'] = NOT_FOUND
        dictionary_copy = ddl_clause_dict.copy()
        ddl_clause_list.append(dictionary_copy)

    # Sort the DDL clauses in the list based on their starting position (i.e., Index).
    ddl_clause_list_sorted = sorted(ddl_clause_list, key=lambda x: x['Index'])

    return ddl_clause_list_sorted


def segmentize_ddl_by_clause(ddl_text, clauses_list_sorted):
    """
    Break the DDL text, excluding the columns list, into segments based on the standard
    clauses that were found in the CREATE EXTERNAL TABLE statement in the DDL (refer to:
    https://docs.aws.amazon.com/athena/latest/ug/create-table.html).

    Parameters
    ----------
        ddl_text: str
            Text of the CREATE EXTERNAL TABLE statement, excluding the columns list, in the table's DDL.
        clauses_list_sorted: List
            A list of clauses, other than the columns list, found in the DDL that are sorted based on their
            starting position.
    Returns
    -------
        ddl_segments: list
            A list of segments into which the DDL statement has been divided.
    Exceptions
    ----------
        None
    """
    logger.debug("ddl_text={}".format(ddl_text))
    logger.debug("clauses_list_sorted={}".format(clauses_list_sorted))

    # Break the DDL text into segments based on entries in the sorted clauses list.
    ddl_segments = []
    for i in range(0, len(clauses_list_sorted) - 1):
        if clauses_list_sorted[i]['Index'] != NOT_FOUND:
            ddl_segments.append(ddl_text[clauses_list_sorted[i]['Index']:
                                         clauses_list_sorted[i + 1]['Index']].strip())

    # Include the last DDL segment.
    if clauses_list_sorted[i + 1]['Index'] != NOT_FOUND:
        ddl_segments.append(ddl_text[clauses_list_sorted[i + 1]['Index']: len(ddl_text)].strip())

    return ddl_segments


def have_columns_changed(table_metadata, ddl_columns):
    """
    Determine if the Athena table's columns structure has changed by comparing the columns list
    segment in the DDL against the corresponding metadata JSON segments:
    "StorageDescriptor": {"Columns": [{"Name": "...", "Type": "...", "Comment": "..."},...]}

    Parameters
    ----------
        table_metadata: Dictionary
            Athena table's metadata in the form of JSON that is obtained by calling get_tables Glue boto3 API.
        ddl_columns: List
            A list of columns and their data types and comments that is extracted from the DDL text.
    Returns
    -------
        Boolean True when changes are detected or False when no changes are detected.
    Exceptions
    ----------
        None
    """
    meta_columns = ''
    for column in table_metadata['StorageDescriptor']['Columns']:
        try:
            meta_columns += ' ' + str(column['Name']) + ' ' + str(column['Type']) + " comment '" + \
                            column['Comment'].replace("'", "\\'") + "'" + ','
        except KeyError:
            meta_columns += ' ' + str(column['Name']) + ' ' + str(column['Type']) + ','

    meta_columns = meta_columns[:-1].strip() + ''
    # Replace multiple spaces in the list of metadata columns with a single space to avoid false
    # differences when comparing with DDL columns list.
    while '  ' in meta_columns:
        meta_columns = meta_columns.replace('  ', ' ').replace(' ,', ',')

    logger.debug("meta_columns={}".format('<' + meta_columns + '>'))

    if ddl_columns == meta_columns.lower():
        logger.debug("Metadata columns list=\n{}\nis the same as DDL columns list=\n{}".format(meta_columns,
                                                                                               ddl_columns))
        return False
    else:
        logger.info("Metadata columns list=\n{}\nis different from DDL columns list=\n{}".format(meta_columns,
                                                                                                 ddl_columns))
        return True


def has_table_comment_changed(table_metadata, ddl_segments):
    """
    Determine if the Athena table's comment has changed by comparing the COMMENT
    segment in the DDL against the corresponding metadata JSON segments:
    "Parameters": {"EXTERNAL": "...", "comment": "...", "transient_lastDdlTime": "..."}

    Parameters
    ----------
        table_metadata: Dictionary
            Athena table's metadata in the form of JSON that is obtained by calling get_tables Glue boto3 API.
        ddl_segments: List
            List of standard segments that are found in the DDL.
    Returns
    -------
        Boolean True when changes are detected or False when no changes are detected.
    Exceptions
    ----------
        None
    """
    ddl_comment_segment = next((segment for segment in ddl_segments if segment.startswith('comment')), None)
    logger.debug("ddl_comment_segment={}".format(ddl_comment_segment))

    try:
        metadata_comment = table_metadata['Parameters']['comment'].replace("'", "\\'").lower()
        logger.debug("metadata_comment={}".format(metadata_comment))
    except KeyError:
        # Because table comment is an optional parameter and may be missing in the metadata JSON,
        # we need to handle the KeyError exception.
        metadata_comment = None

    if metadata_comment is None:
        if ddl_comment_segment is None:
            logger.debug("Metadata and DDL don't have a table comment")
            return False
        else:
            logger.info("Metadata doesn't have a table comment but DDL table comment={}".
                        format(ddl_comment_segment.split(' ', 1)[1]))
            return True
    else:
        if ddl_comment_segment is None:
            logger.info("Metadata table comment={} but DDL doesn't have a table comment".format(metadata_comment))
            return True
        else:
            ddl_segment_key, ddl_segment_value = ddl_comment_segment.split(' ', 1)
            ddl_segment_value = ddl_segment_value.strip("'")
            if metadata_comment == ddl_segment_value:
                logger.debug("Metadata table comment={} is the same as DDL table comment={}".
                             format(metadata_comment, ddl_segment_value))
                return False
            else:
                logger.info("Metadata table comment={} is different from DDL table comment={}".
                            format(metadata_comment, ddl_segment_value))
                return True


def has_partitioning_changed(table_metadata, ddl_segments):
    """
    Determine if the Athena table's partitioning has changed by comparing the PARTITIONED BY
    segment in the DDL against the corresponding metadata JSON segment:
    "PartitionKeys": [{"Name": "...", "Type": "...", "Comment": "..."},...]}

    Parameters
    ----------
        table_metadata: Dictionary
            Athena table's metadata in the form of JSON that is obtained by calling get_tables Glue boto3 API.
        ddl_segments: List
            List of standard segments that are found in the DDL.
    Returns
    -------
        Boolean True when changes are detected or False when no changes are detected.
    Exceptions
    ----------
        None
    """
    ddl_partitioning_segment = next((segment for segment in ddl_segments if segment.startswith('partitioned by')), None)

    metadata_partition = ''
    if len(table_metadata['PartitionKeys']) == 0:
        if ddl_partitioning_segment is None:
            logger.debug("Metadata and DDL don't have partitioning")
            return False
        else:
            logger.info("Metadata doesn't have partitioning but DDL does")
            return True
    else:
        if ddl_partitioning_segment is None:
            logger.info("Metadata has partitioning but DDL doesn't")
            return True
        else:
            ddl_segment_key, ddl_segment_value = ddl_partitioning_segment.split('(', 1)
            # Remove the closing parentheses in the PARTITIONED BY clause & remove spaces at
            # the beginning and at the end of the string.
            ddl_segment_value = ddl_segment_value[:-1]
            ddl_segment_value = ddl_segment_value.strip()

            for i, element in enumerate(table_metadata['PartitionKeys']):
                try:
                    if i == 0:
                        metadata_partition = element['Name'].lower() + ' ' + element['Type'].lower() + \
                                             " comment '" + element['Comment'].replace("'", "\\'").lower() + "'"
                    else:
                        metadata_partition += ', ' + element['Name'].lower() + ' ' + element['Type'].lower() + \
                                              " comment '" + element['Comment'].replace("'", "\\'").lower() + "'"
                except KeyError:
                    # The metadata may include optional comments for the partitioned by columns under PartitionKeys
                    # JSON element. Because the comments are optional, we need to handle them using KeyError.
                    if i == 0:
                        metadata_partition = element['Name'].lower() + ' ' + element['Type'].lower()
                    else:
                        metadata_partition += ', ' + element['Name'].lower() + ' ' + element['Type'].lower()

            if metadata_partition == ddl_segment_value:
                logger.debug("Metadata partitioning={} is the same as DDL partitioning={}".format(
                    metadata_partition, ddl_segment_value))
                return False
            else:
                logger.info("Metadata partitioning={} is different from DDL partitioning={}".format(
                    metadata_partition, ddl_segment_value))
                return True


def has_clustering_changed(table_metadata, ddl_segments):
    """
    Determine if the Athena table's clustering has changed by comparing the CLUSTERED BY
    segment in the DDL against the corresponding metadata JSON segments:
    "StorageDescriptor": {"BucketColumns": ["column-name", ...]}
    "StorageDescriptor": {"NumberOfBuckets": x}

    Parameters
    ----------
        table_metadata: Dictionary
            Athena table's metadata in the form of JSON that is obtained by calling get_tables Glue boto3 API.
        ddl_segments: List
            List of standard segments that are found in the DDL.
    Returns
    -------
        Boolean True when changes are detected or False when no changes are detected.
    Exceptions
    ----------
        None
    """
    ddl_clustering_segment = next((segment for segment in ddl_segments if segment.startswith('clustered by')), None)
    logger.debug("ddl_clustering_segment={}".format(ddl_clustering_segment))

    metadata_cluster = ''
    if len(table_metadata['StorageDescriptor']['BucketColumns']) == 0:
        if ddl_clustering_segment is None:
            logger.debug("Metadata and DDL don't have clustering")
            return False
        else:
            logger.info("Metadata doesn't have clustering but DDL does")
            return True
    else:
        if ddl_clustering_segment is None:
            logger.info("Metadata has clustering but DDL doesn't")
            return True
        else:
            ddl_segment_key, ddl_segment_value = ddl_clustering_segment.split('(', 1)
            ddl_segment_value = ddl_segment_value.replace(')', '').strip()
            metadata_bucket_columns = table_metadata['StorageDescriptor']['BucketColumns']

            for i, element in enumerate(metadata_bucket_columns):
                if i == 0:
                    metadata_cluster = element.lower()
                else:
                    metadata_cluster += ', ' + element.lower()

            metadata_cluster += ' into ' + str(table_metadata['StorageDescriptor']['NumberOfBuckets']) + ' buckets'

            if metadata_cluster == ddl_segment_value:
                logger.debug("Metadata clustering={} is the same as DDL clustering={}".format(
                    metadata_cluster, ddl_segment_value))
                return False
            else:
                logger.info("Metadata clustering={} is different from DDL clustering={}".format(
                    metadata_cluster, ddl_segment_value))
                return True


def has_row_format_changed(table_metadata, ddl_segments):
    """
    Determine if the Athena table's ROW FORMAT structure has changed by comparing the ROW FORMAT
    segment in the DDL against the corresponding metadata JSON segment:
    "StorageDescriptor": {"SerdeInfo": {"SerializationLibrary": "org.apache...", "Parameters": {...}}}

    Note 1: Below are the two possible options for ROW FORMAT clause in the DDL. Note the sub-clauses
    listed in square brackets []:
          1. ROW FORMAT SERDE [WITH SERDEPROPERTIES]
          2. ROW FORMAT DELIMITED
                [FIELDS TERMINATED BY char [ESCAPED BY char]]
                [COLLECTION ITEMS TERMINATED BY char]
                [MAP KEYS TERMINATED BY char]
                [LINES TERMINATED BY char]
                [NULL DEFINED AS char]

    Note 2: Both ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' with no SERDEPROPERTIES and
    ROW FORMAT DELIMITED with no row_format properties result in metadata:
        ['SerdeInfo']['SerializationLibrary'] = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
        and
        ['SerdeInfo']['Parameters']['serialization.format'] = "1"

    Parameters
    ----------
        table_metadata: Dictionary
            Athena table's metadata in the form of JSON that is obtained by calling get_tables Glue boto3 API.
        ddl_segments: List
            List of standard segments that are found in the DDL.
    Returns
    -------
        Boolean True when changes are detected or False when no changes are detected.
    Exceptions
    ----------
        None
    """
    ddl_row_format_segment = next((segment for segment in ddl_segments if segment.startswith('row format ')), None)
    logger.debug("ddl_row_format_segment={}".format(ddl_row_format_segment))

    # When DDL does not include a ROW FORMAT clause, check to see if metadata includes non-default
    # ROW FORMAT properties.
    if ddl_row_format_segment is None:
        if has_row_format_serde_changed(table_metadata) is True:
            return True
        else:
            return have_row_format_delimiters_changed(table_metadata)
    else:
        ddl_row_format_subclauses_list_sorted = index_ddl_clauses(ddl_row_format_segment,
                                                                  ddl_row_format_subclauses_list)
        logger.debug("ddl_row_format_subclauses_list_sorted={}".format(ddl_row_format_subclauses_list_sorted))
        ddl_row_format_sub_segments = segmentize_ddl_by_clause(ddl_row_format_segment,
                                                               ddl_row_format_subclauses_list_sorted)
        logger.debug("ddl_row_format_sub_segments={}".format(ddl_row_format_sub_segments))

        ddl_row_format_type = determine_row_format_type(ddl_row_format_subclauses_list_sorted)
        if ddl_row_format_type == ROW_FORMAT_SERDE or ddl_row_format_type == ROW_FORMAT_WITH_SERDEPROPERTIES:
            return has_row_format_serde_changed(table_metadata, ddl_row_format_sub_segments, ddl_row_format_type)
        else:
            return check_row_format_delimited_properties(ddl_row_format_sub_segments, table_metadata)


def determine_row_format_type(ddl_row_format_subclauses_list_sorted):
    """
    In cases when the DDL includes a ROW FORMAT clause, determine which of the following three types it is:
    1. ROW FORMAT SERDE
    2. ROW FORMAT SERDE WITH SERDEPROPERTIES ("property_name" = "property_value", [, ...] )
    3. ROW FORMAT DELIMITED row_format

    Parameters
    ----------
        ddl_row_format_subclauses_list_sorted: list
            A list of row format sub-clause key/value pairs.

    Returns
    -------
        A string indicating one of the above ROW FORMAT types.
    Exceptions
    ----------
        Raised when DDL incorrectly includes a ROW FORMAT clause without any subclauses.
    """
    ddl_row_format_type = None
    # Determine if DDL includes ROW FORMAT SERDE, ROW FORMAT SERDE WITH SERDEPROPERTIES or
    # ROW FORMAT DELIMITED clauses.
    for i in range(len(ddl_row_format_subclauses_list_sorted)):
        if ddl_row_format_subclauses_list_sorted[i]['Clause'] == 'with serdeproperties (' \
                and ddl_row_format_subclauses_list_sorted[i]['Index'] != NOT_FOUND:
            ddl_row_format_type = ROW_FORMAT_WITH_SERDEPROPERTIES
            break
        elif ddl_row_format_subclauses_list_sorted[i]['Clause'] == "serde '" \
                and ddl_row_format_subclauses_list_sorted[i]['Index'] != NOT_FOUND:
            ddl_row_format_type = ROW_FORMAT_SERDE
            # We don't break out of the loop in case ROW FORMAT is SERDE WITH SERDEPROPERTIES
        elif ddl_row_format_subclauses_list_sorted[i]['Clause'] == 'delimited' \
                and ddl_row_format_subclauses_list_sorted[i]['Index'] != NOT_FOUND:
            ddl_row_format_type = ROW_FORMAT_DELIMITED
            break

    if ddl_row_format_type is None:
        raise Exception("Unable to find any of the expected ROW FORMATS={}, {}, or {} in the DDL.".
                        format(ROW_FORMAT_SERDE, ROW_FORMAT_WITH_SERDEPROPERTIES, ROW_FORMAT_DELIMITED))

    return ddl_row_format_type


def has_row_format_serde_changed(table_metadata, ddl_serde_info=None, ddl_row_format_type=None, ):
    """
    Determine if the Athena table's ROW FORMAT SERDE has changed.
    Note: When DDL does not include a ROW FORMAT clause and the existing table is specified with
          the default ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe', no
          change will be detected.

    Parameters
    ----------
        ddl_serde_info: str
            Row format serde information as found in the DDL.
        ddl_row_format_type: str
            Type of ROW FORMAT segment in the DDL (i.e., 'serde' or 'serde with serdeproperties'
        table_metadata: Dictionary
            Athena table's metadata in the form of JSON that is obtained by calling get_tables Glue boto3 API.
    Returns
    -------
        Boolean True when changes are detected or False when no changes are detected.
    Exceptions
    ----------
        None
    """
    table_metadata_serialization_lib = table_metadata['StorageDescriptor']['SerdeInfo']['SerializationLibrary']
    logger.debug("table_metadata_serialization_lib={}".format(table_metadata_serialization_lib))

    if ddl_serde_info is None:
        # For a list of supported Athena SerDes refer to:
        # https://docs.aws.amazon.com/athena/latest/ug/lazy-simple-serde.html.
        # For a list of default org.apache SerDe classes refer to:
        # https://hive.apache.org/javadocs/r1.2.2/api/org/apache/hadoop/hive/serde2/Deserializer.html
        if table_metadata_serialization_lib.startswith('org.apache.'):
            logger.debug("DDL does not include a ROW FORMAT SERDE clause, but because the metadata"
                         " SerializationLibrary={} is the default, no change is detected.".
                         format(table_metadata_serialization_lib))
            return False
        else:
            logger.info("DDL does not include a ROW FORMAT SERDE clause but because metadata has a non-default"
                        " SerializationLibrary={}, a change is detected".
                        format(table_metadata_serialization_lib))
            return True

    if ddl_row_format_type == ROW_FORMAT_WITH_SERDEPROPERTIES:
        ddl_serde_lib = ddl_serde_info[0].split(' ')[1].strip("'")
        ddl_serdeproperties = ddl_serde_info[1].split(' (')[1].strip(')')
    elif ddl_row_format_type == ROW_FORMAT_SERDE:
        ddl_serde_lib = ddl_serde_info[0].split(' ')[1].strip("'")
        ddl_serdeproperties = None
    else:
        raise Exception("Encountered unexpected value for ddl_row_format_type function argument. "
                        "Please investigate!")

    logger.debug("ddl_serde_lib={}".format(ddl_serde_lib))
    logger.debug("ddl_serdeproperties={}".format(ddl_serdeproperties))

    if ddl_serdeproperties is not None and \
            have_serdeproperties_changed(ddl_serdeproperties, table_metadata) is True:
        return True

    if ddl_serde_lib == table_metadata_serialization_lib.lower():
        logger.debug("DDL's ROW FORMAT SERDE={} is the same as the metadata SerializationLibrary={}".
                     format(ddl_serde_lib, table_metadata_serialization_lib.lower()))
        return False
    else:
        logger.info("DDL's ROW FORMAT SERDE={} is different from the metadata SerializationLibrary={}".
                    format(ddl_serde_lib, table_metadata_serialization_lib.lower()))
        return True


def have_serdeproperties_changed(ddl_serde_properties, table_metadata):
    """
    Determine if the Athena table's ROW FORMAT SERDE WITH SERDEPROPERTIES properties have changed.

    Parameters
    ----------
        ddl_serde_properties: str
            ROW FORMAT SERDE sub-clause in the CREATE EXTERNAL TABLE statement in the DDL.
        table_metadata: Dictionary
            Athena table's metadata in the form of JSON that is obtained by calling get_tables Glue boto3 API.
    Returns
    -------
        Boolean True when changes are detected or False when no changes are detected.
    Exceptions
    ----------
        None
    """
    metadata_serde_properties = table_metadata['StorageDescriptor']['SerdeInfo']['Parameters']
    logger.debug("metadata_serde_properties={}".format(metadata_serde_properties))

    if len(metadata_serde_properties) == 0:
        logger.info("DDL includes SERDEPROPERTIES={} but metadata does not have any "
                    "['StorageDescriptor']['SerdeInfo']['Parameters'] JSON Parameters={}".
                    format(ddl_serde_properties, metadata_serde_properties))
        return True

    # Convert key/value pairs in metadata_serde_properties to lowercase in preparation for DeepDiff below.
    metadata_serde_properties = dict((k.lower(), v.lower()) for k, v in metadata_serde_properties.items())

    # Convert ddl_serde_properties from string to dictionary in preparation for the DeepDiff below.
    # Because metadata automatically includes "serialization.format": "1" when it's not specified in the DDL,
    # we need to add it to the DDL properties to avoid false differences.
    if 'serialization.format' in metadata_serde_properties:
        if 'serialization.format' in ddl_serde_properties:
            ddl_serde_properties = '{' + ddl_serde_properties.replace('=', ': ').replace("'", '"').strip() + '}'
        else:
            ddl_serde_properties = '{' + ddl_serde_properties.replace('=', ': ').replace("'", '"').strip() + \
                                   ', "serialization.format": "1"' + '}'
    else:
        ddl_serde_properties = '{' + ddl_serde_properties.replace('=', ': ').replace("'", '"').strip() + '}'
    logger.debug("ddl_serde_properties={}".format(ddl_serde_properties))
    ddl_serde_properties = json.loads(ddl_serde_properties)

    ddiff = DeepDiff(ddl_serde_properties, metadata_serde_properties, ignore_order=True)
    logger.debug("ddiff={}".format(ddiff))

    if len(ddiff) > 0:
        logger.info("DDL's SERDEPROPERTIES={} are different from the metadata "
                    "JSON ['StorageDescriptor']['SerdeInfo']['Parameters']={}".format(ddl_serde_properties,
                                                                                      metadata_serde_properties))
        return True
    else:
        logger.debug("DDL's SERDEPROPERTIES={} are the same as the metadata "
                     "JSON ['StorageDescriptor']['SerdeInfo']['Parameters']={}".format(ddl_serde_properties,
                                                                                       metadata_serde_properties))
        return False


def check_row_format_delimited_properties(ddl_row_format_delimited, table_metadata):
    """
    Determine if ROW FORMAT DELIMITED properties (e.g., FIELDS TERMINATED BY, LINES TERMINATED BY, etc.)
    have changed.
    Note: When DDL does not include a ROW FORMAT clause and the existing table is specified with
          the default ROW FORMAT DELIMITED, no change will be detected.

    Parameters
    ----------
        ddl_row_format_delimited: str
            ROW FORMAT DELIMITED sub-clause in the CREATE EXTERNAL TABLE statement in the DDL.
        table_metadata: Dictionary
            Athena table's metadata in the form of JSON that is obtained by calling get_tables Glue boto3 API.
    Returns
    -------
        Boolean True when changes are detected or False when no changes are detected.
    Exceptions
    ----------
        None
        """
    logger.debug("ddl_row_format_delimited={}".format(ddl_row_format_delimited))

    if ddl_row_format_delimited is None:
        return have_row_format_delimiters_changed(table_metadata)
    else:
        ddl_fields_terminator = next((segment for segment in ddl_row_format_delimited
                                      if ddl_row_format_subclauses_list[0].replace('\'', '').strip() in segment),
                                     None)
        logger.debug("ddl_fields_terminator={}".format(ddl_fields_terminator))
        ddl_escaped_by = next((segment for segment in ddl_row_format_delimited
                               if ddl_row_format_subclauses_list[1].replace('\'', '').strip() in segment),
                              None)
        logger.debug("ddl_escaped_by={}".format(ddl_escaped_by))
        ddl_collection_terminator = next((segment for segment in ddl_row_format_delimited
                                          if ddl_row_format_subclauses_list[2].replace('\'', '').strip() in segment),
                                         None)
        logger.debug("ddl_collection_terminator={}".format(ddl_collection_terminator))
        ddl_map_keys_terminator = next((segment for segment in ddl_row_format_delimited
                                        if ddl_row_format_subclauses_list[3].replace('\'', '').strip() in segment),
                                       None)
        logger.debug("ddl_map_keys_terminator={}".format(ddl_map_keys_terminator))
        ddl_lines_terminator = next((segment for segment in ddl_row_format_delimited
                                     if ddl_row_format_subclauses_list[4].replace('\'', '').strip() in segment),
                                    None)
        logger.debug("ddl_lines_terminator={}".format(ddl_lines_terminator))
        ddl_null_defined = next((segment for segment in ddl_row_format_delimited
                                 if ddl_row_format_subclauses_list[5].replace('\'', '').strip() in segment),
                                None)
        logger.debug("ddl_null_defined={}".format(ddl_null_defined))

        row_format_has_changed = False
        if ddl_fields_terminator is None and \
                ddl_escaped_by is None and \
                ddl_collection_terminator is None and \
                ddl_map_keys_terminator is None and \
                ddl_lines_terminator is None and \
                ddl_null_defined is None:
            if have_row_format_delimiters_changed(table_metadata, None, 'no properties') is True:
                row_format_has_changed = True

        if ddl_fields_terminator is not None:
            if ddl_escaped_by is not None:
                if have_row_format_delimiters_changed(table_metadata, ddl_fields_terminator + ' ' + ddl_escaped_by,
                                                      'fields escaped') is True:
                    row_format_has_changed = True
            else:
                if have_row_format_delimiters_changed(table_metadata, ddl_fields_terminator, 'fields') is True:
                    row_format_has_changed = True

        if ddl_collection_terminator is not None:
            if have_row_format_delimiters_changed(table_metadata, ddl_collection_terminator, 'collection') is True:
                row_format_has_changed = True

        if ddl_map_keys_terminator is not None:
            if have_row_format_delimiters_changed(table_metadata, ddl_map_keys_terminator, 'map keys') is True:
                row_format_has_changed = True

        if ddl_lines_terminator is not None:
            if have_row_format_delimiters_changed(table_metadata, ddl_lines_terminator, 'lines') is True:
                row_format_has_changed = True

        if ddl_null_defined is not None:
            if have_row_format_delimiters_changed(table_metadata, ddl_null_defined, 'null defined') is True:
                row_format_has_changed = True

        return row_format_has_changed


def have_row_format_delimiters_changed(table_metadata, ddl_row_format_delimiter=None,
                                       ddl_row_format_delimiter_type=None):
    """
    Determine if the Athena table's ROW FORMAT DELIMITED properties have changed.
    Note: ROW FORMAT SERDE (without any properties) is the same as ROW FORMAT DELIMITER (without any properties).

    Parameters
    ----------
        table_metadata: Dictionary
            Athena table's metadata in the form of JSON that is obtained by calling get_tables Glue boto3 API.
        ddl_row_format_delimiter: str
            ROW FORMAT DELIMITED sub-clause in the CREATE EXTERNAL TABLE statement in the DDL.
        ddl_row_format_delimiter_type: str
            The type of ROW FORMAT DELIMITED sub-clause (e.g., FIELDS TERMINATED BY, COLLECTION ITEMS TERMINATED BY
            MAP KEYS TERMINATED BY, and NULL DEFINED AS).
    Returns
    -------
        Boolean True when changes are detected or False when no changes are detected.
    Exceptions
    ----------
        None
    """
    table_metadata_serialization_lib = table_metadata['StorageDescriptor']['SerdeInfo']['SerializationLibrary']
    logger.debug("table_metadata_serialization_lib={}".format(table_metadata_serialization_lib))

    logger.debug("ddl_row_format_delimiter={}".format(ddl_row_format_delimiter))
    logger.debug("ddl_row_format_delimiter_type={}".format(ddl_row_format_delimiter_type))

    meta_json_escape_delim_char, meta_json_field_delim_char, meta_json_collection_delim_char, \
    meta_serialization_format, meta_json_map_keys_delim_char, meta_json_line_delim_char, \
    meta_json_ser_null_format_char = get_meta_row_format_delimiters(table_metadata)

    if ddl_row_format_delimiter_type is None:
        if meta_json_escape_delim_char is not None or meta_json_field_delim_char is not None or \
                meta_json_collection_delim_char is not None or meta_json_map_keys_delim_char is not None or \
                meta_json_line_delim_char is not None or meta_json_ser_null_format_char is not None:
            logger.info("DDL does not include a ROW FORMAT DELIMITED clause, but because metadata"
                        " ['StorageDescriptor']['SerdeInfo']['Parameters'] includes one or more parameters"
                        " a change is detected.")
            return True
        else:
            # LazySimpleSerDe is the default SerDe used for data in CSV, TSV, and custom-delimited formats.
            # Refer to: https://docs.aws.amazon.com/athena/latest/ug/lazy-simple-serde.html. When a table
            # is created without specifying ROW FORMAT or by specifying ROW FORMAT DELIMITED FIELDS
            # TERMINATED BY '|', or ROW FORMAT DELIMITED without any other row_format properties (e.g.,
            # LINES TERMINATED BY char, NULL DEFINED AS char, etc.), we get the following metadata info:
            # "SerdeInfo": {
            #       "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            #       "Parameters": {
            #         "serialization.format": "1"
            #       }
            #     },
            if table_metadata_serialization_lib.startswith('org.apache.'):
                logger.debug("DDL does not include a ROW FORMAT DELIMITED clause but because the metadata"
                             " SerializationLibrary={} is the default, no change is detected.".
                             format(table_metadata_serialization_lib))
                return False
            else:
                logger.info("DDL does not include a ROW FORMAT SERDE clause but because metadata has a non-default"
                            " SerializationLibrary={}, a change is detected".
                            format(table_metadata_serialization_lib))
                return True

    elif ddl_row_format_delimiter_type == 'no properties':
        if meta_serialization_format == '"1"':
            logger.debug("DDL includes a ROW FORMAT DELIMITED clause without any properties, but because the metadata"
                         " serialization.format={} is the default, no change is detected.".
                         format(meta_serialization_format))
            return False
        else:
            logger.info("DDL includes a ROW FORMAT DELIMITED clause without any properties, but because the metadata"
                        " serialization.format={} is not the default (i.e., 1), a change is detected.".
                        format(meta_serialization_format))
            return True
    elif ddl_row_format_delimiter_type == 'fields escaped':
        ddl_rfd_split_1, ddl_rfd_split_2 = ddl_row_format_delimiter.split(" escaped by '")
        ddl_fields_terminated_by_char = ddl_rfd_split_1.replace("fields terminated by '", "").rstrip("'")
        ddl_escaped_by_char = ddl_rfd_split_2.rstrip("'")
        ddl_json_escaped_by_char = '"' + ddl_escaped_by_char + '"'
        ddl_json_fields_terminated_by_char = '"' + ddl_fields_terminated_by_char + '"'
        if meta_json_field_delim_char == ddl_json_fields_terminated_by_char:
            if meta_json_escape_delim_char == ddl_json_escaped_by_char:
                logger.debug("DDL's FIELDS TERMINATED BY={} and DDL's FIELDS ESCAPED BY={} are the same "
                             "as the metadata field.delim={} and metadata escape.delim={}".
                             format(ddl_json_fields_terminated_by_char, ddl_json_escaped_by_char,
                                    meta_json_field_delim_char, meta_json_escape_delim_char))
                return False
            else:
                logger.info("DDL's FIELDS TERMINATED BY={} is the same as the metadata field.delim={}, but"
                            "DDL's FIELDS ESCAPED BY={} is different from the metadata escape.delim={}".
                            format(ddl_json_fields_terminated_by_char, meta_json_field_delim_char,
                                   ddl_json_escaped_by_char, meta_json_escape_delim_char))
                return True
        else:
            logger.info("DDL's FIELDS TERMINATED BY={} is different from the metadata field.delim={}".
                        format(ddl_json_fields_terminated_by_char, meta_json_field_delim_char))
            return True
    elif ddl_row_format_delimiter_type == 'fields':
        ddl_fields_terminated_by_char = ddl_row_format_delimiter.replace("fields terminated by '", "").rstrip("'")
        ddl_json_fields_terminated_by_char = '"' + ddl_fields_terminated_by_char + '"'
        if meta_serialization_format == '"1"' and ddl_json_fields_terminated_by_char == '"|"':
            logger.debug("DDL's FIELDS TERMINATED BY={} is equivalent of the metadata default field.delim={} "
                         "- no change is detected".format(ddl_json_fields_terminated_by_char,
                                                          meta_json_field_delim_char))
            return False
        if meta_json_field_delim_char == ddl_json_fields_terminated_by_char:
            logger.debug("DDL's FIELDS TERMINATED BY={} is the same as the metadata field.delim={}".
                         format(ddl_json_fields_terminated_by_char, meta_json_field_delim_char))
            return False
        else:
            logger.info("DDL's FIELDS TERMINATED BY={} is different from the metadata field.delim={}".
                        format(ddl_json_fields_terminated_by_char, meta_json_field_delim_char))
            return True
    elif ddl_row_format_delimiter_type == 'collection':
        ddl_collection_terminated_by_char = ddl_row_format_delimiter.replace(
            "collection items terminated by '", "").rstrip("'")
        ddl_json_collection_terminated_by_char = '"' + ddl_collection_terminated_by_char + '"'
        # The get_tables Glue boto3 API call currently returns the 'collection.delim' value misspelled as
        # colelction.delim; hence the misspellings below.
        if meta_json_collection_delim_char == ddl_json_collection_terminated_by_char:
            logger.debug("DDL's COLLECTION ITEMS TERMINATED BY={} is the same as the "
                         "metadata colelction.delim={}".
                         format(ddl_json_collection_terminated_by_char,
                                meta_json_collection_delim_char))
            return False
        else:
            logger.info("DDL's COLLECTION ITEMS TERMINATED BY={} is different from the "
                        "metadata colelction.delim={}".
                        format(ddl_json_collection_terminated_by_char,
                               meta_json_collection_delim_char))
            return True
    elif ddl_row_format_delimiter_type == 'map keys':
        ddl_map_keys_terminated_by_char = ddl_row_format_delimiter.replace("map keys terminated by '", ""). \
            rstrip("'")
        ddl_json_map_keys_terminated_by_char = '"' + ddl_map_keys_terminated_by_char + '"'
        if meta_json_map_keys_delim_char == ddl_json_map_keys_terminated_by_char:
            logger.debug("DDL's MAP KEYS TERMINATED BY={} is the same as the "
                         "metadata mapkey.delim={}".
                         format(ddl_json_map_keys_terminated_by_char, meta_json_map_keys_delim_char))
            return False
        else:
            logger.info("DDL's MAP KEYS TERMINATED BY={} is different from the "
                        "metadata mapkey.delim={}".
                        format(ddl_json_map_keys_terminated_by_char, meta_json_map_keys_delim_char))
            return True
    elif ddl_row_format_delimiter_type == 'lines':
        ddl_lines_terminated_by_char = ddl_row_format_delimiter.replace("lines terminated by '", "").rstrip("'")
        ddl_json_lines_terminated_by_char = '"' + ddl_lines_terminated_by_char + '"'
        if meta_json_line_delim_char == ddl_json_lines_terminated_by_char:
            logger.debug("DDL's LINES TERMINATED BY={} is the same as the "
                         "metadata line.delim={}".
                         format(ddl_json_lines_terminated_by_char, meta_json_line_delim_char))
            return False
        else:
            logger.info("DDL's LINES TERMINATED BY={} is different from the "
                        "metadata line.delim={}".
                        format(ddl_json_lines_terminated_by_char, meta_json_line_delim_char))
            return True
    elif ddl_row_format_delimiter_type == 'null defined':
        ddl_null_char = ddl_row_format_delimiter.replace("null defined as '", "").rstrip("'")
        ddl_json_null_char = '"' + ddl_null_char + '"'
        if meta_json_ser_null_format_char == ddl_json_null_char:
            logger.debug("DDL's NULL DEFINED AS={} is the same as the "
                         "metadata serialization.null.format={}".
                         format(ddl_json_null_char, meta_json_ser_null_format_char))
            return False
        else:
            logger.info("DDL's NULL DEFINED AS={} is different from the "
                        "metadata serialization.null.format={}".
                        format(ddl_json_null_char, meta_json_ser_null_format_char))
            return True
    else:
        raise Exception("Function received an invalid ddl_row_format_delimiter_type parameter!")


def get_meta_row_format_delimiters(table_metadata):
    """
    Obtain the  metadata ROW FORMAT DELIMITED properties under the ['StorageDescriptor']['SerdeInfo']['Parameters']
    JSON segment.

    Parameters
    ----------
        table_metadata: Dictionary
            Athena table's metadata in the form of JSON that is obtained by calling get_tables Glue boto3 API.
    Returns
    -------
        A list of ROW FORMAT DELIMITED properties.
    Exceptions
    ----------
        None
    """
    metadata_serdeinfo_parms = table_metadata['StorageDescriptor']['SerdeInfo']['Parameters']

    # To avoid a KeyError exception when a metadata property does not exist, we use a try block.
    try:
        meta_json_escape_delim_char = json.dumps(metadata_serdeinfo_parms['escape.delim'])
        logger.debug("meta_json_escape_delim_char={}".format(meta_json_escape_delim_char))
    except KeyError:
        meta_json_escape_delim_char = None
        logger.debug("meta_json_escape_delim_char={}".format(meta_json_escape_delim_char))

    try:
        meta_json_field_delim_char = json.dumps(metadata_serdeinfo_parms['field.delim'])
        logger.debug("meta_json_field_delim_char={}".format(meta_json_field_delim_char))
    except KeyError:
        meta_json_field_delim_char = None
        logger.debug("meta_json_field_delim_char={}".format(meta_json_field_delim_char))

    try:
        meta_json_collection_delim_char = json.dumps(metadata_serdeinfo_parms['colelction.delim'])
        logger.debug("meta_json_collection_delim_char={}".format(meta_json_collection_delim_char))
    except KeyError:
        meta_json_collection_delim_char = None
        logger.debug("meta_json_collection_delim_char={}".format(meta_json_collection_delim_char))

    try:
        meta_serialization_format = json.dumps(metadata_serdeinfo_parms['serialization.format'])
        logger.debug("meta_serialization_format={}".format(meta_serialization_format))
    except KeyError:
        meta_serialization_format = None
        logger.debug("meta_serialization_format={}".format(meta_serialization_format))

    try:
        meta_json_map_keys_delim_char = json.dumps(metadata_serdeinfo_parms['mapkey.delim'])
        logger.debug("meta_json_map_keys_delim_char={}".format(meta_json_map_keys_delim_char))
    except KeyError:
        meta_json_map_keys_delim_char = None
        logger.debug("meta_json_map_keys_delim_char={}".format(meta_json_map_keys_delim_char))

    try:
        meta_json_line_delim_char = json.dumps(metadata_serdeinfo_parms['line.delim'])
        logger.debug("meta_json_line_delim_char={}".format(meta_json_line_delim_char))
    except KeyError:
        meta_json_line_delim_char = None
        logger.debug("meta_json_line_delim_char={}".format(meta_json_line_delim_char))

    try:
        meta_json_ser_null_format_char = json.dumps(metadata_serdeinfo_parms['serialization.null.format'])
        logger.debug("meta_json_ser_null_format_char={}".format(meta_json_ser_null_format_char))
    except KeyError:
        meta_json_ser_null_format_char = None
        logger.debug("meta_json_ser_null_format_char={}".format(meta_json_ser_null_format_char))

    return meta_json_escape_delim_char, meta_json_field_delim_char, meta_json_collection_delim_char, \
           meta_serialization_format, meta_json_map_keys_delim_char, meta_json_line_delim_char, \
           meta_json_ser_null_format_char


def has_file_format_changed(table_metadata, ddl_segments):
    """
    Determine if the Athena table's file format has changed by comparing the STORED AS
    segment in the DDL against the corresponding metadata JSON segment:
    "StorageDescriptor": {"StorageDescriptor": {"InputFormat": "...", "InputFormat": "..."}}

    Parameters
    ----------
        table_metadata: Dictionary
            Athena table's metadata in the form of JSON that is obtained by calling get_tables Glue boto3 API.
        ddl_segments: List
            List of standard segments that are found in the DDL.
    Returns
    -------
        Boolean True when changes are detected or False when no changes are detected.
    Exceptions
    ----------
        None
    """
    ddl_stored_as_segment = next((segment for segment in ddl_segments if segment.startswith('stored as')), None)
    logger.debug("ddl_stored_as_segment={}".format(ddl_stored_as_segment))

    table_metadata_inputformat = table_metadata['StorageDescriptor']['InputFormat'].lower()
    table_metadata_outputformat = table_metadata['StorageDescriptor']['OutputFormat'].lower()
    logger.debug("table_metadata InputFormat={}".format(table_metadata_inputformat))
    logger.debug("table_metadata OutputFormat={}".format(table_metadata_outputformat))

    if ddl_stored_as_segment is not None:
        ddl_file_format = ddl_stored_as_segment.replace('stored as ', '')
        # STORED AS TEXTFILE in DDL equates to "InputFormat": "org.apache.hadoop.mapred.TextInputFormat" and
        # "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat" in the metadata.
        if ddl_file_format == 'textfile':
            if table_metadata_inputformat == TEXTFILE_STORAGE_FORMAT.lower():
                logger.debug("Metadata InputFormat={} is the same as DDL's STORED AS={}".format(
                    table_metadata_inputformat, ddl_file_format))
                return False
            else:
                logger.info("Metadata InputFormat={} is different from DDL's STORED AS={}".format(
                    table_metadata_inputformat, ddl_file_format))
                return True
        elif ddl_file_format == 'parquet':
            if table_metadata_inputformat == PARQUET_STORAGE_FORMAT.lower():
                logger.debug("Metadata InputFormat={} is the same as DDL's STORED AS={}".format(
                    table_metadata_inputformat, ddl_file_format))
                return False
            else:
                logger.info("Metadata InputFormat={} is different from DDL's STORED AS={}".format(
                    table_metadata_inputformat, ddl_file_format))
                return True
        elif ddl_file_format == 'sequencefile':
            if table_metadata_inputformat == SEQUENCEFILE_STORAGE_FORMAT.lower():
                logger.debug("Metadata InputFormat={} is the same as DDL's STORED AS={}".format(
                    table_metadata_inputformat, ddl_file_format))
                return False
            else:
                logger.info("Metadata InputFormat={} is different from DDL's STORED AS={}".format(
                    table_metadata_inputformat, ddl_file_format))
                return True
        elif ddl_file_format == 'rcfile':
            if table_metadata_inputformat == RCFILE_STORAGE_FORMAT.lower():
                logger.debug("Metadata InputFormat={} is the same as DDL's STORED AS={}".format(
                    table_metadata_inputformat, ddl_file_format))
                return False
            else:
                logger.info("Metadata InputFormat={} is different from DDL's STORED AS={}".format(
                    table_metadata_inputformat, ddl_file_format))
                return True
        elif ddl_file_format == 'orc':
            if table_metadata_inputformat == ORC_STORAGE_FORMAT.lower():
                logger.debug("Metadata InputFormat={} is the same as DDL's STORED AS={}".format(
                    table_metadata_inputformat, ddl_file_format))
                return False
            else:
                logger.info("Metadata InputFormat={} is different from DDL's STORED AS={}".format(
                    table_metadata_inputformat, ddl_file_format))
                return True
        elif ddl_file_format == 'avro':
            if table_metadata_inputformat == AVRO_STORAGE_FORMAT.lower():
                logger.debug("Metadata InputFormat={} is the same as DDL's STORED AS={}".format(
                    table_metadata_inputformat, ddl_file_format))
                return False
            else:
                logger.info("Metadata InputFormat={} is different from DDL's STORED AS={}".format(
                    table_metadata_inputformat, ddl_file_format))
                return True
        elif ddl_file_format.startswith('inputformat'):
            if ddl_file_format.find('outputformat') >= 0:
                ddl_inputformat_key, ddl_inputformat_value, ddl_outputformat_key, ddl_outputformat_value = \
                    ddl_file_format.split(' ')
                ddl_inputformat_value = ddl_inputformat_value.strip("'")
                ddl_outputformat_value = ddl_outputformat_value.strip("'")
            else:
                ddl_inputformat_key, ddl_inputformat_value = ddl_file_format.split(' ')
                ddl_inputformat_value = ddl_inputformat_value.strip("'")

            if ddl_inputformat_value == table_metadata_inputformat:
                if ddl_outputformat_value == table_metadata_outputformat:
                    logger.debug("Metadata InputFormat={} and OutputFormat={} are the same as "
                                 "DDL's STORED AS INPUTFORMAT={} and OUTPUTFORMAT={}".format(
                        table_metadata_inputformat, table_metadata_outputformat,
                        ddl_inputformat_value, ddl_outputformat_value))
                    return False
                else:
                    logger.info("Metadata InputFormat={} is the same as DDL's STORED AS INPUTFORMAT={} but "
                                "Metadata OutputFormat={} is different from DDL's STORED AS OUTPUTFORMAT={}".format(
                        table_metadata_inputformat, ddl_inputformat_value,
                        table_metadata_outputformat, ddl_outputformat_value))
                    return True
            else:
                logger.info("Metadata InputFormat={} is different from DDL's STORED AS INPUTFORMAT={}".format(
                    table_metadata_inputformat, ddl_inputformat_value))
                return True
        else:
            raise Exception(
                "DDL's STORED AS sub-clause does not match any of the expected values (e.g., parquet, textfile, etc.")
    else:
        if table_metadata_inputformat == TEXTFILE_STORAGE_FORMAT.lower():
            logger.debug("DDL does not have a STORED AS clause but because the metadata InputFormat={} reflects "
                         "the default TextInputFormat no file format change is detected!".format(
                table_metadata_inputformat))
            return False
        else:
            logger.info("DDL does not have a STORED AS clause and because the metadata InputFormat={} "
                        "does not reflect the default InputFormat a file format change is detected!".format(
                table_metadata_inputformat))
            return True


def have_tblproperties_changed(table_metadata, ddl_segments):
    """
    Determine if the Athena table's compression or other properties have changed by comparing the
    TBLPROPERTIES segment in the DDL against the corresponding metadata JSON segment:
    "Parameters": {...}

    Note: In some cases, the metadata includes additional properties, including table schema info, beyond
    the permissible values within the DDL TBLPROPERTIES segment. The additional table properties in metadata
    cause false differences between the metadata and DDL -- see an example below. As a result, the logic
    in this function is limited to comparing the following subset of known TBLPROPERTIES:
    classification, has_encrypted_data, orc.compress, parquet.compress, and skip.header.line.count.

    Sample metadata JSON ['Parameters'] with additional properties not present in DDL TBLPROPERTIES:
    {'column_stats_accurate': 'false', 'numfiles': '10', 'numrows': '-1',
    'parquet.compress': 'snappy', 'rawdatasize': '-1', 'spark.sql.create.version': '2.2 or prior',
    'spark.sql.sources.schema.numparts': '3', 'spark.sql.sources.schema.part.0': '{"type":"struct",
    "fields":[{"name":"month_display","type":"string","nullable":true,"metadata":{}}, ...

    Parameters
    ----------
        table_metadata: Dictionary
            Athena table's metadata in the form of JSON that is obtained by calling get_tables Glue boto3 API.
        ddl_segments: List
            List of standard segments that are found in the DDL.
    Returns
    -------
        Boolean True when changes are detected or False when no changes are detected.
    Exceptions
    ----------
        Raised if metadata includes any TBLPROPERTIES other than EXTERNAL, parquet.compress and transient_lastDdlTime.
    """
    ddl_tblproperties_segment = next((segment for segment in ddl_segments if segment.startswith('tblproperties')), None)
    logger.debug("ddl_tblproperties_segment={}".format(ddl_tblproperties_segment))

    default_metadata_tblproperties = table_metadata['Parameters']
    # In preparation for DeepDiff below, exclude metadata properties that are included by default and could
    # cause a false difference. Also, note that table comment, which shows up in the same metadata ['Parameters']
    # segment as the rest of TBLPROPERTIES is also excluded. Because there's a prior check for table comments,
    # any comment differences would have been caught in that logic already and would not make it to this point.
    metadata_tblproperties = dict((key.lower(), value.lower()) for key, value in default_metadata_tblproperties.items()
                                  if key.lower() == 'classification' or key.lower() == 'has_encrypted_data'
                                  or key.lower() == 'orc.compress' or key.lower() == 'parquet.compress'
                                  or key.lower() == 'write.compression' or key.lower().startswith('projection.')
                                  or key.lower() == 'skip.header.line.count'
                                  or key.lower() == 'storage.location.template')
    logger.debug("metadata_tblproperties={}".format(metadata_tblproperties))

    if ddl_tblproperties_segment is not None:
        ddl_tblproperties = ddl_tblproperties_segment.split(' (')[1].strip(')').strip()
        # Convert ddl_tblproperties from string to dictionary in preparation for the DeepDiff below.
        ddl_tblproperties = '{' + ddl_tblproperties.replace('=', ': ').replace("'", '"').strip() + '}'
        logger.debug("ddl_tblproperties={}".format(ddl_tblproperties))
        ddl_tblproperties = json.loads(ddl_tblproperties)
        ddl_tblproperties = {key: value for key, value in ddl_tblproperties.items()
                             if key == 'classification' or key.lower() == 'has_encrypted_data'
                             or key == 'orc.compress' or key.lower() == 'parquet.compress'
                             or key == 'write.compression' or key.startswith('projection.')
                             or key == 'skip.header.line.count'
                             or key.lower() == 'storage.location.template'}
        if len(ddl_tblproperties) == 0:
            if len(metadata_tblproperties) == 0:
                logger.debug("DDL and metdata don't include any TBLPROPERTIES")
                return False
            else:
                logger.info("DDL does not include any non-default TBLPROPERTIES but because metadata JSON "
                            "['Parameters']={}, a change is detected".format(metadata_tblproperties))
                return True
        else:
            if len(metadata_tblproperties) == 0:
                logger.info("DDL includes non-default TBLPROPERTIES={} but because metdata doesn't "
                            "include any non-default JSON ['Parameters'], a change is detected".
                            format(ddl_tblproperties))
                return True
            else:
                ddiff = DeepDiff(ddl_tblproperties, metadata_tblproperties, ignore_order=True)
                logger.debug("ddiff={}".format(ddiff))

                if len(ddiff) > 0:
                    logger.info("DDL's TBLPROPERTIES={} are different from the metadata "
                                "JSON ['Parameters']={}".format(ddl_tblproperties, metadata_tblproperties))
                    return True
                else:
                    logger.debug("DDL's TBLPROPERTIES={} are the same as the metadata "
                                 "JSON ['Parameters']={}".format(ddl_tblproperties, metadata_tblproperties))
                    return False
    else:
        if len(metadata_tblproperties) == 0:
            logger.debug("DDL and metdata don't include any TBLPROPERTIES")
            return False
        else:
            logger.info("DDL does not include any non-default TBLPROPERTIES but because metadata JSON "
                        "['Parameters']={}, a change is detected".format(metadata_tblproperties))
            return True


def has_table_location_changed(table_metadata, ddl_segments):
    """
    Determine if the Athena table's location has changed by comparing the LOCATION
    segment in the DDL against the corresponding metadata JSON segment:
    "StorageDescriptor": {"Location": "..."}

    Parameters
    ----------
        table_metadata: Dictionary
            Athena table's metadata in the form of JSON that is obtained by calling get_tables Glue boto3 API.
        ddl_segments: List
            List of standard segments that are found in the DDL.
    Returns
    -------
        Boolean True when changes are detected or False when no changes are detected.
    Exceptions
    ----------
        Raised if the mandatory LOCATION clause is missing in the DDL.
    """
    ddl_location_segment = next((segment for segment in ddl_segments if segment.startswith("location")), None)
    logger.debug("ddl_location_segment={}".format(ddl_location_segment))

    metadata_location = table_metadata['StorageDescriptor']['Location'].lower()
    logger.debug("metadata_location={}".format(metadata_location))

    if ddl_location_segment is not None:
        ddl_segment_key, ddl_segment_value = ddl_location_segment.split(' ', 1)
        ddl_segment_value = ddl_segment_value.strip("'")
        if metadata_location == ddl_segment_value:
            logger.debug("Metadata location clause={} is the same as the DDL location clause={}".format(
                metadata_location, ddl_segment_value))
            return False
        else:
            logger.info("Metadata location clause={} is different from the DDL location clause={}".format(
                metadata_location, ddl_segment_value))
            return True
    else:
        raise Exception('The mandatory LOCATION clause is missing in the DDL - please correct the DDL first!')


def drop_table(db_name, table_name, table_type, temp_folder, stack_info_obj, product_name, environment_name):
    """
    Drop a table or view, if it needs to be recreated.

    Parameters
    ----------
        db_name: str
            Name of the database in which the table/view should be dropped.
        table_name: str
            Name of the table/view to be dropped.
        table_type: str
            Type of database object to be dropped; table or view.
        temp_folder: str
            The temporary folder where Athena query executions should be stored.
        stack_info_obj: object
            Reference to the stack_info object.
        product_name: str
            Name of the application for which to create the folders.
        environment_name: str
            Name of the environment for which to create the folders.
    Returns
    -------
        None
    Exceptions
    ----------
        None
    """
    logger.info('Dropping: {}.{}'.format(db_name, table_name))

    output_location = 's3://' + stack_info_obj.get_bucket_name_by_label(
        product_name, environment_name, 'output') + '/' + temp_folder

    if table_type == 'VIRTUAL_VIEW':
        query_string = 'DROP VIEW IF EXISTS {}.{}'.format(db_name, table_name)
    else:
        query_string = 'DROP TABLE IF EXISTS {}.{}'.format(db_name, table_name)

    execute_query(product_name, query_string, output_location)


def create_table(table_config, ddl_text, db_name, stack_info_obj, product_name, environment_name):
    """
    Create a table or view based on the information provided in the application configuration JSON file.

    Parameters
    ----------
        table_config: Dictionary
            The dictionary reflecting the table configuration from application JSON file.
        ddl_text: str
            Content of DDL script file.
        db_name: str
            Name of the database in which the table's existence should be checked.
        stack_info_obj: object
            Reference to the stack_info object.
        product_name: str
            Name of the application for which to create the folders.
        environment_name: str
            Name of the environment for which to create the folders.
    Returns
    -------
        None
    Exceptions
    ----------
        None
    """
    logger.info('Creating table or view: {}.{}'.format(db_name, table_config['table_name']))

    # Prepare Athena query output location
    output_location = 's3://' + stack_info_obj.get_bucket_name_by_label(
        product_name, environment_name, 'output') + '/' + table_config['temp_folder']

    execute_query(product_name, ddl_text, output_location)

    if ddl_text.lower().find("partitioned by") >= 0:
        logger.info('Loading partitions for table: {}.{}'.format(db_name, table_config['table_name']))
        query_string = "MSCK REPAIR TABLE {}.{}".format(db_name, table_config['table_name'])
        execute_query(product_name, query_string, output_location)


def execute_query(product_name, query_string, output_location):
    """
    Using Athena boto3 API call, execute an SQL statement.

    Parameters
    ----------
        query_string: str
            Text of the SQL statement.
        output_location: str
            S3 location where the query execution result should be placed. Workgroup settings trump the
            value of this parameter.
    Returns
    -------
        Query execution ID
    Exceptions
    ----------
        None
    """
    response = athena_client.start_query_execution(
        QueryString=query_string,
        ResultConfiguration={
            'OutputLocation': output_location
        },
        WorkGroup=product_name.lower() + '-etl'
    )
    # Obtain query execution id
    query_execution_id = response['QueryExecutionId']
    logger.debug("query_execution_id={}".format(query_execution_id))
    query_execution = dict(query_execution_waiter(query_execution_id))

    if query_execution['QueryExecution']['Status']['State'] == 'FAILED':
        raise Exception('Unable to execute query={}...! Reason -> {}'.
                        format(query_string[0:150], query_execution['QueryExecution']['Status']['StateChangeReason']))
    return query_execution_id


def query_execution_waiter(query_id):
    """
    Wait for query execution to change status.

    Parameters
    ----------
        query_id: str
            The ID of query being executed.
    Returns
    -------
        query_execution: Dictionary
            A dictionary containing the query execution results.
    Exceptions
    ----------
        None
    """
    while True:
        time.sleep(0.1)
        # Get query execution status
        query_execution = athena_client.get_query_execution(QueryExecutionId=query_id)
        if query_execution['QueryExecution']['Status']['State'] not in ('QUEUED', 'RUNNING'):
            break

    return query_execution


def main():
    parser = argparse.ArgumentParser(
        description='A program to provision application resources (s3 bucket folders, database, tables/views) for '
                    "a project/subject area. Before executing this program, be sure to 'export branchEnv=<env>' "
                    'where <env> is the target environment (e.g., dev, qa, uat or prod).'
    )
    parser.add_argument(
        'product_name',
        help='Product for which to deploy resources (e.g., rdms).',
        type=str)
    parser.add_argument(
        'app_config_file',
        help='Prefix of the application configuration JSON file stored in S3 (e.g., config/config.json).',
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
    product_name = args.product_name.lower()
    environment_name = os.getenv('branchEnv')
    if environment_name is None or len(environment_name) == 0:
        raise Exception(
            "Unable to determine environment based on os.getenv('branchEnv'). Be sure to 'export branchEnv=<env>' "
            'where <env> is one of dev, qa, uat or prod')
    app_config_file = args.app_config_file

    if args.region is None:
        region = 'us-west-2'
    else:
        region = args.region.lower()
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
            log_group='/' + product_name + '/' + environment_name + '/log',
            create_log_group=True))

    logger.info(
        'About to execute program={}'.format(PGM_NAME))
    logger.info(
        'Positional arguments set to: product={} and app_config_file={}'.format(product_name, app_config_file))
    logger.info(
        'Optional/default arguments set to: region={} and logger_level={}'.format(region, logger_level))
    logger.info('branchEnv={}'.format(environment_name))

    stack_info_obj = stack_info(logger_level=logger_level)

    app_bucket_name = stack_info_obj.get_bucket_name_by_label(
        product_name, environment_name, 'app')
    logger.debug('App bucket name: {}'.format(app_bucket_name))

    output_bucket_name = stack_info_obj.get_bucket_name_by_label(
        product_name, environment_name, 'output')
    logger.debug('Output bucket name: {}'.format(output_bucket_name))

    try:
        # Obtain database name from config JSON file
        obj = s3_resource.Object(app_bucket_name, app_config_file)
        f = obj.get()['Body']
        config_dict = json.load(f)
        if config_dict['database']['include_env_suffix'].lower() == 'true':
            db_name = config_dict['database']['name'] + '_' + environment_name
        else:
            db_name = config_dict['database']['name']
        logger.debug('db_name={}'.format(db_name))

        if 'location' in config_dict['database']:
            db_location = 'location "s3://' + \
                          stack_info_obj.get_bucket_name_by_label(
                              product_name, environment_name, config_dict['database']['location']['s3_label']) \
                          + config_dict['database']['location']['s3_path'] + '/"'
        else:
            db_location = None

        create_folders(config_dict, stack_info_obj, product_name, environment_name)
        create_database(product_name, db_name, db_location, output_bucket_name)
        process_athena_tables(config_dict, output_bucket_name, app_bucket_name,
                              db_name, stack_info_obj, product_name, environment_name)
        logger.info('Application resources were provisioned successfully!')
    except ClientError as ce:
        if ce.response['Error']['Code'] == 'NoSuchKey':
            logger.error('Unable to locate the required file in S3 bucket: {} -- {}'.
                         format(ce.response['Error']['Key'], ce.response['Error']['Message']))
        else:
            logger.exception(ce)
        raise ce
    except Exception as e:
        logger.exception(e)
        raise e


if __name__ == '__main__':
    main()
