# Data Lake Application Resource Provisioning Modules
This directory contains the Python modules that provision the application resources,
such as Athena databases and their location folders in AWS as well as the Lake Formation
permissions for each Athena database.
1. Module app.py: Creates Athena databases and tables in the data lake along with their supporting
S3 folders. In addition, the program detects structure changes to existing tables automatically and drops and 
recreates the tables. Moreover, it reloads partitions if the recreated table is partitioned.
2. Module manage_table_perms.py: Grants or revokes AWS Lake Formation permissions.
3. Module config_validator.py: Validates the various JSON configuration files against predefined JSON schemas.
For further information refer to "Validating the JSON configuration files" section in:
 [app/config/README.md](../config/README.md)

## app.py Python Module
The program expects the branchEnv environment variable to be set to one of the following values:
- dev, qa, uat or prod

You should export the above environment variable in the terminal prior to executing the program.
For example, export branchEnv=dev

### Deprecated Feature
To drop and recreate any existing Athena objects (tables or views), be sure to populate the database 
names and table names in the file that is passed as app_config_file command line argument (<span style="color:yellow">this feature is deprecated</span>).
The program has been enhanced to compare each existing table's metadata against the table's 
DDL script that is stored in S3 and automatically detect if an existing table's structure has changed. Upon 
detecting changes, the program drops and recreates the table. The program obtains the existing tables'
metadata using get_tables Glue boto3 API call, which is similar to the following CLI command:

- aws glue get-tables --database-name <database-name> 

You can further filter the results from the above command using jq. Below is a sample command that filters on 
user_query table in the perf_test database:

- aws glue get-tables --database-name <span style="color:red">perf_test</span> | jq -r '.TableList[] | select (.Name | endswith("<span style="color:red">user_query</span>"))'

### Usage Info
Following is the usage information for the app.py module, which can also be obtained by issuing
the following command in app/bin directory in a terminal: python app.py --help. Optional arguments
are denoted by square brackets [] in the usage message. Permissible values are denoted by curly brackets {}.
```
usage: app.py [-h] [-o OBJ_RECREATE_FILE] [-r {us-west-1,us-east-1,us-east-2}] [-l {debug,error,critical,info}] product_name app_config_file

A program to provision application resources (s3 bucket folders, database, tables/views) for a project/subject area. Before executing this program, be sure to 'export branchEnv=<env>' where <env> is the target environment (e.g., dev,
qa, uat or prod).

positional arguments:
  product_name          Product for which to deploy resources (e.g., rdms).
  app_config_file       Prefix of the application configuration JSON file stored in S3 (e.g., config/config.json).

optional arguments:
  -h, --help            show this help message and exit
  -o OBJ_RECREATE_FILE, --obj_recreate_file OBJ_RECREATE_FILE
                        Prefix and name of the Athena object recreation JSON file stored in S3. If not specified, the program uses config/obj_recreate.json file, by default.
  -r {us-west-1,us-east-1,us-east-2}, --region {us-west-1,us-east-1,us-east-2}
                        AWS region in which to execute this program.
  -l {debug,error,critical,info}, --logger_level {debug,error,critical,info}
                        Desired level of logging.
```
### Execution Logs
Log messages for each execution of the app.py program are captured in /rdms/{env}/log CloudWatch log group, where
{env} is one of dev, qa, uat or prod. Each execution has its own unique log file name: app.py_{date}-{time}, where 
{date}-{time} signify the date and time of the program execution. 

As shown in the Usage Info section, the level of logging can be changed by providing the --logger_level or -l 
command line option.
## manage_table_perms.py (deprecated)
The program expects the branchEnv environment variable to be set to one of the following values:
- dev, qa, uat or prod

You should export the above environment variable in the terminal prior to executing the program.
For example, export branchEnv=dev

### Usage Info
Following is the usage information for the manage_table_perms.py module, which can also be
obtained by issuing the following command in a terminal: python manage_table_perms.py --help.
Optional arguments are denoted by square brackets [] in the usage message. Permissible values
are denoted by curly brackets {}.
```
usage: manage_table_perms.py [-h] [-p PREFIX] [-s SUFFIX] [-r {us-west-1,us-east-1,us-east-2}] [-l {debug,error,critical,info}] {grant,revoke} product bucket_label app_config_file perm_config_file

A program to manage Lake Formation permissions on Athena tables. Before executing this program, be sure to 'export branchEnv=<env>' where <env> is the target environment (e.g., dev, qa, uat or prod).

positional arguments:
  {grant,revoke}        Mode in which you want to manage Lake Formation permissions (e.g., grant or revoke).
  product               Product for which to manage Lake Formation permissions (e.g., sdap).
  bucket_label          S3 Bucket label for the bucket in which the configuration JSON files are stored (e.g., app).
  app_config_file       Full path/name of the application configuration JSON file in S3 (e.g., config/config.json).
  perm_config_file      Full path/name of the permissions configuration JSON file in S3 (e.g., config/config_perm.json).

optional arguments:
  -h, --help            show this help message and exit
  -p PREFIX, --prefix PREFIX
                        Prefix for use in calls to lf_perms_helper.get_table_name() method to limit the list of table namesreturned to a set that begins with the provided prefix.
  -s SUFFIX, --suffix SUFFIX
                        Suffix for use in calls to lf_perms_helper.get_table_name() method to limit the list of table names returned to a set that ends with the provided suffix.
  -r {us-west-1,us-east-1,us-east-2}, --region {us-west-1,us-east-1,us-east-2}
                        AWS region in which to execute this program.
  -l {debug,error,critical,info}, --logger_level {debug,error,critical,info}
                        Desired level of logging.
```
### Execution Logs
Log messages for each execution of the manage_table_perms.py program are captured in /rdms/{env}/log CloudWatch log group, where
{env} is one of dev, qa, uat or prod. Each execution has its own unique log file name: manage_table_perms.py_{date}-{time}, where 
{date}-{time} signify the date and time of the program execution. 

As shown in the Usage Info section, the level of logging can be changed by providing the --logger_level or -l 
command line option.
### Important Note: 
app.py and manage_table_perms.py programs work in tandem in that the list of
tables in the app.py configuration file (i.e., *_config.json) must match the list of tables in the
manage_table_perms.py configuration file (i.e., *_config_perm.json). Otherwise, an error similar
to the following occurs while executing manage_table_perms.py:
```
Traceback (most recent call last):
  File "C:\Users\hpejman\PycharmProjects\rdms\app\bin\manage_table_perms.py", line 141, in <module>
    main()
  File "C:\Users\hpejman\PycharmProjects\rdms\app\bin\manage_table_perms.py", line 133, in main
    lfph_obj.handle_permissions(table_names_list)
  File "C:\Users\hpejman\PycharmProjects\rdms\venv\lib\site-packages\ucop_util\lf_perms_helper.py", line 719, in handle_permissions
    self.handle_role_perms(perm_dict)
  File "C:\Users\hpejman\PycharmProjects\rdms\venv\lib\site-packages\ucop_util\lf_perms_helper.py", line 459, in handle_role_perms
    bucket_arn_value = self.get_bucket_arn(perm_dict['bucket_label'])
KeyError: 'bucket_label'
```
To obtain a list of tables in each of the configuration files execute the following commands:

```cat <app-config-file-name>.json | jq -r '.athena_tables[].table_name'```

```cat <perm_config-file-name>.json | jq -r '.grants[].table[].name'```
