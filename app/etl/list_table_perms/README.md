# Function Info
This Lambda function produces a list of all policies that are associated with Athena tables
using Lake Formation based on the configuration file that was used initially to create
the Athena tables for the application.
The function requires a number of predefined environment variables and
writes its output to the S3 bucket that is specified by "out_bucket_label"
environment variable. The output file name is based on the "out_bucket_key" environment
variable. The output file contains the results produced by the Lambda function.

Refer to env.json file for a list of additional environment variables that are required by
the Lambda function.
Below is a snippet of a sample output file:
```
*** Data Lake permissions for Athena tables in erm_reporting_dev database ***
---------------- Policies for table: sedgwick_wc ----------------
Principal={'DataLakePrincipalIdentifier': 'arn:aws:iam::115854119938:role/rdmsDevMultirole016LocalAnalystWebRole'}
	Resource={'TableWithColumns': {'CatalogId': '115854119938', 'DatabaseName': 'erm_reporting_dev', 'Name': 'sedgwick_wc', 'ColumnWildcard': {}}}
	Permissions=['SELECT']
	PermissionsWithGrantOption=[]
Principal={'DataLakePrincipalIdentifier': 'arn:aws:iam::115854119938:role/rdms-dev-lambda-bucket-role'}
	Resource={'Table': {'CatalogId': '115854119938', 'DatabaseName': 'erm_reporting_dev', 'Name': 'sedgwick_wc'}}
	Permissions=['ALTER']
	PermissionsWithGrantOption=[]
Principal={'DataLakePrincipalIde...
---------------- Policies for table: sedgwick_wc_view ----------------
Principal={'DataLakePrincipalIdentifier': 'arn:aws:iam::115854119938:role/rdmsDevMultirole016LocalAnalystWebRole'}
	Resource={'TableWithColumns': {'CatalogId': '115854119938', 'DatabaseName': 'erm_reporting_dev', 'Name': 'sedgwick_wc_view', 'ColumnWildcard': {}}}
	Permissions=['SELECT']
	PermissionsWithGrantOption=[]
Principal={'DataLakePrincipalIdentifier': 'arn:aws:iam::115854119938:role/rdms-dev-lambda-bucket-role'}
	Resource={'Table': {'CatalogId': '115854119938', 'DatabaseName': 'erm_reporting_dev', 'Name': 'sedgwick_wc_view'}}
	Permissions=['ALTER']
	PermissionsWithGrantOption=[]...

```
## Instructions to run the function locally
As an alternative to using [SAM](https://aws.amazon.com/serverless/sam/), the generic run_lambda_local.sh script in the app/cli directory
may be used to test any Lambda function in your local IDE (e.g., PyCharm). This feature is useful
during the development process, because it allows you to test your function locally before you
deploy it to AWS.
The local run requires the following additional two files:
- a JSON file that includes
the environment variables required by the Lambda function
```buildoutcfg=
{
  "CODEPIPELINE_GIT_BRANCH_NAME": "dev",
  "product": "rdms",
  "logger_level_value": "DEBUG",
  "region_name": "us-west-2",
  "bucket_label": "app",
  "database_name": "erm_reporting_dev",
  "config_file_name": "config/erm_reporting_config.json",
  "out_bucket_label": "output",
  "out_bucket_key": "_temporary/etl/erm_reporting_perms.out"
}
```
- a JSON file that includes the required event for the Lambda function. This file may contain
an empty JSON structure, if the function does not require an event.
```buildoutcfg=
{}
```

* Prerequisite: To run the Lambda function locally make sure python-lambda-local package is installed.

```
pip install python-lambda-local

```
The script could be executed in a terminal in your IDE. Sample script execution:
```buildoutcfg=
cd ~/rdms/app/cli
run_lambda_local.sh \
    ~/rdms/app/etl/Lambda/ListTablePerms \
    ~/rdms/ucop-util-pkg/dist/ \
    lambda_handler \
    300 \
    env.json \
    lambda_list_table_perms.py \
    event.json
```

The above script requires the following 7 command line arguments as shown in the sample
execution above. For more details refer to the comments in the script.
-  arg1=directory where function source code (.py) is stored
-  arg2=directory where local Python packages are stored (e.g., ucop_util)
-  arg3=name of the Lambda handler
-  arg4=Lambda function time out period in seconds
-  arg5=name of JSON file that contains all required environment variables
-  arg6=name of the Python file (.py) that contains the source code for the Lambda function
-  arg7=name of JSON file that contains the event for the function

## AWS Serverless Architecture Model (SAM)
The template.yaml could be utilized by SAM CLI to deploy the Lambda function to AWS.
SAM offers the following benefits:
- Single Deployment Configuration
- Local Testing and Debugging
- Built-In Best Practices
- Integration with Development Tools
- Built on AWS CloudFormation

For more information refer to: https://aws.amazon.com/serverless/sam/
