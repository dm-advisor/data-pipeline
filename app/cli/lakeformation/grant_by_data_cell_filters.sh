#!/bin/bash
##################################################################################################
# This script can be used to grant permissions using Lake Formation data cells filters for
# a project.
#
# While coding/editing the CSV input file be sure to use Linux line feed (LF) instead of
# Windows (CRLF).
#
# To verify that permissions where granted properly, copy and paste the command
# below into a text editor. Then, remove the hash marks (#), replace strings surrounded by
# < > brackets with real values, copy and paste the command into a terminal session and execute it:
# aws lakeformation list-permissions --principal '{"DataLakePrincipalIdentifier":"<role-arn>"}' \
# --resource '{"DataCellsFilter":{"DatabaseName":"<db-name>", "TableName":"<table-name>",
# "Name":"<filter-name>"}}'
##################################################################################################
source ./../env_conf.sh || { echo "Unable to source env_conf.sh"; exit 1; }
SCRIPT_NAME=$(basename "$0")

if [ "$#" -ne 2 ]; then
  printf "ERROR: Wrong number of arguments - required 2 arguments; received %s" "$#"
  printf "\nUsage: %s environment path/csv-input-file-name" "$SCRIPT_NAME"
  printf "\nExample: %s dev ../../config/lakeformation/grants/data_cell_access_4_data_access_roles.csv" "$SCRIPT_NAME"
  exit 1
fi

environment=${1}
info "environment is set to: ${environment}"
input=${2}
info "Input file name is set to: ${input}"

if [ ! -f "${input}" ]; then
    error "Unable to locate CSV input file: ${input}"
fi

json_template="../../config/lakeformation/grants/data_cell_access_template.json"

if [ ! -f "${json_template}" ]; then
    error "Unable to locate the required JSON template file: ${json_template}"
fi

# Read the CSV file and iterate through records one at a time.
while IFS='|' read -r role_name filter_name db_name table_name perms perms_with_grant
do
  # Substitute the "{env}" literal the in role_name with the value of $environment, passed through command line.
  role_name_by_env=${role_name/"{env}"/"${environment}"} \
  || error "Environment parameter substitution in role name failed!"

   # Obtain AWS role ARN using environment-specific role_name.
  role_arn=$(aws iam list-roles | \
      jq -r --arg ROLENAME "${role_name_by_env}" '.Roles[] | select (.RoleName == $ROLENAME)' | jq -r .Arn)
  [[ -z "${role_arn}" ]] && error "Unable to determine AWS role ARN for role name: ${role_name_by_env}.
      Make sure security token has not expired!"

  info "Now granting ${perms} to role ARN: ${role_arn} on table: ${table_name}, in database: ${db_name},"
  info "using filter: ${filter_name}"

  # Substitute the variables in the JSON template with actual values from CSV file.
  temp_v=$(sed -e "s!\${PRINCIPAL}!${role_arn}!" \
              -e "s/\${DB_NAME}/${db_name}/" \
              -e "s/\${TABLE_NAME}/${table_name}/" \
              -e "s/\${FILTER_NAME}/${filter_name}/" \
              -e "s/\${PERMS}/${perms}/" \
              -e "s/\${PERMS_WITH_GRANT}/${perms_with_grant}/" \
              "${json_template}") \
              || error "Parameter substitution in JSON template file failed!"

  aws lakeformation grant-permissions --cli-input-json \
    "${temp_v}" \
    || error "aws lakeformation grant-permissions command failed!"
done <<< "$(tail -n +2 < "${input}" | grep  -v '^#')" # Feed the csv file to the while loop and skip the header and commented lines.

info "Script completed successfully!"
