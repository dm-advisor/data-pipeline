#!/bin/bash
##################################################################################################
# This script can be used to revoke permissions using Lake Formation tags for a project.
#
# While coding/editing the CSV input file be sure to use Linux line feed (LF) instead of
# Windows (CRLF).
#
# To verify that permissions where revoked properly, copy and paste the command
# below into a text editor. Then, remove the hash marks (#), replace strings surrounded by
# < > brackets with real values, copy and paste the command into a terminal session and execute it:
# aws lakeformation list-permissions --principal '{"DataLakePrincipalIdentifier":"<role-arn>"}' \
# --resource '{"LFTagPolicy":{"ResourceType":"Table",
# "Expression":[{"TagKey": "<lf-tag-key1>","TagValues": ["<lf-tag-value>", "<lf-tag-value>", ...]},
# {"TagKey":"<lf-tag-key2>","TagValues":[<lf-tag-value>", "<lf-tag-value>", ...]}]}}'
##################################################################################################
source ./../env_conf.sh || { echo "Unable to source env_conf.sh"; exit 1; }
SCRIPT_NAME=$(basename "$0")

if [ "$#" -ne 2 ]; then
  printf "ERROR: Wrong number of arguments - required 2 arguments; received %s" "$#"
  printf "\nUsage: %s environment path/csv-input-file-name" "$SCRIPT_NAME"
  printf "\nExample 1: %s dev ../../config/lakeformation/grants/lf_tag_access_4_data_access_roles.csv" "$SCRIPT_NAME"
  printf "\nExample 2: %s dev ../../config/lakeformation/grants/lf_tag_access_4_service_roles.csv" "$SCRIPT_NAME"
  exit 1
fi

environment=${1}
info "environment is set to: ${environment}"
input=${2}
info "Input file name is set to: ${input}"

if [ ! -f "${input}" ]; then
    error "Unable to locate CSV input file: ${input}"
fi

json_template="../../config/lakeformation/grants/lf_tag_access_template.json"

if [ ! -f "${json_template}" ]; then
    error "Unable to locate the required JSON template file: ${json_template}"
fi

# Read the CSV file and iterate through records one at a time.
while IFS='|' read -r role_name lf_tag_key1 lf_tag_values1 lf_tag_key2 lf_tag_values2 lf_tag_key3 lf_tag_values3 perms perms_with_grant
do
  # Substitute the "{env}" literal in role_name with the value of $environment, passed through command line.
  role_name_by_env=${role_name/"{env}"/"${environment}"} \
  || error "Environment parameter substitution in role name failed!"

   # Obtain AWS role ARN using environment-specific role_name.
  role_arn=$(aws iam list-roles | \
      jq -r --arg ROLENAME "${role_name_by_env}" '.Roles[] | select (.RoleName == $ROLENAME)' | jq -r .Arn)
  [[ -z "${role_arn}" ]] && error "Unable to determine AWS role ARN for role name: ${role_name}.
      Make sure security token has not expired!"

  info "Now revoking ${perms} from role ARN: ${role_arn},"
  info "using tag key1: ${lf_tag_key1}, tag values1: ${lf_tag_values1}
  and tag key2: ${lf_tag_key2}, tag values2: ${lf_tag_values2}
  and tag key3: ${lf_tag_key3}, tag values3: ${lf_tag_values3}"

  # Substitute the variables in the JSON template with actual values from CSV file.
  temp_v=$(sed -e "s!\${PRINCIPAL}!${role_arn}!" \
              -e "s/\${LF_TAG_KEY1}/${lf_tag_key1}/" \
              -e "s/\${LF_TAG_VALUES1}/${lf_tag_values1}/" \
              -e "s/\${LF_TAG_KEY2}/${lf_tag_key2}/" \
              -e "s/\${LF_TAG_VALUES2}/${lf_tag_values2}/" \
              -e "s/\${LF_TAG_KEY3}/${lf_tag_key3}/" \
              -e "s/\${LF_TAG_VALUES3}/${lf_tag_values3}/" \
              -e "s/\${PERMS}/${perms}/" \
              -e "s/\${PERMS_WITH_GRANT}/${perms_with_grant}/" \
              "${json_template}") \
              || error "Parameter substitution in JSON template file failed!"

  aws lakeformation revoke-permissions --cli-input-json \
    "${temp_v}" \
    || error "aws lakeformation revoke-permissions command failed!"
done <<< "$(tail -n +2 < "${input}" | grep  -v '^#')" # Feed the csv file to the while loop and skip the header and commented lines.

info "Script completed successfully!"
