#!/bin/bash
##################################################################################################
# This script can be used to list Lake Formation assigned tag policies.
# Note that the max items may need to be adjusted depending on how many results are returned.
##################################################################################################
source ./../env_conf.sh || { echo "Unable to source env_conf.sh"; exit 1; }

SCRIPT_NAME=$(basename "$0")

if [ "$#" -ne 1 ]; then
  printf "ERROR: Wrong number of arguments - required 1 argument; received %s" "$#"
  printf "\nUsage: %s <resource-type>" "$SCRIPT_NAME"
  printf "\n       where <resource-type> is one of CATALOG, DATABASE, TABLE, DATA_LOCATION, LF_TAG, LF_TAG_POLICY, LF_TAG_POLICY_DATABASE or LF_TAG_POLICY_TABLE"
  printf '\nExample: %s LF_TAG_POLICY"' "$SCRIPT_NAME"
  exit 1
fi

resource_type=${1}
if [[ "${resource_type}" = "CATALOG"  || "${resource_type}" = "DATABASE" \
   || "${resource_type}" = "TABLE"  || "${resource_type}" = "DATA_LOCATION" \
   || "${resource_type}" = "LF_TAG"  || "${resource_type}" = "LF_TAG_POLICY" \
   || "${resource_type}" = "LF_TAG_POLICY_DATABASE"  || "${resource_type}" = "LF_TAG_POLICY_TABLE" ]]; then
    info "resource_type=${resource_type}"
else
    error "The specified resource_type: ${resource_type} is invalid! Valid values are:
    CATALOG, DATABASE, TABLE, DATA_LOCATION, LF_TAG, LF_TAG_POLICY, LF_TAG_POLICY_DATABASE or LF_TAG_POLICY_TABLE"
fi

aws_command="aws lakeformation list-permissions"
aws_command_with_resource_type="${aws_command} --resource-type ${resource_type} --max-results 10000"

info "command=${aws_command_with_resource_type}"
unset NEXT_TOKEN
OUTPUT="/tmp/principal-resource-permissions.txt"
cat /dev/null > ${OUTPUT} # Empty out any existing output file.

function parse_output() {
  if [ -n "${cli_output}" ]; then
    echo "${cli_output}" | jq -r '.PrincipalResourcePermissions[]' >> ${OUTPUT}
    NEXT_TOKEN=$(echo "${cli_output}" | jq -r ".NextToken")
  fi
}

# The command is executed with its option passed as a command line argument and output is parsed in the statements below.
cli_output=$($aws_command_with_resource_type)
parse_output

# The while loop below runs until either the command errors due to throttling or
# comes back with a pagination token.  To avoid throttling errors, it sleeps for
# three seconds and then tries again.
info "Now paginating through results..."
while [ "$NEXT_TOKEN" != "null" ]; do
  info "Paginating through response from command: ${aws_command_with_resource_type} --next-token ${NEXT_TOKEN}"
  sleep 1
  cli_output=$(${aws_command_with_resource_type} --next-token "${NEXT_TOKEN}") || error "${aws_command_with_resource_type} failed!"
  parse_output
done  #pagination loop

info "Now listing PrincipalResourcePermissions based on ${resource_type}..."
if [ -s ${OUTPUT} ]; then
  cat ${OUTPUT}
else
  # The file is empty.
  info "No permissions found using command: ${aws_command_with_resource_type}"
fi

info "Script completed successfully!"
