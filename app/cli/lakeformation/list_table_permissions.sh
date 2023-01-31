#!/bin/bash
##################################################################################################
# This script can be used to list Lake Formation permissions for a table.
# Note that the max items may need to be adjusted depending on how many results are returned.
##################################################################################################
source ./../env_conf.sh || { echo "Unable to source env_conf.sh"; exit 1; }

SCRIPT_NAME=$(basename "$0")

if [ "$#" -ne 2 ]; then
  printf "ERROR: Wrong number of arguments - required 1 argument; received %s" "$#"
  printf "\nUsage: %s <database-name> <table-name>" "$SCRIPT_NAME"
  printf '\nExample: %s erm_reporting sedgwick_wc_limited"' "$SCRIPT_NAME"
  exit 1
fi

database_name=${1}
table_name=${2}

aws_command="aws lakeformation list-permissions
    --resource={\"Table\":{\"DatabaseName\":\"${database_name}\",\"Name\":\"${table_name}\"}}
    --max-results 10000"

#info "command=${aws_command}"
unset NEXT_TOKEN
OUTPUT="/tmp/list-table-permissions.txt"
cat /dev/null > ${OUTPUT} # Empty out any existing output file.

function parse_output() {
  if [ -n "${cli_output}" ]; then
    echo "${cli_output}" | jq -r '.PrincipalResourcePermissions[]' >> ${OUTPUT}
    NEXT_TOKEN=$(echo "${cli_output}" | jq -r ".NextToken")
  fi
}

# The command is executed with its option passed as a command line argument and output is parsed in the statements below.
cli_output=$($aws_command) || error "${aws_command} failed!"
parse_output

# The while loop below runs until either the command errors due to throttling or
# comes back with a pagination token. To avoid throttling errors, it sleeps for
# three seconds and then tries again.
#info "Now paginating through results..."
while [ "$NEXT_TOKEN" != "null" ]; do
  #info "Paginating through response from command: ${aws_command} --next-token ${NEXT_TOKEN}"
  sleep 1
  cli_output=$(${aws_command} --next-token "${NEXT_TOKEN}") || error "${aws_command} failed!"
  parse_output
done  #pagination loop

# "Now listing PrincipalResourcePermissions..."
if [ -s ${OUTPUT} ]; then
  cat ${OUTPUT}
else
  # The file is empty.
  info "No permissions found using command: ${aws_command}"
fi

#info "Script completed successfully!"
