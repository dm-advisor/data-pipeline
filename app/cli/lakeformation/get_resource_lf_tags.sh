#!/bin/bash
##################################################################################################
# This script can be used to get Lake Formation tags that are assigned to resources (i.e.,
# tables and columns) for a project.
#
# While coding/editing the CSV input file be sure to use Linux line feed (LF) instead of
# Windows (CRLF).
##################################################################################################
source ./../env_conf.sh || { echo "Unable to source env_conf.sh"; exit 1; }
SCRIPT_NAME=$(basename "$0")

if [ "$#" -ne 1 ]; then
  printf "ERROR: Wrong number of arguments - required 1 argument; received %s" "$#"
  printf "\nUsage: %s path/csv-input-file-name" "$SCRIPT_NAME"
  printf "\nExample: %s ../../config/lakeformation/tags/get_resource_lf_tags.csv" "$SCRIPT_NAME"
  exit 1
fi

input=$1
info "Input file name is set to: ${input}"

if [ ! -f "${input}" ]; then
    error "Unable to locate CSV input file: ${input}"
fi

json_template="../../config/lakeformation/tags/get_resource_lf_tags_template.json"

if [ ! -f "${json_template}" ]; then
    error "Unable to locate the required JSON template file: ${json_template}"
fi

# Read the CSV file and iterate through records one at a time.
while IFS='|' read -r db_name table_name
do
  info "Now getting tags for table: ${table_name} in database: ${db_name}"
  #info "using LF tag key1: ${lf_tag_key1}, tag value1: ${lf_tag_value1} and LF tag key2:
  #      ${lf_tag_key2}, tag value2: ${lf_tag_value2}"

  # Substitute the variables in the JSON template with actual values from CSV file.
  temp_v=$(sed -e "s/\${DB_NAME}/${db_name}/" \
               -e "s/\${TABLE_NAME}/${table_name}/" \
              "${json_template}") \
              || error "Parameter substitution in JSON template file failed!"

  aws lakeformation get-resource-lf-tags --cli-input-json \
    "${temp_v}" \
    || error "aws lakeformation get-resource-lf-tags failed!"
done <<< "$(tail -n +2 < "${input}" | grep  -v '^#')" # Feed the csv file to the while loop and skip the header and commented lines.

info "Script completed successfully!"
