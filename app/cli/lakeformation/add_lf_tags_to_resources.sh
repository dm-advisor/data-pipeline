#!/bin/bash
##################################################################################################
# This script can be used to assign (add) Lake Formation tags to tables and columns for a project.
#
# While coding/editing the CSV input file be sure to use Linux line feed (LF) instead of
# Windows (CRLF).
#
# To verify that tags have been assigned properly, run the following script from
# $PROJECT_ROOT/app/cli/lakeformation directory:
# ./get_resource_lf_tags.sh ../../config/lakeformation/tags/get_resource_lf_tags.csv
##################################################################################################
source ./../env_conf.sh || { echo "Unable to source env_conf.sh"; exit 1; }
SCRIPT_NAME=$(basename "$0")

if [ "$#" -ne 1 ]; then
  printf "ERROR: Wrong number of arguments - required 1 argument; received %s" "$#"
  printf "\nUsage: %s path/csv-input-file-name" "$SCRIPT_NAME"
  printf "\nExample: %s ../../config/lakeformation/tags/add_lf_tags_to_resource.csv" "$SCRIPT_NAME"
  exit 1
fi

input=$1
info "Input file name is set to: ${input}"

if [ ! -f "${input}" ]; then
    error "Unable to locate CSV input file: ${input}"
fi

json_table_template="../../config/lakeformation/tags/add_lf_tags_table_template.json"
json_columns_template="../../config/lakeformation/tags/add_lf_tags_columns_template.json"

if [ ! -f "${json_table_template}" ] || [ ! -f "${json_columns_template}" ]; then
    error "Unable to locate one or both required JSON template files: ${json_table_template}
    and ${json_columns_template}"
fi

# Read the CSV file and iterate through records one at a time.
while IFS='|' read -r db_name resource_type resource_name lf_tag_key lf_tag_value column_names
do
  if [[ "${resource_type}" == "table" ]]; then
    info "Now adding tags to table: ${resource_name} in database: ${db_name},"
    info "using LF tag key: ${lf_tag_key} and tag value: ${lf_tag_value}"
    # Substitute the variables in the JSON template with actual values from CSV file.
    temp_v=$(sed -e "s/\${DB_NAME}/${db_name}/" \
                 -e "s/\${TABLE_NAME}/${resource_name}/" \
                 -e "s/\${LF_TAG_KEY}/${lf_tag_key}/" \
                 -e "s/\${LF_TAG_VALUE}/${lf_tag_value}/" \
              "${json_table_template}") \
              || error "Parameter substitution in JSON template file failed while processing table!"
  elif [[ "${resource_type}" == "columns" ]]; then
    info "Now adding tags to columns: ${column_names} in table: ${resource_name} in database: ${db_name},"
    info "using LF tag key: ${lf_tag_key} and tag value: ${lf_tag_value}"
    # Substitute the variables in the JSON template with actual values from CSV file.
    temp_v=$(sed -e "s/\${DB_NAME}/${db_name}/" \
                 -e "s/\${TABLE_NAME}/${resource_name}/" \
                 -e "s/\${LF_TAG_KEY}/${lf_tag_key}/" \
                 -e "s/\${LF_TAG_VALUE}/${lf_tag_value}/" \
                 -e "s/\${COLUMN_NAMES}/${column_names}/" \
              "${json_columns_template}") \
              || error "Parameter substitution in JSON template file failed while processing columns!"
  else
    error "Invalid resource type in the CSV input file. Valid values are 'table' and 'columns'!"
  fi

  aws lakeformation add-lf-tags-to-resource --cli-input-json \
    "${temp_v}" \
    || error "aws lakeformation add-lf-tags-to-resource failed!"
done <<< "$(tail -n +2 < "${input}" | grep  -v '^#')" # Feed the csv file to the while loop and skip the header and commented lines.

info "Script completed successfully!"
