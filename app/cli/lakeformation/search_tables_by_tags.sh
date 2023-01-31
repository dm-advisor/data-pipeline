#!/bin/bash
##################################################################################################
# This script can be used to search tables by Lake Formation tags for a project.
##################################################################################################
source ./../env_conf.sh || { echo "Unable to source env_conf.sh"; exit 1; }
SCRIPT_NAME=$(basename "$0")

if [ "$#" -ne 1 ]; then
  printf "ERROR: Wrong number of arguments - required 1 argument; received %s" "$#"
  printf "\nUsage: %s path/json-file-name" "$SCRIPT_NAME"
  printf "\nExample: %s ../../config/lakeformation/tags/search_tables_by_lf_tags.csv" "$SCRIPT_NAME"
  exit 1
fi

input=${1}
info "Input file name is set to: ${input}"

if [ ! -f "${input}" ]; then
    error "Unable to locate CSV input file: ${input}"
fi

json_template="../../config/lakeformation/tags/search_tables_by_lf_tags_template.json"

if [ ! -f "${json_template}" ]; then
    error "Unable to locate the required JSON template file: ${json_template}"
fi

# Read the CSV file and iterate through records one at a time.
while IFS='|' read -r tag_key1 tag_values1 tag_key2 tag_values2 tag_key3 tag_values3
do
  # Substitute the variables in the JSON template with actual values from CSV file.
  temp_v=$(sed -e "s/\${TAG_KEY1}/${tag_key1}/" \
              -e "s/\${TAG_VALUES1}/${tag_values1}/" \
              -e "s/\${TAG_KEY2}/${tag_key2}/" \
              -e "s/\${TAG_VALUES2}/${tag_values2}/" \
              -e "s/\${TAG_KEY3}/${tag_key3}/" \
              -e "s/\${TAG_VALUES3}/${tag_values3}/" \
              "${json_template}") \
              || error "Parameter substitution in JSON template file failed!"

  info "Now displaying search results for: ${temp_v}"

  aws lakeformation search-tables-by-lf-tags --page-size 100 --cli-input-json \
  "${temp_v}" \
  || error "aws lakeformation search-tables-by-lf-tags command failed!"
done <<< "$(tail -n +2 < "${input}" | grep  -v '^#')" # Feed the csv file to the while loop and skip the header and commented lines.

info "Script completed successfully!"
