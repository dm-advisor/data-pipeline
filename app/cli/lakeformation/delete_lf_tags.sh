#!/bin/bash
##################################################################################################
# This script can be used to delete Lake Formation tags for a project.
#
# While coding/editing the CSV input file be sure to use Linux line feed (LF) instead of
# Windows (CRLF).
##################################################################################################
source ./../env_conf.sh || { echo "Unable to source env_conf.sh"; exit 1; }
SCRIPT_NAME=$(basename "$0")

if [ "$#" -ne 1 ]; then
  printf "ERROR: Wrong number of arguments - required 1 argument; received %s" "$#"
  printf "\nUsage: %s path/csv-input-file-name" "$SCRIPT_NAME"
  printf "\nExample: %s ../../config/lakeformation/tags/delete_lf_tag.csv" "$SCRIPT_NAME"
  exit 1
fi

input=$1
info "Input file name is set to: ${input}"

if [ ! -f "${input}" ]; then
    error "Unable to locate CSV input file: ${input}"
fi

json_template="../../config/lakeformation/tags/delete_lf_tag_template.json"

if [ ! -f "${json_template}" ]; then
    error "Unable to locate the required JSON template file: ${json_template}"
fi

account=$(aws sts get-caller-identity | jq -r '.Account') || error "Call to get-caller-identity failed!"
[[ -z "${account}" ]] && error "Unable to identify AWS account number using get-caller-identity"

# Read the CSV file and iterate through records one at a time.
while IFS='|' read -r tag_key
do
  info "Now deleting: tag_key=${tag_key}"
  # Substitute the variables in the JSON template with actual values from CSV file.
  temp_v=$(sed -e "s/\${ACCOUNT}/${account}/" \
              -e "s/\${TAG_KEY}/${tag_key}/" \
              "${json_template}") \
              || error "Parameter substitution in JSON template file failed!"

  aws lakeformation delete-lf-tag --cli-input-json \
    "${temp_v}" \
    || error "aws lakeformation delete-lf-tag command failed!"
done <<< "$(tail -n +2 < "${input}" | grep  -v '^#')" # Feed the csv file to the while loop and skip the header and commented lines.

info "Now listing any remaining LF tags..."
aws lakeformation list-lf-tags || error "aws lakeformation list-lf-tags failed!"

info "Script completed successfully!"
