#!/bin/bash
##################################################################################################
# This script can be used to list Lake Formation tags for a project.
##################################################################################################
source ./../env_conf.sh || { echo "Unable to source env_conf.sh"; exit 1; }
SCRIPT_NAME=$(basename "$0")

if [ "$#" -ne 0 ]; then
  printf "ERROR: Wrong number of arguments - required 0 argument; received %s" "$#"
  printf "\nUsage: %s" "$SCRIPT_NAME"
  exit 1
fi

info "Now listing LF tags..."
aws lakeformation list-lf-tags || error "aws lakeformation list-lf-tags command failed!"

info "Script completed successfully!"
