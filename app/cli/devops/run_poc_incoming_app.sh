#!/bin/bash
##################################################################################################
# This script can be used to provision poc_incoming application resources and
# grant/revoke Lake Formation permissions.
##################################################################################################
source ./env_conf.sh || { echo "Unable to source env_conf.sh"; exit 1; }
SCRIPT_NAME=$(basename "$0")
# Set default logging level to "error" if it is not specified as the 2nd command line argument.
LOG_LVL=${2:-info}

if (( $# < 1 || $# > 2 )); then
  printf "ERROR: Wrong number of arguments - required 1 argument; received %s" "$#"
  printf "\nUsage: %s arg1 - where arg1=<target-environment>" "$SCRIPT_NAME"
  printf "\n\t* arg2 is an optional argument for changing the default logging level from error to one of debug, info or critical."
  printf "\nExample: %s uat debug" "$SCRIPT_NAME"
  exit 1
fi

info "Environment is set to: $1"
export branchEnv=$1

info "Validating JSON application config file against its schema..."
python ../bin/config_validator.py -l "${LOG_LVL}" poc app config/schema/app_config_schema.json config/poc_incoming_config.json \
     || error "Application config validation failed!"

#info "Validating JSON Lake Formation permissions config file against its schema..."
#python ../bin/config_validator.py -l "${LOG_LVL}" poc app config/schema/perm_config_schema.json config/poc_incoming_config_perm.json \
#     || error "Lake Formation permission config validation failed!"

info "Provisioning application resources..."
python ../bin/app.py poc config/poc_incoming_config.json -l "${LOG_LVL}" || \
       error "Program: app.py failed! Return code=$?"

#info "Granting Lake Formation permissions..."
#python ../bin/manage_table_perms.py -l "${LOG_LVL}" \
#    grant poc app config/poc_incoming_config.json config/poc_incoming_config_perm.json || \
#       error "Program: manage_table_perms.py failed! Return code=$?"

info "Script completed successfully!"
