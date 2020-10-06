#!/bin/bash

# Globals:
#   CONFIG_FILE
# Arguments:
#   $1 The name of the attribute to retrieve.
#######################################
config() {
  local value=`cat "$CONFIG_FILE" | python3 -c "import sys, json; print(json.load(sys.stdin)['$1'])"`
  echo "$value"
}

#######################################
# Sets Global variables based on the provided configuration file.
# Globals:
#   None.
# Arguments:
#   $1 The path to the config file.
#######################################
load_config() {
  CONFIG_FILE="$1"
  COMPOSER_BUCKET=$(config "gcs_composer_bucket")
}

#######################################
# Main function used to perform the deployment.
# Globals:
#   COMPOSER_BUCKET
# Arguments:
#   $1 The path to the config file.
#######################################
main() {
  load_config "$1"

  # clone orchestra to a local temp directory
  rm -rf "/tmp/orchestra"
  git clone --depth=1 \
            --branch "master" \
            "https://github.com/google/orchestra.git" "/tmp/orchestra"

  # install the orchestra plugin
  gsutil -m rsync -r -d \
         -x ".*/example_dags/.*" \
         "/tmp/orchestra/orchestra" \
         "gs://$COMPOSER_BUCKET/plugins/orchestra"

  gsutil rsync -r "./plugins" "gs://$COMPOSER_BUCKET/plugins"
  gsutil rsync -r "./resources" "gs://$COMPOSER_BUCKET/dags/resources"
  gsutil rsync -r "./dags" "gs://$COMPOSER_BUCKET/dags"
  gsutil cp -r "$1" "gs://$COMPOSER_BUCKET/dags"
}


main "$@"

