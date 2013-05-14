#!/bin/bash
#
# Copyright 2013 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -o errexit
declare -r NEED_JAVA="You need to install the JRE in order to use the datastore SDK."
JAVA=$(which java) || (echo ${NEED_JAVA} && false)
declare -r GCD_DIR=$(dirname "$0")
declare -r APPENGINE_SDK_DIR="${APPENGINE_SDK_DIR:-${GCD_DIR}/.appengine}"
declare -r GCD_WAR_DIR="${GCD_WAR_DIR:-${GCD_DIR}/.war}"
declare -r USAGE="usage: gcd <start|update_indexes|vacuum_indexes> <directory> [-A <dataset_id>] [options]"
declare -r COMMAND="${1:?${USAGE}}"; shift
declare -r GCD_APP_DIR="${1:?${USAGE}}"; shift
mkdir -p "${GCD_APP_DIR}"
cp -R "${GCD_WAR_DIR}"/* "${GCD_APP_DIR}" # always update the servlet configuration
declare -r OPTION=${1}
if [[ "${OPTION}" = "-A" ]]; then
  shift
  GCD_DATASET_ID="${1}"
  shift
fi
EXTRA_OPTIONS=""
if (echo $@ \
  | grep -v -- "--property=datastore.default_high_rep_job_policy_unapplied_job_pct=" > /dev/null) then
  EXTRA_OPTIONS="--property=datastore.default_high_rep_job_policy_unapplied_job_pct=10"
fi
GCD_DATASET_ID="${GCD_DATASET_ID:-$(basename "${GCD_APP_DIR}")}"
# update the application id in the app directory to match the dataset
sed -i -e "s/<application>[a-z]*<\/application>/<application>${GCD_DATASET_ID}<\/application>/g" \
  ${GCD_APP_DIR}/WEB-INF/appengine-web.xml
declare -r DEV_APPSERVER="${APPENGINE_SDK_DIR}/bin/dev_appserver.sh"
declare -r DEV_APPSERVER_OPTIONS="--disable_update_check --jvm_flag=-Doauth.is_admin=true ${EXTRA_OPTIONS}"
declare -r APPCFG="${APPENGINE_SDK_DIR}/bin/appcfg.sh"
declare -r APPCFG_OPTIONS=""
declare -r DATASTORE_BASE_URL="datastore"
declare -r DATASTORE_API_VERSION="v1"

case "${COMMAND}" in
  start)
    "${DEV_APPSERVER}" ${DEV_APPSERVER_OPTIONS} $@ "${GCD_APP_DIR}"
    ;;
  update_indexes)
    "${APPCFG}" ${APPCFG_OPTIONS} -A "${GCD_DATASET_ID}" $@ update_indexes "${GCD_APP_DIR}"
    ;;
  vacuum_indexes)
    "${APPCFG}" ${APPCFG_OPTIONS} -A "${GCD_DATASET_ID}" $@ vacuum_indexes "${GCD_APP_DIR}"
    ;;
  *)
    echo ${USAGE}
    ;;
esac
