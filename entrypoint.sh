#!/bin/bash
set -e
source /opt/conda/etc/profile.d/conda.sh
conda activate
set -x

[[ -f /run/secrets/ALERT_MAIL_PASSWORD ]] && \
    ALERT_MAIL_PASSWORD="$(cat /run/secrets/ALERT_MAIL_PASSWORD)"
[[ -f /run/secrets/ALERT_MAIL_LOGIN ]] && \
    ALERT_MAIL_LOGIN="$(cat /run/secrets/ALERT_MAIL_LOGIN)"

python run.py \
    -p "${ALERT_MAIL_PASSWORD}" \
    --login "${ALERT_MAIL_LOGIN}"\
    --db_url "${MONGODB_URI}"
