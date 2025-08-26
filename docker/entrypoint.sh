#!/bin/sh
set -e

cd /srv/hdx-resource-changedetection
# Transform config templates if they exist
for template in docker/.useragents.yaml.tpl docker/.hdx_configuration.yaml.tpl; do
    [ -f "$template" ] || continue
    filename=$(basename "$template" .tpl)  # basename + remove .tpl
    envsubst < "$template" > "/root/$filename"
done

PYTHON=$(which python3 || which python)
# Execute Python with original args
exec "$PYTHON" "$@"
