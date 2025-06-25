FROM public.ecr.aws/unocha/python:3.13-stable

WORKDIR /srv/hdx-resource-changedetection

COPY . .

COPY docker/entrypoint.sh /

RUN apk add --no-cache gettext-envsubst && \
    mkdir -p /var/log/hdx-resource-changedetection && \
    pip3 install --no-cache-dir --upgrade -r requirements.txt && \
    rm -rf /var/lib/apk/* && rm -r /root/.cache && \
    chmod +x /entrypoint.sh

ENTRYPOINT [ "/entrypoint.sh" ]

CMD []
