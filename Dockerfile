FROM public.ecr.aws/unocha/python:3.12-stable

WORKDIR /srv/hdx-resource-changedetection

COPY . .

RUN mkdir -p \
    /var/log/hdx-resource-changedetection && \
    pip3 install --upgrade -r requirements.txt && \
    rm -rf /var/lib/apk/* && rm -r /root/.cache

ENTRYPOINT /usr/bin/python

CMD []
