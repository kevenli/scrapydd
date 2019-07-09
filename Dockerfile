FROM alpine

WORKDIR /scrapydd
RUN apk add python2 py2-pip py2-twisted py2-cffi python2-dev libffi-dev openssl-dev gcc libc-dev make libxml2-dev libxslt-dev
ADD . /scrapydd_src
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN pip install -r /scrapydd_src/requirements.txt
RUN python /scrapydd_src/setup.py install
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["scrapydd"]