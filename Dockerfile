FROM alpine

WORKDIR /scrapydd
RUN apk add python2 py2-pip py2-twisted py2-cffi
ADD . /scrapydd_src
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN pip install -r /scrapydd_src/requirements.txt
RUN python /scrapydd_src/setup.py install
ENTRYPOINT ["entrypoint.sh"]
CMD ["scrapydd"]