FROM python:2.7-alpine

WORKDIR /scrapydd
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

ADD . /scrapydd
RUN pip install -r requirements.txt
RUN python setup.py install
ENTRYPOINT ["entrypoint.sh"]
CMD ["scrapydd"]

