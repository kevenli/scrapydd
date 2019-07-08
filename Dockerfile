FROM python:2.7-alpine

WORKDIR /scrapydd
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

RUN pip install scrapydd
ENTRYPOINT ["entrypoint.sh"]
CMD ["scrapydd"]

