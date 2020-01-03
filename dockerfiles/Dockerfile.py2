FROM alpine

WORKDIR /scrapydd
RUN apk add python2 py2-pip py2-twisted py2-cffi python2-dev libffi-dev \
    openssl-dev gcc libc-dev make libxml2-dev libxslt-dev \
    tzdata jpeg-dev zlib-dev freetype-dev lcms2-dev openjpeg-dev tiff-dev tk-dev tcl-dev
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
RUN pip install scrapydd
ENV TZ /usr/share/zoneinfo/Etc/UTC
CMD ["scrapydd"]