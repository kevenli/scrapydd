FROM kevenli/scrapydd-dockerbase:py3

RUN apk add libffi-dev \
    openssl-dev gcc libc-dev make libxml2-dev libxslt-dev \
    tzdata jpeg-dev zlib-dev freetype-dev lcms2-dev openjpeg-dev tiff-dev tk-dev tcl-dev
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
WORKDIR /src
ADD scrapydd ./scrapydd
COPY setup.py requirements.txt README.rst ./
RUN pip install -r requirements.txt
RUN python setup.py install
WORKDIR /app
ENV TZ /usr/share/zoneinfo/Etc/UTC
CMD ["scrapydd"]