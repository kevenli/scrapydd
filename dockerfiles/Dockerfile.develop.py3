FROM scrapydd

WORKDIR /src
ADD tests ./tests
ADD samples ./samples
COPY requirements_test.txt ./
RUN pip install -r requirements_test.txt
