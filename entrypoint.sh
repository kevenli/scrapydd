#!/bin/sh
if [ "$1" = 'scrapydd' ]; then
    shift
fi

set -- scrapydd $@
exec "$@"
