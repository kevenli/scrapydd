====================================
ScrapyDD (Scrapy Distributed Daemon)
====================================

.. image:: https://img.shields.io/pypi/v/scrapydd.svg
   :target: https://pypi.python.org/pypi/scrapydd
   :alt: PyPI Version

.. image:: https://img.shields.io/travis/kevenli/scrapydd/master.svg
   :target: http://travis-ci.org/kevenli/scrapydd
   :alt: Build Status

.. image:: https://img.shields.io/codecov/c/github/kevenli/scrapydd/master.svg
   :target: http://codecov.io/gh/kevenli/scrapydd?branch=master
   :alt: Coverage report


Scrapydd is a system for scrapy spiders distributed running and scheduleing system, including server and client agent.

Advantages:
===========
* Distributed, easily add runner(agent) to scale out.
* Project requirements auto install.
* Cron expression time driven trigger, run your spider on time.
* Webhook loosely couple the data crawling and data processing.
* Spider status insight, system will look into the log to clarify spider health status.


Installing Scrapydd
-------------------
By pip::

    pip install scrapydd

You can also install scrapydd manually:

1. Download compressed package from `github releases`_.
2. Decompress the package
3. Run ``python setup.py install``


Run Scrapydd Server
-------------------

    scrapydd server 
    
The server default serve on 0.0.0.0:6800, with both api and web ui.
Add --daemon parameter in commmand line to run in background.

Run Scrapydd Agent
------------------

    scrapydd agent
    
Add --daemon parameter in commmand line to run in background.


Docs
----
The docs is hosted `here`_


.. _here: http://scrapydd.readthedocs.org
