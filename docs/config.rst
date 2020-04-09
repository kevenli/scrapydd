Configuration
=============
Both server and agent use the ``scrapydd.conf`` file for system configuration.
The file will be looked up in the following locations:

* /etc/scrapydd/scrapydd.conf
* /etc/scrapyd/conf.d/*
* ./scrapydd.conf
* ~/.scrapydd.conf

Config can be also overriden by environment variables, environment variables should have a "SCRAPYDD_" prefix and then the config name with upper case. 
For example to override a server address on agent, an environment variable should be "SCRAPYDD_SERVER=xxx".

Server
------
Server configurations should appears under the ``[server]`` section.


bind_address
~~~~~~~~~~~~~~
The ipaddress which web server bind on. Default: ``0.0.0.0``

bind_port
~~~~~~~~~~
The port web server running on. Default: ``6800``

client_validation
~~~~~~~~~~~~~~~~~~
Whether validate client's certificate on SSL, Default: ``false``

database_url
~~~~~~~~~~~~
Database connection url. This will be passed to the inside sqlalchemy `create_engine` method.
Default: ``sqlite:///database.db``

debug
~~~~~~
Whether run server on debug mode. Debug mode will set logging level to DEBUG.
Default: ``false``.

enable_authentication
~~~~~~~~~~~~~~~~~~~~~
Whether enable authentication, once this option is on, user need to login to make operation.
Default: ``true``

https_port
~~~~~~~~~~~
HTTPS port to listen on, specify this key will enable SSL mode.

Default: None

runner_type
~~~~~~~~~~~
Project package runner, Default: venv.

Available options: venv(run sub-command on VirtuanEnv), docker (run sub-command on
Docker container)

runner_docker_image
~~~~~~~~~~~~~~~~~~~
Runner container image name, Default: kevenli/scrpaydd

This effects when runner_type is docker.

server_name
~~~~~~~~~~~~
Server's hostname.
When SSL enabled, the public certificate will be loaded as filename `server_name`.crt and
private certificate will be loaded as filename `server_name`.key in the ``keys`` folder.
Default: ``localhost``



Agent
-----
Agent configurations should appears under the ``[agent]`` section.

debug
~~~~~~~~
Whether run agent on debug mode. Debug mode will set logging level to DEBUG. Default: ``false``

server
~~~~~~~~~
The IP address or hostname of the server which this agent connect to. Default: ``localhost``

server_port
~~~~~~~~~~~~~~
The port of server. Default: ``6800``

slots
~~~~~~~~
How many concurrent jobs the agent would run. Default: ``1``

request_timeout
~~~~~~~~~~~~~~~~
Request timeout in seconds when communicating to server. Default: ``60``


Example
--------
Server configuration::

    [server]
    bind_address = 0.0.0.0
    bind_port = 6800
    debug = false

Agent configuration::

    [agent]
    server = localhost
    server_port = 6800
    debug = false
    slots = 1
