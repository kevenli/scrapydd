Configuration
=============

Server
------

`bind_address`: The ipaddress which web server bind on. Default: ``0.0.0.0``

`bind_port`: The port web server running on. Default: ``6800``

`debug`: Whether run server on debug mode. Debug mode will set logging level to DEBUG.
Default: ``false``.

Agent
-----

`debug`: Whether run agent on debug mode. Debug mode will set logging level to DEBUG. Default: ``false``

`server`: The ip/dns server which agent connect to. Default: ``localhost``

`server_port`: The port of server. Default: ``6800``

`slots`: How many concurrent jobs the agent would run. Default: ``1``