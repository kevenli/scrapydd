Node Registration
=================
Scrapydd is a distributed system, mainly consist of two two roles:
server and worker (also known as agent).

System administrator can add/remove node with the WebUI and some commands
on the node machine.

Concepts
---------
There are two types of workers:

Temporary Node:
    This type of worker runs without the need of
    pre-registration, and once it is closed for any reason, a new node
    will be created when it goes online again.
Permanent Node:
    This type of worker needs to be registered before it can run.
    Each time its process go online back, it relates to the same Node.

Temporary node is intended to be use in small deployment to provide
easier way to use, along with the `enable_authentication` settings set
to `false`. It is *STRONGLY RECOMMENDED* to enable authentication
for any business use.


Registration Process
---------------------
To register a Permanent Node, the Sys Admin need to operate the follow
processes.

1. Go to the `Admin Area` - `Nodes` Page.
2. Copy `New Node Key` and `SecretKey`. Each pair of keys will last few minutes.
3. Go to the worker machine, first check the `server`, `tags` settings_ under the `agent` section are set to appropriate value.
4. Run `scrapydd agent -g`, input `node_key` and `secretkey` following the prompt.

.. _settings: config.html

