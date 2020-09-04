Node API
========
API used for communicating between server and worker node.
*This document is for system developer only*.

Node Resource
-------------

Fields:

- name (string):
    ``nodes/*``. Readonly. A restful resource name ResourceName_.
- id (int64):
    A unique 64-bit integer generated with Snowflake algorithm.
- display_name (string):
    Human readable name of the node, if not specified at
    registration (creation), a random display_name will be assigned by server. User can
    modify it at runtime.
- tags (list<string>):
    Tags the node can support.
- node_key (string):
    The node_key used to register a permanent node. The detail of node
    registration see NodeRegistration_ .
- is_online (bool):
    *Readonly*, whether the current node is online, this field cannot
    be modified manually.
- client_ip (string):
    *Readonly*, on which ip address the node client process connected to
    server.


.. _NodeRegistration: node_registration.html
.. _ResourceName: https://cloud.google.com/apis/design/resource_names#resource_name_as_string

Create Node
~~~~~~~~~~~
**POST /v1/nodes**

Body Parameter:
    Node

    Required fields:

    - node_key


Parameters:

Response::

    200 OK
    {
        "id": 1913485971031199744,
        "name": "nerdy-cyan-rottweiler",
        "tags": [
            "CA",
            "AllDay"
        ],
        "is_online": true,
        "client_ip": "127.0.0.1"
    }


Exceptions:

* 400: Invalid Parameter.
* 401: Unauthenticated request.
* 401: Permission denied.