Webhook
=======
Webhook help to support system integrations. When a spider job is completed, the server will start to send crawled data
to a customized url.

The webhook post data to ``payload_url``, each key/value field is urlencoded before post, unicode data will be treated as UTF8
encoding, and if the value is dict/tuple/list, it will be json enconded. One request for each crawled item.

The frequency of posting data would be no more than 1 request/second.

You can modify spider's webhook settings list this::

    curl -XPOST http://localhost:6800/projects/{projectname}/spiders/{spidername}/webhook -d payload_url = {address}

Or to delete an existing webhook::

    curl -XDELETE http://localhost:6800/projects/{projectname}/spiders/{spidername}/webhook

