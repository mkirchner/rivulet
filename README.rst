rivulet
=======

.. image:: https://travis-ci.org/mkirchner/rivulet.png
   :target: https://travis-ci.org/mkirchner/rivulet
   :alt: Latest Travis CI build status

Redis-Based Message Streaming for Python

.. contents::
   :local:

Quickstart
----------

Installation
^^^^^^^^^^^^

.. code-block:: bash

    $ pip install rivulet


Usage
^^^^^

.. code-block:: python

    # create a client (subscriptions are optional)
    c = rivulet.connect(redis_url)

    channels = ['my-channel-0', 'my-channel-1']

    # subscribe using a policy ('earliest', 'current', or 'latest')
    c.subscribe(channels)

    # read from a channel, blocks if the timeout is negative
    inbox = c.read()
    # this returns a map
    #
    # {
    #     "my-channel-1": [
    #         {"id": 123,
    #          "ts": 123465623,  # timestamp, ms since epoch
    #          "src": "some-client-id",
    #          "data": "the message as a string"}, ...],
    #     "my-channel-2": ...
    # }
    for channel, messages in inbox:
        for message in messages:
            do_something(message['data'])

    # write to a channel (requires a subscription to channel channel_id)
    c.write(channel_id, messages)

    # unsusbscribe
    c.unsubscribe(channels)


License
-------

MIT


Notes
-----

Todos
^^^^^

* reconnect 
* management functionality

  * list channels, delete channels
  * count, list, update subscribers
  * cound, list, prune messages
  * etc


Implementation details
^^^^^^^^^^^^^^^^^^^^^^

Data model:

1. :code:`rvl:lock:<channel_id>`: Used to maintain locks across
   multi-step redis calls (in particular when sending a message using
   a server-issued, monotonically increasing message seq id).
2. :code:`rvl:id:<channel_id>`: A counter providing sequence ids for
   messages in channel `channel_id`
3. :code:`rvl:msg:<channel_id>`: ZSET that maps a packed JSON string
   to the message sed id (allowing range queries on messages over seq
   ids).
4. :code:`rvl:channel:<channel_id>`: ZSET that maps a client id to the last
   message id in channel `channel_id` seen by the clinet
5. :code:`rvl:index:<client_id>`: ZSET that maps the channel_id to the last
   message id seen by client `client_id`

