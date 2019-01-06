**********
Quickstart
**********

Install
=======

.. code-block:: bash

    $ pip install rivulet


A simple example
================

.. code-block:: python

    # create a client
    c = rivulet.connect(redis_url)

    # subscribe
    channels = ['my-channel-0', 'my-channel-1']
    c.subscribe(channels)

    # write to a channel
    c.write(channel_id, messages)

    # read from a channel
    inbox = c.read()
    # inbox = {
    #     "my-channel-1": [
    #         {"id": 123,
    #          "ts": 123465623,  # timestamp, ms since epoch
    #          "src": "some-client-id",
    #          "data": "the message as a string"}, ...],
    #     "my-channel-2": ...
    # }

    # process all messages in all channels
    for channel, messages in inbox:
        for message in messages:
            do_something(message['data'])

    # unsusbscribe
    c.unsubscribe(channels)

