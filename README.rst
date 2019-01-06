rivulet: A Redis-Based Message Broker for Python
================================================

.. image:: https://travis-ci.org/mkirchner/rivulet.png
   :target: https://travis-ci.org/mkirchner/rivulet
   :alt: Latest Travis CI build status

.. image:: https://readthedocs.org/projects/rivulet/badge/?version=latest
   :target: https://rivulet.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status


Documentation: `Read the docs <https://rivulet.readthedocs.io/en/latest/>`_.


Quickstart
----------

    $ pip install rivulet


Notes
-----

Limitations
^^^^^^^^^^^

* No balanced consumers (yet)
* Without the proper management tools (see todos), managing messages is
  painful raw redis.

Todos
^^^^^

* Extend testing

  * Connection drops
  * Parallel producers, consumers (stressing the locking setup)

* Provies management functionality

  * list channels, delete channels
  * count, list, update subscribers
  * count, list, prune messages


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

