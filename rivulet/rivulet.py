"""
**************************
The :code:`rivulet` client
**************************

"""

import enum
import json
import time
import uuid

from typing import List, Dict, Union

import redis
import redis.exceptions
# pylint: disable=unused-import
from redis.client import Pipeline
# pylint: disable=redefined-builtin
from rivulet.exceptions import ConnectionError, BackendError, TimeoutError


class IndexPolicy(enum.Enum):
    """Enum of rivulet index policies."""
    EARLIEST = 0
    CURRENT = 1
    LATEST = 2


class Client:
    """A redis-based message broker."""

    def __init__(self,
                 redis_url: str,
                 client_id: str = None,
                 channel_ids: List[str] = None,
                 bufsize: int = 4096,
                 **redis_args) -> None:
        try:
            self.redis = redis.from_url(
                redis_url,
                encoding='utf-8',
                decode_responses=True,
                **redis_args)
        except redis.exceptions.ConnectionError as e:
            raise ConnectionError from e
        except redis.exceptions.RedisError as e:
            raise BackendError from e
        self.bufsize = bufsize
        self.client_id = client_id if client_id else uuid.uuid4().hex
        if channel_ids:
            self.subscribe(channel_ids)

    @property
    def subscriptions(self) -> List[str]:
        """
        Returns the list of channels this client is subscribed to.

        :return: A list of channel names.
        :rtype: List[str]
        :raises: BackendError, if anythong goes wrong with Redis.

        .. note:: The implementation always actively queries the current list
            of channel subscription from the Redis backend, the client does not
            hold state and does not cache.

        """
        try:
            subs = self.redis.zrange(f'indexes:client#{self.client_id}', 0, -1)
            return subs
        except redis.exceptions.RedisError as e:
            raise BackendError from e

    def ping(self) -> bool:
        """
        Ping the backend.

        :returns: True if successful.

        """
        try:
            return self.redis.ping()
        except redis.exceptions.RedisError as e:
            raise ConnectionError from e

    def subscribe(self,
                  channel_ids: List[str],
                  index_policy: IndexPolicy = IndexPolicy.CURRENT,
                  timeout_ms: int = 1000) -> None:
        """
        Subscribe to a list of channels.

        :param channel_ids: A list of channels to subscribe to.
        :param index_policy: The index policy for channel subscription,
                             one of :code:`EARLIEST`, :code:`CURRENT`,
                             :code:`LATEST`.
        :param timeout_ms: Time in milliseconds to wait for successful
                           subscription. If subscription fails with a
                           `TimeoutError`, the client is free to retry (use
                           a backoff policy to avoid lock contention).
        :raises: :code:`TimeoutError` if subscription fails withing the
                 specified :code:`timeout_ms` interval. :code:`BackendError`
                 for any other redis failures.


        Index policies define how the client determines which message marks
        the begin of a subscription:

        :code:`IndexPolicy.EARLIEST`:
            The subscriptions starts with the earliest message that is
            still available in the channel.

        :code:`IndexPolicy.CURRENT`:
            The subscription starts with the current index of the client.
            The current index is stored in the redis backend. This is a
            helpful option to restart a subscription after the client was
            restarted or similar. If there is no stored index for a
            particular client & channel combination, `CURRENT` falls back to
            `LATEST`.

        :code:`IndexPolicy.LATEST`:
            The subscription starts with the latest (i.e. most recent)
            message in the channel.

        """
        pipeline = self.redis.pipeline(transaction=True)
        for channel_id in channel_ids:
            try:
                with self.redis.lock(
                        f'lock:ids:channel#{channel_id}',
                        timeout=timeout_ms / 1000.0):

                    # pull the current indexes for the channel
                    zset = self.redis.zrange(
                        f'clients:channel#{channel_id}',
                        0,
                        -1,
                        withscores=True)
                    if zset:
                        client_ids, indexes = zip(*zset)
                    else:
                        client_ids, indexes = [], [0]

                    if index_policy == IndexPolicy.EARLIEST:
                        index = min(indexes)
                    elif index_policy == IndexPolicy.CURRENT:
                        if self.client_id in client_ids:
                            continue
                        else:
                            # CURRENT fallback is LATEST
                            index = max(indexes)
                    elif index_policy == IndexPolicy.LATEST:
                        index = max(indexes)

                    pipeline.zadd(f'clients:channel#{channel_id}',
                                  {self.client_id: index})
                    pipeline.zadd(f'indexes:client#{self.client_id}',
                                  {channel_id: index})
                    pipeline.execute()
            except redis.exceptions.LockError as e:
                raise TimeoutError from e
            except redis.exceptions.RedisError as e:
                raise BackendError from e

    def unsubscribe(self, channel_ids: List[str]) -> None:
        """
        Unsubscribes the client from a set of channels.

        After unsubscribing the client, the function removes all
        messages in the channel that have been read by all remaining
        subscribers.
        If there are no subscribers left the call will also delete
        all remaining messages and control data structures from the
        redis server.

        :param channel_ids: A list of channels the unsubscribe from.
                            Invalid or non-existent channels will *not* raise
                            an error.

        """
        try:
            pipeline = self.redis.pipeline(transaction=True)
            for channel_id in channel_ids:
                pipeline.zrem(f'clients:channel#{channel_id}', self.client_id)
                pipeline.zrem(f'indexes:client#{self.client_id}', channel_id)
                # count the number of remaining clients in the channel
                pipeline.zcard(f'clients:channel#{channel_id}')
                responses = pipeline.execute()
                if responses[-1]:
                    # drop the oldest messages
                    min_score = self.redis.zrange(
                        f'clients:channel#{channel_id}', 0, 0,
                        withscores=True)[0][1]
                    self.redis.zremrangebyscore(
                        f'messages:channel#{channel_id}', 0, min_score)

                else:
                    # there are no subscribers to the channel anymore,
                    # drop all messages
                    pipeline.delete(f'messages:channel#{channel_id}')
                    pipeline.delete(f'ids:channel#{channel_id}')
                    pipeline.execute()
        except redis.exceptions.RedisError as e:
            raise BackendError from e

    def write(self, channel_id: str, data: str,
              timeout_ms: int = 10000) -> None:
        """
        Write a message to a topic.

        :param channel_id: The name of the channel to write to.
        :param data: The message data.
        :param timeout_ms: (optional) User-specified write timeout
                           (default=10s)
        :raises: :code:`TimeoutError` if the message cannot be send within the
                 specified :code:`timeout_ms` interval. :code:`BackendError`
                 for any other redis failures.

        The function obtains a unique (within the topic) message id from the
        redis server wraps the data into a message envelope and adds the
        message to the topic.

        A message is a simple python dictionary:

        .. code-block:: python

                message = {
                    'id': message_id,
                    'ts': timestamp_in_ms_since_epoch,
                    'src': client_id,
                    'data': data
                }
        """
        try:
            with self.redis.lock(
                    f'lock:ids:channel#{channel_id}',
                    timeout=timeout_ms / 1000.0):
                message_id = self.redis.incr(f'ids:channel#{channel_id}')
                ts_ms = int(time.time() * 10**6)  # microseconds since epoch
                message = json.dumps({
                    'id': message_id,
                    'ts': ts_ms,
                    'src': self.client_id,
                    'data': data
                })
                self.redis.zadd(f'messages:channel#{channel_id}',
                                {message: message_id})
        except redis.exceptions.LockError as e:
            raise TimeoutError from e
        except redis.exceptions.RedisError as e:
            raise BackendError from e

    def read(self, message_limit: int = 512
             ) -> Dict[str, List[Dict[str, Union[int, str]]]]:
        """
        Read available messages from all channel subscriptions.

        :param message_limit: Maximum number of messages to read from a
                              topic in a single call. The maximum overall
                              number of retrieved messages is
                              (:code:`message_limit` x number of subscribed
                              topics)
        :return: A dictionary with subscribed topics as keys and a list of
                 messages for every topic as values.
        :raises: :code:`BackendError` if there are any redis backend
                 errors.

        The messages returned for every topic are simple python dictionaries.
        The data sent with :code:`write()` is available in the :code:`data`
        field.

        .. code-block:: python

                message = {
                    'id': message_id,
                    'ts': timestamp_in_ms_since_epoch,
                    'src': client_id,
                    'data': data
                }
        """
        try:
            current_indexes = self.redis.zrange(
                f'indexes:client#{self.client_id}', 0, -1, withscores=True)
        except redis.exceptions.RedisError as e:
            raise BackendError from e

        # the code below requires rework for balanced consumer locking
        pipeline = self.redis.pipeline(transaction=True)
        pipeline2 = self.redis.pipeline(transaction=True)

        for channel_id, current_index in current_indexes:
            pipeline.zrangebyscore(f'messages:channel#{channel_id}',
                                   current_index + 1,
                                   current_index + message_limit)
            pipeline2.zrange(
                f'clients:channel#{channel_id}', 0, -1, withscores=True)

        try:
            raw_messages = pipeline.execute()
            channel_indexes = pipeline2.execute()
        except redis.exceptions.RedisError as e:
            raise BackendError from e

        inbox = zip(current_indexes, channel_indexes, raw_messages)

        message_lists = {}

        for ((channel_id, current_index), channel_indexes,
             raw_messages) in inbox:
            if not raw_messages:
                continue
            messages = [
                json.loads(raw_message) for raw_message in raw_messages
            ]
            newest_index = messages[-1]['id']
            pipeline.zadd(f'indexes:client#{self.client_id}',
                          {channel_id: newest_index})
            pipeline.zadd(f'clients:channel#{channel_id}',
                          {self.client_id: newest_index})
            # check this
            _, indexes = zip(*channel_indexes)
            min_index = min(indexes)
            if min_index - self.bufsize > current_index:
                pipeline.zremrangebyscore(f'messages:channel#{channel_id}', 0,
                                          min_index)
            message_lists.update({channel_id: messages})

        try:
            pipeline.execute()
        except redis.exceptions.RedisError as e:
            raise BackendError from e
        return message_lists


def connect(redis_url: str, client_id=None, channel_ids=None,
            **redis_args) -> Client:
    """
    Convenience function to create a rivulet client.

    :redis_url: A conncetion URL for the redis backend, of the form
                `https://[:password]@server:port/db`.
    :client_id: A unique identifier for the client (default=uuid).
    :channel_ids: A list of channel ids to connect to (optional).
    :redis_args: Additional arguments to pass on to the redis `from_url()`
                 constructor.
    :return: A `rivulet` client object.
    :raises:
        `ConnectionError` if a connection to the redis server cannot be established.
        `BackendError` if subscriptions cannot be carried out successfully.
    """
    return Client(
        redis_url, client_id=client_id, channel_ids=channel_ids, **redis_args)
