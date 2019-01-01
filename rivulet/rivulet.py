"""Redis-Based Message Streaming for Python."""

import enum
import json
import time
import uuid

from typing import List, Dict

import redis
import redis.exceptions
from redis.client import Pipeline

from rivulet.exceptions import ConnectionError, BackendError


class IndexPolicy(enum.Enum):
    EARLIEST = 0
    CURRENT = 1
    LATEST = 2


class Client:
    """A Redis-based message streaming client."""

    def __init__(self,
                 redis_url: str,
                 client_id: str = None,
                 channel_ids: List[str] = None,
                 bufsize: int = 4096,
                 **redis_args) -> None:
        self.redis = redis.from_url(
            redis_url, encoding='utf-8', decode_responses=True, **redis_args)
        self.bufsize = bufsize
        self.client_id = client_id if client_id else uuid.uuid4().hex
        if channel_ids:
            self.subscribe(channel_ids)

    @property
    def subscriptions(self):
        try:
            subs = self.redis.zrange(f'indexes:client#{self.client_id}', 0, -1)
            return subs
        except redis.exceptions.RedisError as e:
            raise BackendError from e

    def ping(self) -> None:
        try:
            self.redis.ping()
        except redis.exceptions.RedisError as e:
            raise ConnectionError from e

    def subscribe(self,
                  channel_ids: List[str],
                  index_policy: IndexPolicy = IndexPolicy.CURRENT,
                  timeout_ms: int = 1000) -> None:

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
                raise BackendError from e
            except redis.exceptions.RedisError as e:
                raise BackendError from e

    def unsubscribe(self, channel_ids: List[str]) -> None:

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
        try:
            with self.redis.lock(
                    f'lock:ids:channel#{channel_id}',
                    timeout=timeout_ms / 1000.0):
                message_id = self.redis.incr(f'ids:channel#{channel_id}')
                ts = int(time.time() * 10**6)  # microseconds since epoch
                message = json.dumps({
                    'id': message_id,
                    'ts': ts,
                    'src': self.client_id,
                    'data': data
                })
                self.redis.zadd(f'messages:channel#{channel_id}',
                                {message: message_id})
        except redis.exceptions.LockError as e:
            raise BackendError from e
        except redis.exceptions.RedisError as e:
            raise BackendError from e

    def read(self, timeout_ms: int = 0,
             message_limit: int = 512) -> Dict[str, List[str]]:
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

        for i, ((channel_id, current_index), channel_indexes,
                raw_messages) in enumerate(inbox):
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
            channels, indexes = zip(*channel_indexes)
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
    return Client(
        redis_url, client_id=client_id, channel_ids=channel_ids, **redis_args)
