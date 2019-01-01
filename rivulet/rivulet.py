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


class Client:
    """A Redis-based message streaming client."""

    def __init__(self,
                 redis_url: str,
                 client_id: str = None,
                 channel_ids: List[str] = None,
                 **redis_args) -> None:
        self.redis = redis.from_url(
            redis_url, encoding='utf-8', decode_responses=True)
        self.client_id = client_id if client_id else uuid.uuid4().hex
        if channel_ids:
            self.subscribe(channel_ids)

    @property
    def subscriptions(self):
        subs = self.redis.zrange(f'indexes:client#{self.client_id}', 0, -1)
        return subs

    def ping(self) -> None:
        try:
            self.redis.ping()
        except redis.exceptions.RedisError as e:
            raise ConnectionError from e

    def subscribe(self,
                  channel_ids: List[str],
                  index_policy: IndexPolicy = IndexPolicy.CURRENT) -> None:
        def _add_subscription(
                pipeline: Pipeline,
                client_id: str,
                channel_id: str,
                index_policy: IndexPolicy = IndexPolicy.CURRENT) -> Pipeline:
            # keep pre-existing index if requested
            nx = index_policy == IndexPolicy.CURRENT
            pipeline.zadd(
                f'clients:channel#{channel_id}', {self.client_id: 0}, nx=nx)
            # update the channels & indexes list for the client
            pipeline.zadd(
                f'indexes:client#{self.client_id}', {channel_id: 0}, nx=nx)
            return pipeline

        try:
            pipeline = self.redis.pipeline(transaction=True)
            for channel_id in channel_ids:
                pipeline = _add_subscription(pipeline, self.client_id,
                                             channel_id, index_policy)
            pipeline.execute()
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

    def read(self, timeout_ms: int = 0,
             message_limit: int = 512) -> Dict[str, List[str]]:
        current_indexes = self.redis.zrange(
            f'indexes:client#{self.client_id}', 0, -1, withscores=True)
        # the code below requires rework for balanced consumer locking
        pipeline = self.redis.pipeline(transaction=True)
        pipeline2 = self.redis.pipeline(transaction=True)

        for channel_id, current_index in current_indexes:
            pipeline.zrangebyscore(f'messages:channel#{channel_id}',
                                   current_index + 1,
                                   current_index + message_limit)
            pipeline2.zrange(
                f'clients:channel#{channel_id}', 0, -1, withscores=True)

        raw_messages = pipeline.execute()
        channel_indexes = pipeline2.execute()
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
            if min_index > current_index:
                pipeline.zremrangebyscore(f'messages:channel#{channel_id}', 0,
                                          min_index)
            message_lists.update({channel_id: messages})

        pipeline.execute()
        return message_lists


def connect(redis_url: str, client_id=None, channel_ids=None,
            **redis_args) -> Client:
    return Client(
        redis_url, client_id=client_id, channel_ids=channel_ids, **redis_args)
