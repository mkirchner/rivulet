"""Redis-Based Message Streaming for Python."""

import uuid

from typing import List, Dict

import redis
import redis.exceptions

from rivulet.exceptions import ConnectionError


class Client:
    """A Redis-based message streaming client."""

    def __init__(self,
                 redis_url: str,
                 client_id: str = None,
                 channel_ids: List[str] = None,
                 **redis_args) -> None:
        self.redis = redis.from_url(redis_url)
        self.client_id = client_id if client_id else uuid.uuid4().hex
        if channel_ids:
            self.subscribe(channel_ids)

    def ping(self) -> None:
        try:
            self.redis.ping()
        except redis.exceptions.RedisError as e:
            raise ConnectionError from e

    def subscribe(self, channel_ids: List[str]) -> None:
        pass

    def unsubscribe(self, channel_ids: List[str]) -> None:
        pass

    def write(self, msg: str) -> None:
        pass

    def read(self, timeout_ms: int = 0) -> Dict[str, List[str]]:
        pass


def connect(redis_url: str, client_id=None, channel_ids=None,
            **redis_args) -> Client:
    return Client(
        redis_url, client_id=client_id, channel_ids=channel_ids, **redis_args)


# def create_channel(conn, sender, recipients, message, channel_id=None):
#     channel_id = channel_id or str(conn.incr('ids:channel:'))
#
#     recipients.append(sender)
#     recipientsd = dict((r, 0) for r in recipients)
#     pipeline = conn.pipeline(True)
#     pipeline.zadd('channel:' + channel_id, **recipientsd)
#     for rec in recipients:
#         pipeline.zadd('index:' + rec, channel_id, 0)
#     pipeline.execute()
#
#     return send_message(conn, channel_id, sender, message)
