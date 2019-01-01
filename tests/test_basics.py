"""Test the basic client functionality."""

import os
import pytest
import rivulet
import time
import uuid


@pytest.fixture
def redis_url():
    url = os.getenv("REDIS_URL")
    assert url, 'REDIS_URL environment variable not defined.'
    return url


def test_connect_happy_path(redis_url):
    client = rivulet.connect(redis_url)
    client.ping()


def test_happy_path(redis_url):
    n_channels = 3
    n_messages = 10
    client = rivulet.connect(redis_url)
    channels = [uuid.uuid4().hex for _ in range(n_channels)]
    client.subscribe(channels)
    assert len(client.subscriptions) == len(
        channels), "Number of subscriptions should equal number of channels"
    _ = client.read()  # drop all pre-existing messages
    for i in range(n_messages):
        for channel_no, channel in enumerate(channels):
            client.write(channel, f'{channel_no}_{i}')
    time.sleep(0.1)  # allow for a little latency
    msgs = client.read()
    assert len(msgs) == len(
        channels
    ), "Number of message keys should correspond to number of channels"
    assert all(
        channel in msgs.keys() for channel in channels), "Incorrect channels"
    for channel in channels:
        assert len(msgs[channel]) == n_messages, "Wrong number of messages"
    client.unsubscribe(channels)
    assert len(
        client.subscriptions) == 0, "Number of subscriptions should be zero"


def test_message_limit(redis_url):
    n_channels = 3
    n_messages = 10
    client = rivulet.connect(redis_url)
    channels = [uuid.uuid4().hex for _ in range(n_channels)]
    client.subscribe(channels)
    _ = client.read()  # drop all pre-existing messages
    for i in range(n_messages):
        for channel_no, channel in enumerate(channels):
            client.write(channel, f'{channel_no}_{i}')

    time.sleep(0.1)  # allow for a little latency

    message_limit = 5
    for i in range(2):
        msgs = client.read(message_limit=message_limit)
        for channel in channels:
            assert len(
                msgs[channel]) == message_limit, "Wrong number of messages"
    client.unsubscribe(channels)
