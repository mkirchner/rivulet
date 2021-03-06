"""Test the basic client functionality."""

import os
import time
import uuid

import pytest

import rivulet

# pylint: disable=redefined-outer-name


@pytest.fixture
def redis_url():
    """Provides a redis URL as a fixture."""
    url = os.getenv("REDIS_URL")
    assert url, 'REDIS_URL environment variable not defined.'
    return url


def test_connect_happy_path(redis_url):
    """Check if we can connect to redis"""
    client = rivulet.connect(redis_url)
    assert client.ping(), "Could not ping the redis server."


def test_happy_path(redis_url):
    """Send 10 messages on 3 channels and check the results."""
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
    assert not client.subscriptions, "Number of subscriptions should be zero"


def test_message_limit(redis_url):
    """Check if recv honors the message limit."""
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


def test_index_policy_earliest(redis_url):
    """Test the EARLIEST index policy."""
    client = rivulet.connect(redis_url)
    channel = uuid.uuid4().hex
    client.subscribe([channel])
    while client.read():  # drop all existing messages
        pass
    # write to channel
    n_messages = 10
    for i in range(n_messages):
        client.write(channel, f'msg_{i}')
    # attach another client w/ policy EARLIEST
    client_2 = rivulet.connect(redis_url)
    client_2.subscribe([channel], index_policy=rivulet.IndexPolicy.EARLIEST)
    # read messages with both clients and make sure the numbers match
    inbox = client.read()
    assert len(
        inbox[channel]) == n_messages, "Wrong number of messages in inbox"
    inbox_2 = client_2.read()
    assert len(
        inbox_2[channel]) == n_messages, "Wrong number of messages in inbox"
    client_2.unsubscribe([channel])
    client.unsubscribe([channel])


def test_index_policy_latest_and_current_fallback_to_latest(redis_url):
    """Test the LATEST and CURRENT fallback index policy."""
    client = rivulet.connect(redis_url)
    channel = uuid.uuid4().hex
    client.subscribe([channel])
    while client.read():  # drop all existing messages
        pass
    # write to channel
    n_messages = 10
    for i in range(n_messages):
        client.write(channel, f'msg_{i}')
    # read the messages to advance the index
    inbox = client.read()
    assert len(
        inbox[channel]) == n_messages, "Wrong number of messages in inbox"
    # attach another client w/ policy LATEST
    client_2 = rivulet.connect(redis_url)
    client_2.subscribe([channel], index_policy=rivulet.IndexPolicy.LATEST)
    # attach another client w/ policy CURRENT which will fall back to LATEST
    client_3 = rivulet.connect(redis_url)
    client_3.subscribe([channel], index_policy=rivulet.IndexPolicy.CURRENT)
    # write more messages
    for i in range(n_messages):
        client.write(channel, f'msg_{i}')
    # read messages with both clients and make sure the numbers match
    inbox = client.read()
    assert len(
        inbox[channel]) == n_messages, "Wrong number of messages in inbox"
    inbox_2 = client_2.read()
    assert len(
        inbox_2[channel]) == n_messages, "Wrong number of messages in inbox"
    inbox_3 = client_3.read()
    assert len(
        inbox_3[channel]) == n_messages, "Wrong number of messages in inbox"

    client_3.unsubscribe([channel])
    client_2.unsubscribe([channel])
    client.unsubscribe([channel])


# def _write_n_messages(client, channel, n):
#     for i in range(n):
#         client.write(channel, f'msg_{i}')
#
#
# def test_single_threaded_write_performance(benchmark, redis_url):
#     # setup
#     n_channels = 1
#     n_messages = 1000
#     client = rivulet.connect(redis_url)
#     channels = [uuid.uuid4().hex for _ in range(n_channels)]
#     client.subscribe(channels)
#
#     while client.read():  # drop all pre-existing messages
#         pass
#
#     benchmark(
#         _write_n_messages, client=client, channel=channels[0], n=n_messages)
#
#     client.unsubscribe(channels)
