"""Test the basic client functionality."""

import os
import pytest
import rivulet
import time


@pytest.fixture
def redis_url():
    url = os.getenv("REDIS_URL")
    assert url, 'REDIS_URL environment variable not defined.'
    return url


def test_connect_happy_path(redis_url):
    client = rivulet.connect(redis_url)
    client.ping()


def test_happy_path(redis_url):
    client = rivulet.connect(redis_url)
    channels = ['test-channel', 'test-channel2']
    client.subscribe(channels)
    _ = client.read()  # drop all pre-existing messages
    for i in range(5):
        client.write(channels[0], f'Hello World! {i}')
        client.write(channels[1], f'Hello World! {i}')
    time.sleep(0.1)
    msgs = client.read()
    assert len(msgs) == 2, "Wrong number of channels"
    assert all(
        channel in msgs.keys() for channel in channels), "Incorrect channels"
    assert len(msgs[channels[0]]) == 5, "Wrong number of messages"
    assert len(msgs[channels[1]]) == 5, "Wrong number of messages"

    client.unsubscribe(channels)
