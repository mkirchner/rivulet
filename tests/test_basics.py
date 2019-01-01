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
    client.subscribe(['test-channel', 'test-channel2'])
    _ = client.read()  # drop all pre-existing messages
    for i in range(5):
        client.write('test-channel', f'Hello World! {i}')
        client.write('test-channel2', f'Hello World! {i}')
    time.sleep(0.1)
    msgs = client.read()
    print(msgs)
    client.unsubscribe(['test-channel', 'test-channel2'])
