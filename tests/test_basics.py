"""Test the basic client functionality."""

import os
import pytest
import rivulet


@pytest.fixture
def redis_url():
    url = os.getenv("REDIS_URL")
    assert url, 'REDIS_URL environment variable not defined.'
    return url


def test_connect_happy_path(redis_url):
    client = rivulet.connect(redis_url)
    client.ping()
