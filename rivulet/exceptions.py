"""
Rivulet exceptions.
"""

# pylint: disable=redefined-builtin


class RivuletError(Exception):
    """
    Rivulet exception base class.
    """


class ConnectionError(RivuletError):
    """
    Raised for errors related to redis connection handling.
    """


class BackendError(RivuletError):
    """
    Signifies an error in the backend (redis).
    """


class TimeoutError(BackendError):
    """
    Raised if a timeout is reached.
    """
