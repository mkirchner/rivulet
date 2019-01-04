"""

"""


class RivuletError(Exception):
    """
    Rivulet exception base class.
    """
    pass


class ConnectionError(RivuletError):
    """
    Raised for errors related to redis connection handling.
    """
    pass


class BackendError(RivuletError):
    """
    Signifies an error in the backend (redis).
    """
    pass


class TimeoutError(BackendError):
    """
    Raised if a timeout is reached.
    """
