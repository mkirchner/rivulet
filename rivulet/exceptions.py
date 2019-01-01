"""Rivulet exceptions. """


class RivuletError(Exception):
    pass


class ConnectionError(RivuletError):
    pass


class BackendError(RivuletError):
    pass
