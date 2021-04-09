import logging
import os
import threading
from abc import ABCMeta, abstractmethod

from kirameki.pool import exc


class BasePool(metaclass=ABCMeta):
    def __init__(self, minconn, maxconn, connection_factory):
        self.minconn = minconn
        self.maxconn = maxconn
        self.connection_factory = connection_factory

        self._log = logging.getLogger(type(self).__qualname__)

        self.__reset()

    def get_connection(self, timeout=None, **kwargs):
        self._safe_call()
        self._log.debug(
            "acquiring connection (timeout=%r, kwargs=%r)", timeout, kwargs
        )
        return self._get_connection(timeout=timeout, **kwargs)

    def return_connection(self, conn, discard=False, **kwargs):
        self._safe_call()
        self._log.debug(
            "returning %r (discard=%r, kwargs=%r)", conn, discard, kwargs
        )
        return self._return_connection(conn, discard=discard, **kwargs)

    def close(self, **kwargs):
        self._safe_call()
        self._log.debug("closing (kwargs=%r)", kwargs)
        return self._close(**kwargs)

    def _safe_call(self):
        if self.closed():
            raise exc.PoolClosed()

        if self._pid != os.getpid():
            if not self._fork_lock.acquire(timeout=3):
                raise exc.PoolDeadlocked()
            try:
                if self._pid != os.getpid():
                    self._reset()
            finally:
                self._fork_lock.release()

    def __reset(self):
        self._log.debug("resets")
        self._reset()
        self._pid = os.getpid()
        self._fork_lock = threading.Lock()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if not self.closed():
            self.close()

    @abstractmethod
    def closed(self):
        pass

    @abstractmethod
    def size(self):
        pass

    @abstractmethod
    def _get_connection(self, **kwargs):
        pass

    @abstractmethod
    def _return_connection(self, conn, **kwargs):
        pass

    @abstractmethod
    def _reset(self):
        pass

    @abstractmethod
    def _close(self, **kwargs):
        pass
