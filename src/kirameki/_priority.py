import queue
import threading
import time
from collections import namedtuple
from concurrent import futures

import psycopg2
from psycopg2.extensions import TRANSACTION_STATUS_IDLE

from kirameki import exc
from kirameki._base import BasePool


class PriorityPool(BasePool):
    _entry = namedtuple("_entry", "created_on conn")

    def __init__(self, *args, stale_timeout=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.stale_timeout = stale_timeout

    # begin pool interface

    def closed(self):
        return self._closed

    def size(self):
        # XXX(auri): some invocations of this method might not be
        # fully thread-safe due to _in_pending; not sure if a
        # situation like that could ever occur in a real world
        # scenario
        return self._queue.qsize() + self._in_pending + len(self._in_use)

    def _get_connection(self, timeout=None):
        if self.size() < self.maxconn:
            self._connect()

        try:
            entry = self._queue.get(timeout=timeout)
        except queue.Empty:
            raise exc.PoolTimeout() from None
        if entry is None:
            # NOTE(auri): the chain is started by #_unsafe_close()
            # after draining the queue of real connections; we must
            # pass it on to awake other threads waiting for a connection
            self._queue.put_nowait(None)
            raise exc.PoolClosed()
        self._log.debug("acquired %r", entry)
        self._in_use[id(entry.conn)] = entry
        return entry.conn

    def _return_connection(self, conn, discard=False):
        try:
            entry = self._in_use.pop(id(conn))
        except KeyError:
            raise exc.PoolError(
                "attempting to insert a foreign connection"
            ) from None

        if conn.closed:
            self._log.warning("returned closed connection")
            self._ensure_minconn()
            return
        txn_status = conn.info.transaction_status
        if txn_status != TRANSACTION_STATUS_IDLE:
            self._log.warning("discarding unclean connection %r", conn)
            self._ensure_minconn()
            try:
                conn.rollback()
            finally:
                conn.close()
            return
        if discard or (
            self.stale_timeout is not None
            and (time.monotonic() - entry.created_on) >= self.stale_timeout
        ):
            self._log.debug("discarding stale (or on request) connection %r", conn)
            self._ensure_minconn()
            conn.close()
            return

        try:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute("DISCARD ALL")
            conn.set_session(
                isolation_level="default",
                readonly="default",
                deferrable="default",
                autocommit="default",
            )
        except:  # noqa:E722
            self._ensure_minconn()
            raise

        self._queue.put_nowait(entry)

    def _reset(self):
        self._closed = False
        self._queue = queue.PriorityQueue(self.maxconn)
        # NOTE(auri): do not change this dict implementation to
        # anything other than the builtin dict; the atomicity of
        # its __getitem__ is relied upon in this implementation
        self._in_use = {}
        self._connect_pool = futures.ThreadPoolExecutor(1)
        self._pending_lock = threading.Lock()
        self._in_pending = 0

    def _close(self):
        self._closed = True
        errors = []

        def _close(entry):
            self._log.debug("discarding %r on close", entry)
            try:
                entry.conn.close()
            except Exception as e:
                errors.append(e)

        while True:
            try:
                _, entry = self._in_use.popitem()
            except KeyError:
                break
            _close(entry)

        self._connect_pool.shutdown()

        while True:
            try:
                entry = self._queue.get_nowait()
            except queue.Empty:
                break
            _close(entry)

        # NOTE(auri): awakens the first thread waiting for a
        # connection; the thread then returns the None to the
        # queue thus chaining the reaction
        self._queue.put_nowait(None)

        if errors:
            raise exc.PoolError(errors)

    # end pool interface

    def _ensure_minconn(self):
        if self.size() < self.minconn:
            self._connect()

    def _connect(self):
        self._inc_pending()
        fut = self._connect_pool.submit(self._unsafe_connect)
        fut.add_done_callback(lambda _: self._dec_pending())

    def _inc_pending(self):
        # NOTE(auri): the lock is necessary because the increment
        # operation, although appearing atomic, isn't, and losing
        # even one pending connection would yield a catastrophic
        # outcome
        with self._pending_lock:
            self._in_pending += 1

    def _dec_pending(self):
        with self._pending_lock:
            self._in_pending -= 1
            if self._in_pending < 0:
                self._log.critical("_in_pending = %r", self._in_pending)

    def _unsafe_connect(self):
        try:
            self._log.debug("connecting")
            conn = self.connection_factory()
        except psycopg2.Error:
            self._log.error("failed to connect", exc_info=True)
            if not max(0, self.size() - 1):
                self._log.critical("stalled")

                def _stalled_close():
                    if self.closed():
                        return
                    try:
                        self.close()
                    except Exception:
                        self._log.critical(
                            "failed to close stalled pool", exc_info=True
                        )

                threading.Thread(target=_stalled_close).start()
        else:
            if self._closed:
                self._log.debug("pool closed while connecting")
                conn.close()
                return
            self._log.debug("connected")
            created_on = time.monotonic()
            entry = self._entry(created_on, conn)
            self._queue.put_nowait(entry)


__all__ = ["PriorityPool"]
