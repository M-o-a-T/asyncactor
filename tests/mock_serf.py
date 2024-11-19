from __future__ import annotations

try:
    from contextlib import AsyncExitStack, asynccontextmanager
except ImportError:
    from async_exit_stack import AsyncExitStack
    from async_generator import asynccontextmanager
import anyio
import logging
import time
from unittest import mock

from asyncactor.abc import MonitorStream, Transport

import attr
import trio

from moat.util import create_queue

logger = logging.getLogger(__name__)

otm = time.time


@asynccontextmanager
async def stdtest(**kw):  # pylint: disable=W0613
    clock = trio.lowlevel.current_clock()
    clock.autojump_threshold = 0.01

    @attr.s
    class S:
        tg = attr.ib()
        serfs = attr.ib(factory=set)
        splits = attr.ib(factory=set)
        s = []  # servers
        c = []  # clients

        async def ready(self, i=None):
            if i is not None:
                await self.s[i].is_ready
                return self.s[i]
            for s in self.s:
                if s is not None:
                    await s.is_ready
            return self.s

        def __iter__(self):
            return iter(self.s)

        @asynccontextmanager
        async def client(self, i: int = 0, **kv):  # pylint: disable=W0613
            """Get a client for the i'th server."""
            t = MockTransport(tg, self, i)
            self.serfs.add(t)
            try:
                logger.debug("C START %d", i)
                yield t
            finally:
                logger.debug("C STOP %d", i)
                self.serfs.remove(t)

        def split(self, s):
            assert s not in self.splits
            logger.debug("Split: add %d", s)
            self.splits.add(s)

        def join(self, s):
            logger.debug("Split: join %d", s)
            self.splits.remove(s)

    def tm():
        try:
            return trio.current_time()
        except RuntimeError:
            return otm()

    async with anyio.create_task_group() as tg:
        st = S(tg)
        async with AsyncExitStack() as ex:
            ex.enter_context(mock.patch("time.time", new=tm))
            ex.enter_context(mock.patch("time.monotonic", new=tm))
            logging._startTime = tm()

            try:
                yield st
            finally:
                with anyio.fail_after(2, shield=True):
                    logger.info("Runtime: %s", clock.current_time())
                    tg.cancel_scope.cancel()
        logger.info("End")
        # unwinding ex:AsyncExitStack


class MockTransport(Transport):
    def __init__(self, tg, master, i=0, **cfg):
        self.cfg = cfg
        self.tg = tg
        self.streams = set()
        self._master = master
        self.i = i

    def __hash__(self):
        return id(self)

    @property
    def topic(self):
        return "mock_%d" % (self.i,)

    def monitor(self):
        return MockSerfStream(self)

    async def send(self, payload):
        # logger.debug("SERF>%s> %r", typ, payload)

        for s in list(self._master.serfs):
            for x in self._master.splits:
                if (s.i < x) != (self.i < x):
                    break
            else:
                for st in list(s.streams):
                    await st.q.put(payload)


class MockSerfStream(MonitorStream):
    q = None

    async def __aenter__(self):
        logger.debug("SERF:MON START")
        assert self.q is None
        self.q = create_queue(100)
        self.transport.streams.add(self)
        return self

    async def __aexit__(self, *tb):
        self.transport.streams.remove(self)
        logger.debug("SERF:MON END")
        self.q = None

    def __aiter__(self):
        self.q = create_queue(100)
        return self

    async def __anext__(self):
        return await self.q.get()
        # logger.debug("SERF<%s< %r", self.typ, res)
