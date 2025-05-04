"""
Listener on top of a MoaT-KV transport
"""

from __future__ import annotations

from asyncactor.abc import MonitorStream, Transport

from moat.util import CtxObj

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from typing import Self


class MoatKVTransport(Transport):
    "Transport"

    def __init__(self, conn, *topic):
        self.conn = conn
        self.topic = topic

    def monitor(self):
        "monitor"
        return MoatKVMonitor(self)

    async def send(self, payload):
        "send message"
        await self.conn.send(*self.topic, payload=payload)

    def __repr__(self):
        return f"<Moat-KV:{self.topic} {self.conn!r}>"


class MoatKVMonitor(MonitorStream, CtxObj):
    "Monitor"

    _mon = None
    _it = None

    async def _ctx(self) -> AsyncIterator[Self]:
        async with self.transport.conn.monitor(*self.transport.topic) as self._mon:
            yield self

    def __aiter__(self):
        self._it = self._mon.__aiter__()
        return self

    async def __anext__(self):
        msg = await self._it.__anext__()
        return msg.payload

    def __repr__(self):
        return f"<Mon:{self.transport}>"


Transport = MoatKVTransport
