"""
Listener on top of an async mqttproto connection
"""

from __future__ import annotations

from asyncactor.abc import MonitorStream, Transport

from mqttproto import QoS

from moat.util import CtxObj, Path
from moat.lib.codec import get_codec

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable

    from mqttproto.async_client import AsyncMQTTClient

    from typing import Self


class MQTTPTransport(Transport):
    """
    MQTTProto Transport for AsyncActor.
    """

    def __init__(self, conn: AsyncMQTTClient, topic: Path, codec="cbor"):
        self.conn = conn
        self.topic = topic
        self.tag = "/".join(topic)
        self.codec = get_codec(codec)

    def monitor(self):
        """
        Accept incoming messages::

            async with conn.monitor() as m:
                async for msg in m:
                    await process(msg)
        """
        return MQTTMonitor(self)

    def send(self, payload) -> Awaitable[None]:
        """
        Send a message.
        """
        return self.conn.publish(self.tag, self.codec.encode(payload), qos=QoS.AT_LEAST_ONCE)

    def __repr__(self):
        return f"<MQTT:[self.tag] @{self.conn!r}>"


class MQTTMonitor(MonitorStream, CtxObj):
    "Monitor"

    async def _ctx(self) -> AsyncIterator[Self]:
        c = self.transport
        async with c.conn.subscribe(c.tag, maximum_qos=QoS.AT_LEAST_ONCE) as mon:
            self._it = aiter(mon)
            try:
                yield self
            finally:
                self._it = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        msg = self.transport.codec.decode((await anext(self._it)).payload)
        return msg

    def __repr__(self):
        return f"<Mon:{self.transport}>"


Transport = MQTTPTransport
