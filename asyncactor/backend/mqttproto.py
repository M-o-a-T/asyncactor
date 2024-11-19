"""
Listener on top of an async mqttproto connection
"""

from __future__ import annotations

from asyncactor.abc import Transport, MonitorStream
from moat.util import create_queue, Path, CtxObj
from mqttproto import QoS
from moat.lib.codec import get_codec

from mqttproto.async_client import AsyncMQTTClient


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
        return "<MQTT:%s @%r>" % (self.tag, self.conn)


class MQTTMonitor(MonitorStream, CtxObj):
    async def _ctx(self):
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
        return "<Mon:%r>" % (self.transport,)


Transport = MQTTPTransport
