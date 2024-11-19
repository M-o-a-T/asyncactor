"""
Listener on top of an MQTT connection.

DEPRECATED. Use "mqttproto" instead.
"""

from __future__ import annotations

from asyncactor.abc import MonitorStream, Transport

import msgpack

from moat.util import CtxObj, create_queue
from moat.mqtt.client import MQTTClient
from moat.mqtt.mqtt.constants import QOS_0


class MQTTTransport(Transport):
    """
    MQTT Transport for AsyncActor.

    MQTT does not have channels. Thus you need to call :meth:`deliver`
    on every incoming message that matches the topic.
    (The actor does subscribe itself.)

    DEPRECATED. Use "mqttproto" instead.
    """

    def __init__(self, conn: MQTTClient, *topic):
        self.conn = conn
        self.topic = topic
        self.tag = "/".join(topic)
        self._monitor = None

    def monitor(self) -> Awaitable[AsyncContextManager[MonitorStream]]:
        """
        Accept incoming messages::

            async with conn.monitor() as m:
                async for msg in m:
                    await process(msg)
        """
        return MQTTMonitor(self)

    async def send(self, payload):
        """
        Send a message.
        """
        payload = msgpack.packb(payload, use_bin_type=True)
        await self.conn.publish(self.tag, payload, QOS_0, False)

    def deliver(self, payload):
        """
        MQTT didn't have dedicated channels.

        Call this to inject all payloads destined for this transport's topic.
        """
        payload = msgpack.unpackb(payload, raw=False, use_list=False)
        return self._monitor._q.put(payload)

    def __repr__(self):
        return "<MQTT:%s @%r>" % (self.tag, self.conn)


class MQTTMonitor(MonitorStream, CtxObj):
    _mon = None

    def __init__(self, *a, **k):
        super().__init__(*a, **k)

    async def _ctx(self):
        self._q = create_queue(1)
        c = self.transport
        await c.conn.subscribe([(c.tag, QOS_0)])
        try:
            c._monitor = self
            yield self
        finally:
            await c.conn.unsubscribe([c.tag])
            c._monitor = None
        del self._q

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self._q.get()

    def __repr__(self):
        return "<Mon:%r>" % (self.transport,)


Transport = MQTTTransport
