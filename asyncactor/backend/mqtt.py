#
# Listener on top of an MQTT connection
#
from asyncactor.abc import Transport, MonitorStream
from distmqtt.client import MQTTClient
from distmqtt.mqtt.constants import QOS_0
from distmqtt.utils import create_queue
import msgpack


class MQTTTransport(Transport):
    """
    MQTT Transport for AsyncActor.

    MQTT does not have channels. Thus you need to call :meth:`deliver`
    on every incoming message that matches the topic.
    """

    def __init__(self, conn: MQTTClient, *topic):
        self.conn = conn
        self.topic = topic
        self.tag = "/".join(topic)
        self._monitor = None

    def monitor(self):
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
        MQTT doesn't have dedicated channels.

        Call this to inject all payloads destined for this transport's topic.
        """
        payload = msgpack.unpackb(payload, raw=False, use_list=False)
        return self._monitor._q.put(payload)

    def __repr__(self):
        return "<MQTT:%s @%r>" % (self.tag, self.conn)


class MQTTMonitor(MonitorStream):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self._q = create_queue(1)

    async def __aenter__(self):
        c = self.transport
        if c._monitor is not None:
            raise RuntimeError("You can't have more than one monitor")
        c._monitor = self
        await c.conn.subscribe([(c.tag, QOS_0)])
        return self

    async def __aexit__(self, *tb):
        c = self.transport
        c._monitor = None
        await c.conn.unsubscribe([c.tag])

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self._q.get()

    def __repr__(self):
        return "<Mon:%r>" % (self.transport,)


Transport = MQTTTransport
