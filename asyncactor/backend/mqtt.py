
#
# Listener on top of an MQTT connection
#
import anyio
from asyncactor.abc import Transport, MonitorStream
from distmqtt.client import MQTTClient
from distmqtt.mqtt.constants import QOS_0
import msgpack 


class MQTTTransport(Transport):
    """
    MQTT Transport for AsyncActor.

    MQTT does not have channels. Thus you need to call :meth:`deliver` 
    on every incoming message that matches the topic.
    """
    def __init__(self, conn: MQTTClient, *topic):
        self.conn = conn
        self.topic = '/'.join(topic)
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
        await self.conn.publish(self.topic, payload, QOS_0, False)

    def deliver(self, payload):
        """
        MQTT doesn't have dedicated channels.

        Call this to inject all payloads destined for this transport's topic.
        """
        payload = msgpack.unpackb(payload, raw=False, use_list=False)
        return self._monitor._q.put(payload)


class MQTTMonitor(MonitorStream):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self._q = anyio.create_queue(1)

    async def __aenter__(self):
        c = self.transport
        if c._monitor is not None:
            raise RuntimeError("You can't have more than one monitor")
        c._monitor = self
        await c.conn.subscribe([(c.topic, QOS_0)])
        return self

    async def __aexit__(self, *tb):
        c = self.transport
        c._monitor = None
        await c.conn.unsubscribe([c.topic])

    def __aiter__(self):
        return self

    def __anext__(self):
        return self._q.get()

Transport = MQTTTransport
