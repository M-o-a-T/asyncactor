#
# Listener on top of an AsyncSerf connection
#
from asyncactor.abc import Transport, MonitorStream
from asyncserf import Serf
import msgpack


class SerfTransport(Transport):
    def __init__(self, conn: Serf, *topic):
        self.conn = conn
        self.topic = topic
        self.tag = ".".join(topic)

    def monitor(self):
        return SerfMonitor(self)

    async def send(self, payload):
        payload = msgpack.packb(payload, use_bin_type=True)
        await self.conn.event(name=self.tag, payload=payload, coalesce=False)

    def __repr__(self):
        return "<Serf:%s @%r>" % (self.tag, self.conn)


class SerfMonitor(MonitorStream):
    _mon1 = None
    _mon2 = None
    _it = None

    async def __aenter__(self):
        self._mon1 = self.transport.conn.stream("user:" + self.transport.tag)
        self._mon2 = await self._mon1.__aenter__()
        return self

    async def __aexit__(self, *tb):
        return await self._mon1.__aexit__(*tb)

    def __aiter__(self):
        self._it = self._mon2.__aiter__()
        return self

    async def __anext__(self):
        msg = await self._it.__anext__()
        msg = msgpack.unpackb(msg.payload, raw=False, use_list=False)
        return msg

    def __repr__(self):
        return "<Mon:%r>" % (self.transport,)


Transport = SerfTransport
