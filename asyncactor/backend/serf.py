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
    _mon = None
    _it = None

    async def _ctx(self):
        async with self.transport.conn.stream("user:" + self.transport.tag) as self._mon:
            yield self

    def __aiter__(self):
        self._it = self._mon.__aiter__()
        return self

    async def __anext__(self):
        msg = await self._it.__anext__()
        msg = msgpack.unpackb(msg.payload, raw=False, use_list=False)
        return msg

    def __repr__(self):
        return "<Mon:%r>" % (self.transport,)


Transport = SerfTransport
