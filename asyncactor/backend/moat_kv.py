#
# Listener on top of a MoaT-KV transport
#
from asyncactor.abc import Transport, MonitorStream
import msgpack


class MoatKVTransport(Transport):
    def __init__(self, conn, *topic):
        self.conn = conn
        self.topic = topic

    def monitor(self):
        return MoatKVMonitor(self)

    async def send(self, payload):
        payload = msgpack.packb(payload, use_bin_type=True)
        await self.conn.send(*self.topic, payload=payload)

    def __repr__(self):
        return "<Moat-KV:%s %r>" % (self.topic, self.conn)


class MoatKVMonitor(MonitorStream):
    _mon = None
    _it = None

    async def _ctx(self):
        async with self.transport.conn.monitor(*self.transport.topic) as self._mon:
            yield self

    def __aiter__(self):
        self._it = self._mon2.__aiter__()
        return self

    async def __anext__(self):
        msg = await self._it.__anext__()
        msg = msgpack.unpackb(msg.payload, raw=False, use_list=False)
        return msg

    def __repr__(self):
        return "<Mon:%r>" % (self.transport,)


Transport = MoatKVTransport
