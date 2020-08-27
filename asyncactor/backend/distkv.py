#
# Listener on top of a DistKV transport
#
from asyncactor.abc import Transport, MonitorStream
from asyncserf import Serf
import msgpack


class DistKVTransport(Transport):
    def __init__(self, conn: Serf, *topic):
        self.conn = conn
        self.topic = topic

    def monitor(self):
        return DistKVMonitor(self)

    async def send(self, payload):
        payload = msgpack.packb(payload, use_bin_type=True)
        await self.conn.send(*self.topic, payload=payload)

    def __repr__(self):
        return "<DistKV:%s %r>" % (self.topic, self.conn)


class DistKVMonitor(MonitorStream):
    _mon1 = None
    _mon2 = None
    _it = None

    async def __aenter__(self):
        self._mon1 = self.transport.conn.monitor(*self.transport.topic)
        if hasattr(self._mon1, "__aenter__"):
            self._mon2 = await self._mon1.__aenter__()
        else:
            self._mon2 = self._mon1
        return self

    async def __aexit__(self, *tb):
        if hasattr(self._mon1, "__aexit__"):
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


Transport = DistKVTransport
