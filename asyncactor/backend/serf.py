
#
# Listener on top of an AsyncSerf connection
#
from asyncactor.abc import Transport, MonitorStream
from asyncserf import Serf

class SerfTransport(Transport):
    def __init__(self, conn: Serf, usertag: str):
        self.conn = conn
        self.tag = tag

    def monitor(self):
        return SerfMonitor(self)

    async def send(self, payload):
        await self.conn.event(name=self.tag, payload=payload, coalesce=False)

class SerfMonitor(MonitorStream):
    async def __aenter__(self):
        self._mon1 = self.conn.stream("user:"+self.tag)
        self._mon2 = await self._mon1.__aenter__()
        return self

    def __aexit__(self, *tb):
        return self._mon1.__aexit__(*tb)

    def __aiter__(self):
        self._it = self._mon2.__aiter__()
        return self

    def __anext__(self):
        return self._it.__anext__()

Transport = SerfTransport
