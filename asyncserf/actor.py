import anyio
import time
from collections import deque
from random import Random
import os
import logging

from .client import Serf

class NodeEvent:
    pass

class TagEvent(NodeEvent):
    """This event says that for the moment, you're "it"."""
    def __repr__(self):
        return "<Tag>"
    pass

class UntagEvent(NodeEvent):
    """Your tag cycle time has passed. You're no longer "it"."""
    def __repr__(self):
        return "<UnTag>"
    pass

class DetagEvent(UntagEvent):
    """A ping from another node has arrived while you're "it".
    Unfortunately, it is "better" than ours.

    Arguments:
      node (str): The node that superseded us.
    """
    def __init__(self, node):
        self.node = node
    def __repr__(self):
        return "<DeTag %r>" % (self.node,)

class RawPingEvent(NodeEvent):
    """A ping from another node shows up. Not yet filtered!

    Arguments:
      msg (dict): The ping message of the currently-active actor.
    """
    def __init__(self, msg):
        self.msg = msg
    def __repr__(self):
        return "<RawPing %r>" % (self.msg,)

class PingEvent(NodeEvent):
    """A ping from another node shows up: the node in ``self.msg['node']`` is "it".

    Arguments:
      msg (dict): The ping message of the currently-active actor.
    """
    def __init__(self, msg):
        self.msg = msg
    def __repr__(self):
        return "<Ping %r>" % (self.msg,)

class GoodNodeEvent(NodeEvent):
    """A known-good node has been seen. We might want to get data from it.

    Arguments:
      nodes (list(str)): Nodes known to have a non-``None`` value.
    """
    def __init__(self, nodes):
        self.nodes = nodes
    def __repr__(self):
        return "<Good %r>" % (self.nodes,)

class RecoverEvent(NodeEvent):
    """We need to recover from a network split.

    Arguments:
      prio: Our recovery priority. Zero is highest.
      replace: Flag whether the other side superseded ours.
      local_nodes: A list of recent actors on our side.
      remote_nodes: A list of recent actors on the other side.
    """
    def __init__(self, prio, replace, local_nodes, remote_nodes):
        self.prio = prio
        self.replace = replace
        self.local_nodes = local_nodes
        self.remote_nodes = remote_nodes
    def __repr__(self):
        return "<Recover %d %s %r>" % (self.prio,self.replace,self.remote_nodes)


class NodeList(list):
    """
    This is an augmented :class: `list`, used to store a list of unique
    node names, up to some maximum.

    This is a simplistic implementation. It should not be used for large
    lists.

    Arguments:
      maxlen (int): The max length of the list. Use zero for "indefinite".

    >>> n = NodeList(3)
    >>> n += "a"
    >>> n
    ['a']
    >>> n += "a"
    >>> n
    ['a']
    >>> n += "b"
    >>> n
    ['b', 'a']
    >>> n += "a"
    >>> n
    ['a', 'b']
    >>> n += "c"
    >>> n
    ['c', 'a', 'b']
    >>> n += "d"
    >>> n
    ['d', 'c', 'a']
    >>> n += "c"
    >>> n
    ['c', 'd']
    >>> 
    """
    def __init__(self, maxlen, data=()):
        super().__init__(data)
        self.maxlen=maxlen

    def __iadd__(self, name):
        """Move 'name' to the front (or add it).

        This shortens the list by one if you replace a node that's not
        at the end (if maxlen is >0). The effect is that nodes are removed
        from the end gradually. Thi sis useful when a network split results
        in stale nodes.
        """
        try:
            i = self.index(name)
        except ValueError:
            i = -1
        self = type(self)(self.maxlen, self)

        if i >= 0:
            self.pop(i)

        # We pop an additional item if
        # + the length is bounded
        # + there's something that can be removed
        # + we either
        # -- removed something (except from the end), or
        # -- the list is maxed out, i.e. we didn't remove anything
        if self.maxlen > 0 and len(self) > 0 and (0 <= i < len(self) or len(self) == self.maxlen):
            self.pop(-1)
        self.insert(0, name)
        return self

    __add__ = __iadd__


class Actor:
    """
    Some jobs need a single controller so that tasks don't step on each other's
    toes. Serf does not have a way to elect one.

    This class doesn't elect a controller either. It simply provides a
    timeout-based keepalive and round-robin scheme to periodically select a
    single host in a group.

    Actor messages are Serf broadcasts; they consist of:

    * my name
    * my current value (must be ``None`` if not ready)
    * a history of the names in previous pings (with my name filtered out).

    Arguments:
      client: The Serf client to use
      prefix (str): The Serf event name to use. Actors with the same prefix
        form a group and do not affect actors using a different prefix.
      name (str): This node's name. **Must** be unambiguous.
      cfg (dict): a dict containing additional configuration values.

    The config dict may contain these values.

    Arguments:
      cycle (float): The interval between messages, Default: 10. During this time,
        after receiving a TagEvent, that actor is "it".
      gap (float): Additional processing time to agree on the next tagged node.
        Default: 1.5
      nodes: The number of nodes that should be active participants. Set to
        -1 for "all of them". Default: 5.
      splits: The maximum number of independent zones which your network
        might conceivably partition itself to, plus 1. Default: 4, which
        works for any number of linearly connected networks. If you have a
        central router with a subnetwork on every of its 8 ports, set this
        parameter to 9.
      n_hosts: The rough number of nodes that might participate.
        Defaults to 10.

    Actor coordinates with its peers to broadcast one ping per cycle; the
    actual time between cycle starts may be up to ``2*gap`` longer.

    """

    _worker = None
    _reader = None
    _pinger = None
    _value = None

    DEFAULTS = dict(cycle=10, gap=1.5, nodes=5, splits=4, n_hosts=10)

    def __init__(self, client: Serf, prefix: str, name: str, cfg: dict = {}):
        self._client = client
        self._prefix = prefix
        self._name = name
        self.logger = logging.getLogger("asyncserf.actor."+self._name)

        self._cfg = {}
        self._cfg.update(self.DEFAULTS)
        self._cfg.update(cfg)

        self._cycle = self._cfg['cycle']
        self._gap = self._cfg['gap']
        self._nodes = self._cfg['nodes']
        self._splits = self._cfg['splits']
        self._n_hosts = self._cfg['n_hosts']

        self._evt_q = anyio.create_queue(1)
        self._rdr_q = anyio.create_queue(99)
        self._ping_q = anyio.create_queue(1)
        self._ready = False
        self._tagged = 0  # False: waiting, True: tagged
        self._valid_pings = 0

        self._values = {}  # map names to steps
        self._history = NodeList(self._nodes)  # those in the loop
        self._prev_history = None
        rs = os.environ.get("PYTHONHASHSEED",None)
        if rs is None:
            self._rand = Random()
        else:
            import trio._core._run as tcr
            self._rand = tcr._r

        self._next_ping_time = 0
        self._recover_pings = {}

    @property
    def random(self):
        """A random float between 0 and 1 (uniform)"""
        return self._rand.random()

    @property
    def history_pos(self, node):
        try:
            return self._history.index(node)
        except IndexError:
            return -1

    async def __aenter__(self):
        if self._worker is not None or self._reader is not None:
            raise RuntimeError("You can't enter me twice")
        evt = anyio.create_event()
        self._reader = await self._client.spawn(self._read, evt)
        await evt.wait()
        self._worker = await self._client.spawn(self._run)
        self._pinger = await self._client.spawn(self._ping)
        return self

    async def _ping(self):
        while True:
            msg = await self._ping_q.get()
            async with anyio.move_on_after(self._gap*2):
                while True:
                    msg = await self._ping_q.get()
            if not self._tagged:
                self._valid_pings += 1
                await self._evt_q.put(PingEvent(msg))

    async def __aexit__(self, *tb):
        async with anyio.open_cancel_scope(shield=True):
            w,self._worker = self._worker, None
            if w is not None:
                await w.cancel()
            w,self._reader = self._reader, None
            if w is not None:
                await w.cancel()
            w,self._pinger = self._pinger, None
            if w is not None:
                await w.cancel()

    def __aiter__(self):
        return self

    async def __anext__(self):
        evt = await self._evt_q.get()
        self.logger.debug("EVT %r",evt)
        return evt

    async def set_value(self, val):
        """Set the value that's included when we send a ping.

        It is used for comparison when pings collide.
        """
        self._value = val

    def set_ready(self):
        """
        Set a flag to indicate that the client is fully operational and
        should participate actively.

        You *must* call this.
        """
        self._ready = True

    async def _read(self, evt: anyio.abc.Event = None):
        async with self._client.serf_mon(self._prefix) as mon:
            await evt.set()
            async for msg in mon:
                await self._rdr_q.put(msg['data'])

    async def _run(self):
        await anyio.sleep((self.random / 2 + 1.5) * self._gap + self._cycle)
        await self._send_ping()

        while True:
            msg = None
            if self._tagged:
                if self._tagged == 1:
                    t = self._gap
                    self._tagged = 2
                elif self._tagged == 2:
                    t = self._cycle
                    await self._evt_q.put(TagEvent())
                    self._tagged = 3
                    self._valid_pings += 1
                elif self._tagged == 3:
                    await self._evt_q.put(UntagEvent())
                    self._tagged = 0
                else:
                    raise RuntimeError("tagged", self._tagged)
            if not self._tagged:
                t = max(self._next_ping_time - time.time(), 0)
            async with anyio.move_on_after(t):
                msg = await self._rdr_q.get()
            if msg is None:
                self.logger.debug("IN %d", t)
                if self._tagged == 0:
                    await self._send_ping()
                continue

            await self._evt_q.put(RawPingEvent(msg))
            await self.process_msg(msg)

    async def process_msg(self, msg):
        """Process this incoming message."""

        prev_node = self._history[0]
        this_val = msg["value"]
        if 'node' in msg:
            msg_node = msg['node']
        else:
            msg_node = msg["history"][0]
            ping = self._recover_pings.get(msg_node, None)
            if isinstance(ping,anyio.abc.Event):
                await ping.set()
            else:
                self._recover_pings[msg_node] = self._valid_pings


        if msg_node == self._name:
            # my own message, returned
            return
        self.logger.debug("IN %r", msg)

        self._values[msg_node] = this_val = msg["value"]

        if msg["history"] and msg["history"][0] == self._history[0]:
            if 'node' in msg:
                self._prev_history = self._history
                self._history += msg_node
                self._get_next_ping_time()
                await self._ping_q.put(msg)
            else:
                # This is a notification ping for our side, after a split.
                # Ignore it: we already initiated recovery when sending the
                # notification, see below.
                pass
            return

        # Colliding pings.
        same_prev = msg["history"] and (msg["history"][0:1] == self._history[1:2])
        # only check one host. This is intentional: the current host may
        # have been elided from the list which would cause a false conflict.

        prefer_new = self.has_priority(msg_node, prev_node)

        hist = NodeList(0, self._history)
        if prefer_new:
            self._history = NodeList(self._nodes, msg["history"])  # self._prev_history
            if 'node' in msg:
                self._history += msg['node']

            self.logger.debug("Coll Ack %s", msg)
            if self._tagged:
                if self._tagged == 3:
                    await self._evt_q.put(DetagEvent(msg_node))
                self._tagged = 0

            self._get_next_ping_time()
            await self._ping_q.put(msg)

        if same_prev:
            # These pings refer to the same previous ping. Good.
            if not prefer_new:
                self.logger.debug("Coll PRE %s", msg)
            return

        self.logger.debug("Coll %s %s", prefer_new, msg)

        # We either have a healed network split (bad) or are new (oh well).

        if self._value is not None:  # I am ready
            pos = -1
            try:
                pos = hist.index(self._name)
            except ValueError:
                pass
            else:
                h = NodeList(0, msg["history"])
                if "node" in msg:
                    h += msg['node']
                await self._evt_q.put(RecoverEvent(pos, prefer_new, hist, h))
            if pos > -1 and prefer_new:
                evt = anyio.create_event()
                await self._client.spawn(self._send_delay_ping, pos, evt, hist)
                await evt.wait()


        elif this_val is not None:
            # The other node has become ready
            await self._evt_q.put(GoodNodeEvent([msg["node"]]+list(h for h in msg["history"] if self._values[h] is not None)))


    def get_value(self, node):
        """
        Retrieve the value that a given node transmitted last.

        Arguments:
          mode: The node's name.

        Returns: the value, or ``None`` if not set.
        """
        return self._values.get(node, None)

    def has_priority(self, a: str, b: str):
        """
        Choose whether one "ping" message should be prioritized over another.

        This method must be deterministic, i.e. ``has_priority(a,b) ==
        not has_priority(b,a)`` must **always** be true.

        Arguments:
          a: The name of one node.
          b: The name of another node.

        Returns: ``True`` if node ``a`` is "better", ``False`` otherwise.

        Ping messages contain a value, which can be retrieved with
        :meth:`get_value`.

        The ping history is not available and cannot be used for
        prioritizing anyway: if you do, you can always construct a case
        where the above reciprocality condition fails, esp. after a
        network split is healed.

        Note that :meth:`get_value` may return ``None`` for either node.
        A value that's ``None`` **must** have lower priority than any
        other value.
        """
        a_val = self.get_value(a)
        b_val = self.get_value(b)
        if a_val is None:
            if b_val is not None:
                return False
            # otherwise both are None: fall through
        elif b_val is None:
            return True
        elif a_val != b_val:
            return a_val > b_val

        # Same values: compare nodes
        assert a != b
        return a < b

    async def _send_ping(self, history=None):
        if history is not None:
            msg = {"value": self._values[history[0]]}
        else:
            msg = {"node": self._name, "value": self._value}
            self._values[self._name] = self._value
            if self._history:
                self._tagged = 1
            history = self._prev_history = self._history
            self._history += self._name
            self._get_next_ping_time()
        msg["history"] = history[0:self._splits]
        self.logger.debug("SEND %r",msg)
        await self._client.serf_send(self._prefix, msg)

    async def _send_delay_ping(self, pos, evt, history):
        node = history[0]
        ping = self._recover_pings.get(node, None)
        if isinstance(ping, anyio.abc.Event) or isinstance(ping,int) and ping >= self._valid_pings - self._nodes/2:
            if isinstance(ping, int):
                del self._recover_pings[node]
            await evt.set()
            return
        self._recover_pings[node] = e = anyio.create_event()
        await evt.set()

        async with anyio.move_on_after(self._gap * (1 - 1 / (1 << pos))) as x:
            await e.wait()
        if self._recover_pings.get(node, None) is e:
            del self._recover_pings[node]
        else:
            return
        if x.cancel_called:
            if pos:
                self.logger.info("PingDelay: no signal %d", pos)
            await self._send_ping(history=history)

    def _get_next_ping_time(self):
        t = self._time_to_next_ping()
        self.logger.debug("TN %.3f",t)
        self._next_ping_time = time.time() + self._cycle + self._gap * t

    def _time_to_next_ping(self):
        """Calculates the time until sending the next ping is a good idea,
        assuming that none arrive in the meantime, in cycles."""
        if not self._history:
            # we might be the only node
            return 1.9-self.random/5

        if self._history[0] == self._name:
            # we sent the last ping.
            return 2

        # check whether the first half of the ping chain contains nonzero ticks
        # so that if we're not fully up yet, the chain doesn't only consist of
        # nodes that don't work.
        p = s = 0
        lv = 1
        for h in self._history:
            if self._values.get(h, None) is not None and p == 0:
                p = lv
            if h == self._name:
                s = lv
            lv += 1
            if lv > self._nodes:
                break
        if not self._ready:
            if p > lv // 2:
                # No, the first active host is too far back.
                return 2+self.random/3

        return self.ping_delay(s-1,lv, len(self._history) < self._nodes, max(len(self._values), self._n_hosts))

    def ping_delay(self, pos, length, todo, total):
        """
        Calculates the time until sending the next ping.

        This function must return a float between 0 and 2. The value should
        be spread out so that the most likely value is substantially lower
        than the next-most-likely one, to avoid collisions. I.e. if you
        have five nodes, it's much better to return something like ``1,
        1.5, 1.75, 1.875, 1.9375`` than using ``1, 1.2, 1.4, 1.8, 2``.

        Arguments:
          pos: The position of this node in the ping history.
            Zero: at the front. Negative: not in the list.
          length: The total length of the list.
          todo: True if the number of

        The default implementation uses ``0.5, 0.75, 0.875, …`` for nodes
        on the list, prioritizing the last node; some value between 0 and
        1/3 for (rare) random inserts; and some random value between 1.5
        and 2 as fall-back.

        Those random inserts happen more frequently when there are not
        enough nodes
        """

        if pos >= 0:
            # We are on the chain. Send ping depending on our position.
            return 1 - 1/(1<<(length-pos))/2
            # this will never be 1 because we need to leave some time for
            # interlopers, below. Otherwise we could divide by l-1, as
            # l must be at least 2. s must also be at least 1.

        if todo:
            # the chain is too short. Try harder to get onto it.
            f = 3
        else:
            f = 10
        if self.random < 1 / f / total:
            # send early (try getting onto the chain)
            return self.random / 3
        else:
            # send late (fallback)
            return 1.5 + self.random / 2

