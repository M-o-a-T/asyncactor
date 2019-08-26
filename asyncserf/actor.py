import anyio
import time
from random import Random
import os
import logging
import msgpack
from functools import partial

from .client import Serf
from .exceptions import SerfTimeoutError, SerfCollisionError
from asyncserf.util import ValueEvent


class NodeEvent:
    pass


class AuthPingEvent(NodeEvent):
    """
    Superclass for tag and ping: must arrive within :meth:`cycle_time_max` seconds of each other.

    Non-abstract subclasses of this must have ``name`` and ``value`` attributes.
    (TODO: enforce this)
    """

    pass


class TagEvent(AuthPingEvent):
    """
    This event says that for the moment, you're "it".
    """

    def __init__(self, name, value):
        self.node = name
        self.value = value

    def __repr__(self):
        return "<Tag>"


class UntagEvent(NodeEvent):
    """
    Your tag cycle time has passed. You're no longer "it".
    """

    def __repr__(self):
        return "<UnTag>"


class DetagEvent(UntagEvent):
    """
    A ping from another node has arrived while you're "it".
    Unfortunately, it is "better" than ours.

    Arguments:
      node (str): The node that superseded us.
    """

    def __init__(self, node):
        self.node = node

    def __repr__(self):
        return "<DeTag %r>" % (self.node,)


class RawPingEvent(NodeEvent):
    """
    A ping from another node shows up. Not yet filtered!

    Arguments:
      msg (dict): The ping message of the currently-active actor.
    """

    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
        return "<RawPing %r>" % (self.msg,)


class PingEvent(AuthPingEvent):
    """
    A ping from another node shows up: the node ``.node`` is "it".

    Arguments:
      msg (dict): The ping message of the currently-active actor.
    """

    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
        return "<Ping %r>" % (self.msg,)

    @property
    def node(self):
        """
        Name of the node. Shortcut to ``msg['node']``.
        """
        return self.msg["node"]

    @property
    def value(self):
        """
        Name of the node. Shortcut to ``msg['node']``.
        """
        try:
            return self.msg["value"]
        except KeyError:
            return None


class GoodNodeEvent(NodeEvent):
    """
    A known-good node has been seen. We might want to get data from it.

    Arguments:
      nodes (list(str)): Nodes known to have a non-``None`` value.

    This event is seen while starting up, when our value is ``None``.
    """

    def __init__(self, nodes):
        self.nodes = nodes

    def __repr__(self):
        return "<Good %r>" % (self.nodes,)


class RecoverEvent(NodeEvent):
    """
    We need to recover from a network split.

    Arguments:
      prio: Our recovery priority. Zero is highest.
      replace: Flag whether the other side has superseded ours.
      local_nodes: A list of recent actors on our side.
      remote_nodes: A list of recent actors on the other side.
    """

    def __init__(self, prio, replace, local_nodes, remote_nodes):
        self.prio = prio
        self.replace = replace
        self.local_nodes = local_nodes
        self.remote_nodes = remote_nodes

    def __repr__(self):
        return "<Recover %d %s %r %r>" % (
            self.prio,
            self.replace,
            self.local_nodes,
            self.remote_nodes,
        )


class NodeList(list):
    """
    This is an augmented :class: `list`, used to store unique node names,
    up to some maximum (if used).

    This is a simplistic implementation. It should not be used for large
    lists.

    Arguments:
      maxlen (int): The max length of the list. Use zero for "indefinite".
      mutable (bool): A flag whether "nodelist += foo" should modify "foo"
        in-place. If not (the default), a new list will be allocated.

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

    def __init__(self, maxlen, data=(), mutable=False):
        super().__init__(data)
        self.maxlen = maxlen
        self._mutable = mutable

    def __iadd__(self, name, mutable=None):
        """Move 'name' to the front (or add it).

        This shortens the list by one if you replace a node that's not
        at the end (if maxlen is >0). The effect is that nodes are removed
        from the end gradually. Thi sis useful when a network split results
        in stale nodes.
        """
        if mutable is None:
            mutable = self._mutable

        try:
            i = self.index(name)
        except ValueError:
            i = -1
        if mutable:
            self = type(self)(self.maxlen, self, mutable=self._mutable)

        if i >= 0:
            self.pop(i)

        # We pop an additional item if
        # + the length is bounded
        # + there's something that can be removed
        # + we either
        # -- removed something (except from the end), or
        # -- the list is maxed out, i.e. we didn't remove anything
        if (
            self.maxlen > 0
            and len(self) > 0
            and (0 <= i < len(self) or len(self) == self.maxlen)
        ):
            self.pop(-1)
        self.insert(0, name)
        return self

    def __add__(self, name):
        return self.__iadd__(name, mutable=False)


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
      tg: a taskgroup. May be ``None`` in which case the client's is used.
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

    def __init__(
        self,
        client: Serf,
        prefix: str,
        name: str,
        tg: anyio.abc.TaskGroup = None,
        cfg: dict = {},
        enabled: bool = True,
        packer=None,
        unpacker=None,
    ):
        self._client = client
        if tg is None:
            tg = client.tg
        self._prefix = prefix
        self._name = name
        self._tg = tg
        self.logger = logging.getLogger(
            "asyncserf.actor.%s.%s" % (self._prefix, self._name)
        )

        if packer is None:
            packer = msgpack.Packer(strict_types=False, use_bin_type=True).pack
        if unpacker is None:
            unpacker = partial(msgpack.unpackb, raw=False, use_list=False)
        self._packer = packer
        self._unpacker = unpacker
        self._cfg = {}
        self._cfg.update(self.DEFAULTS)
        self._cfg.update(cfg)

        self._cycle = self._cfg["cycle"]
        self._gap = self._cfg["gap"]
        self._nodes = self._cfg["nodes"]
        self._splits = self._cfg["splits"]
        self._n_hosts = self._cfg["n_hosts"]
        self._self_seen = False

        if self._cycle < 2:
            raise ValueError("cycle must be >= 2")
        if self._gap < 1:
            raise ValueError("gap must be >= 1")
        if self._cycle < self._gap:
            raise ValueError("cycle must be >= gap")

        self._evt_q = anyio.create_queue(1)
        self._rdr_q = anyio.create_queue(99)
        self._ping_q = anyio.create_queue(1)
        self._ready = False
        self._tagged = 0 if enabled else -1  # >0: "our" tag is progressing
        self._valid_pings = 0

        self._values = {}  # map names to steps
        self._history = NodeList(self._nodes)  # those in the loop
        self._prev_history = None
        rs = os.environ.get("PYTHONHASHSEED", None)
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
    def name(self):
        return self._name

    @property
    def cycle_time(self):
        return self._cycle

    @property
    def cycle_time_max(self):
        """
        Max time between two ``AuthPingEvent`` messages.
        """
        return self._cycle + 2.5 * self._gap

    @property
    def history(self):
        """
        A copy of the current history.
        """
        return self._history.copy()

    @property
    def history_size(self):
        """
        The length of the current history.
        """
        return len(self._history)

    @property
    def history_maxsize(self):
        """
        The length of the current history.
        """
        return self._nodes

    @property
    def history_pos(self, node):
        """
        Return our position in the current history.

        If we're disabled, always return -1.
        """
        if self._tagged < 0:
            return -1
        try:
            return self._history.index(node)
        except IndexError:
            return -1

    async def spawn(self, proc, *args, **kw):
        """
        Run a task within this object's task group.

        Returns:
          a cancel scope you can use to stop the task.
        """

        async def _run(proc, args, kw, res):
            """
            Helper for starting a task.

            This accepts a :class:`ValueEvent`, to pass the task's cancel scope
            back to the caller.
            """
            async with anyio.open_cancel_scope() as scope:
                await res.set(scope)
                await proc(*args, **kw)

        res = ValueEvent()
        await self._tg.spawn(_run, proc, args, kw, res)
        return await res.get()

    async def _ping(self):
        """
        This subtask generates :cls:`PingEvent` messages. It ensures that
        there's exactly one PingEvent per cycle, if possible.
        """
        while True:
            msg = await self._ping_q.get()
            async with anyio.move_on_after(self._gap * 2):
                while True:
                    msg = await self._ping_q.get()
            if self._tagged <= 0:
                self._valid_pings += 1
                await self.post_event(PingEvent(msg))

    async def __aenter__(self):
        if self._worker is not None or self._reader is not None:
            raise RuntimeError("You can't enter me twice")
        evt = anyio.create_event()
        self._reader = await self.spawn(self.read_task, self._prefix, evt)
        await evt.wait()
        self._worker = await self.spawn(self._run)
        self._pinger = await self.spawn(self._ping)
        return self

    async def __aexit__(self, *tb):
        async with anyio.open_cancel_scope(shield=True):
            w, self._worker = self._worker, None
            if w is not None:
                await w.cancel()
            w, self._reader = self._reader, None
            if w is not None:
                await w.cancel()
            w, self._pinger = self._pinger, None
            if w is not None:
                await w.cancel()

    def __aiter__(self):
        return self

    async def __anext__(self):
        evt = await self._evt_q.get()
        return evt

    async def set_value(self, val):
        """Set the value that's included when we send a ping.

        It is used for comparison when pings collide.
        """
        self._value = val

    def set_ready(self):
        """
        Set a flag to indicate that the client is fully operational and
        this node should participate actively.

        You *must* call this.
        """
        self._ready = True

    async def read_task(self, prefix: str, evt: anyio.abc.Event = None):
        """
        Reader task. Override e.g. if you have a Serf middleman.

        Call :meth:`queue_msg` with each incoming message.
        """
        async with self._client.stream("user:" + prefix) as mon:
            self.logger.debug("start listening")
            await evt.set()
            async for msg in mon:
                msg = self._unpacker(msg.payload)
                # self.logger.debug("recv %r", msg)
                await self.queue_msg(msg)

    async def queue_msg(self, msg):
        """
        Enqueue method.

        Call this from your override of :meth:`read_task`
        with each incoming message.
        """
        self.logger.debug("IN %s %r", self._name, msg)
        node = msg.get("node", None)
        if node is not None and node == self._name:
            if "history" not in msg:
                # This is a Hello message
                if self._self_seen:
                    # We may see our own Hello only once
                    raise SerfCollisionError()
                self._self_seen = True
            return  # sent by us
        if "history" not in msg:
            return  # somebody else's Ping
        await self._rdr_q.put(msg)

    async def _run(self):
        """
        """
        await self._send_ping(False)
        await anyio.sleep((self.random / 2 + 1.5) * self._gap + self._cycle)
        if not self._self_seen:
            raise SerfTimeoutError()
        await self._send_ping()

        t_dest = 0
        while True:
            t_left = t_dest - time.time()
            if self._tagged == 0:
                t = max(self._next_ping_time - time.time(), 0)
            elif t_left <= 0:
                if self._tagged < 0:
                    t = 2 * self._gap
                else:
                    # Timing Check: G + C-G/2 + G+G/2 == C+2G -- OK
                    if self._tagged == 1:
                        t = self._gap

                        self._tagged = 2

                    elif self._tagged == 2:
                        t = self._cycle - self._gap / 2

                        await self.post_event(TagEvent(self._name, self._value))
                        self._tagged = 3
                        self._valid_pings += 1

                    elif self._tagged == 3:
                        t = self._gap * 1.5

                        await self.post_event(UntagEvent())
                        self._tagged = 0

                    else:
                        raise RuntimeError("tagged", self._tagged)
                t_dest = t + time.time()
            else:
                t = t_left

            msg = None
            async with anyio.move_on_after(t):
                msg = await self._rdr_q.get()

            if msg is None:
                if self._tagged == 0:
                    await self._send_ping()
                continue

            if self._tagged == 1:
                # If we're about to be tagged and another message arrives,
                # skip this turn, for added safety.
                self._tagged = 0
            await self.post_event(RawPingEvent(msg))

            if await self.process_msg(msg):
                if self._tagged == 3:
                    await self.post_event(UntagEvent())
                self._tagged = 0

    async def enable(self, length=None):
        """
        Enable this actor.

        Args:
          length (int): New max length of the history. Default: Leave alone.
        """
        if self._tagged != -1:
            return
        self._tagged = 0
        self._history.clear()
        if length is not None:
            self._nodes = length
        self._history.maxlen = self._nodes
        await self._send_ping()

    async def disable(self, length=0):
        """
        Disable this actor.

        The history length is set to "indefinite" so that a passive node
        captures whatever is currently going on.

        Args:
          length (int): New max length of the history. Default: Zero.
            Set to ``None`` to not change the current size limit.
        """
        if self._tagged == 3:
            await self.post_event(UntagEvent())
        if length is not None:
            self._nodes = length
        self._history.maxlen = length
        self._tagged = -1

    async def process_msg(self, msg):
        """Process this incoming message."""

        if "node" in msg:
            msg_node = msg["node"]
        else:
            msg_node = msg["history"][0]

        if self._tagged < 0:
            self._get_next_ping_time()
            await self._ping_q.put(msg)
            return

        # We start off by sending a Ping. Thus our history is not empty.

        prev_node = self._history[0]
        this_val = msg["value"]

        if "node" in msg:
            # This is a recovery ping.
            ping = self._recover_pings.get(msg_node, None)
            if isinstance(ping, anyio.abc.Event):
                # We're waiting for this.
                await ping.set()
            else:
                # This ping is not expected, but it might have arrived before its cause.
                # Record that fact so that we don't also send it.
                self._recover_pings[msg_node] = self._valid_pings

        if msg_node == self._name:
            # my own message, returned
            return

        if msg_node == prev_node:
            # again, from that sender. Ideally that should not happen
            # because our timeout should be earlier, but Shit Happens.
            return

        self._values[msg_node] = this_val = msg["value"]

        if self._value is None and this_val is not None:
            # The other node is ready
            await self.post_event(
                GoodNodeEvent(
                    list(
                        h
                        for h in msg["history"]
                        if self._values.get(h, None) is not None
                    )
                )
            )

        if msg["history"] and (list(msg["history"][1:2]) == self._history[0:1]):
            if "node" in msg:
                # Standard ping.
                self._prev_history = self._history
                self._history += msg_node
                self._get_next_ping_time()
                await self._ping_q.put(msg)
                return True
            else:
                # This is a recovery ping for our side, after a split.
                # Ignore it: we already initiated recovery when sending the
                # notification, see below.
                return

        # Colliding pings.
        same_prev = msg["history"] and (list(msg["history"][1:2]) == self._history[1:2])

        prefer_new = self.has_priority(msg_node, prev_node)

        hist = NodeList(0, self._history)
        if prefer_new:
            nh = NodeList(self._nodes, msg["history"])  # self._prev_history
            self._history = nh

            if self._tagged:
                if self._tagged == 3:
                    await self.post_event(DetagEvent(msg_node))
                self._tagged = 0

            self._get_next_ping_time()
            await self._ping_q.put(msg)

        if same_prev:
            # These pings refer to the same previous ping. Good.
            return prefer_new

        # We either have a healed network split (bad) or are new (oh well).
        if self._value is not None:  # I am ready
            try:
                pos = hist.index(self._name)
            except ValueError:
                pass
            else:
                h = NodeList(0, msg["history"])
                if "node" in msg:
                    h += msg["node"]
                await self.post_event(RecoverEvent(pos, prefer_new, hist, h))

                evt = anyio.create_event()
                await self.spawn(self._send_delay_ping, pos, evt, hist)
                await evt.wait()

        return prefer_new

    async def post_event(self, event):
        """
        Send this event to the main loop. The event will
        be returned by this Actor's async iterator.

        Args:
          event: The event to forward.

        You may use this for your own events.
        """
        await self._evt_q.put(event)

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
        assert a != b, (a, b)
        return a < b

    async def _send_ping(self, history=None):
        if history is False:
            msg = {"node": self._name}
        else:
            if self._tagged < 0:
                return

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
            msg["history"] = history[0 : self._splits]  # noqa: E203
        await self.send_event(self._prefix, msg)

    async def send_event(self, prefix, msg):
        """
        Send a message. Override e.g. if you have a middleman.
        """
        await self._client.event(prefix, self._packer(msg), coalesce=False)

    async def _send_delay_ping(self, pos, evt, history):
        """
        After recovery, each side needs to send one message. Depending on
        my position in the list of members on my side, I wait some time
        for earlier nodes to do that. If no message arrives I do it.

        This is complicated by the fact that multiple recovery efforts may
        or may not be in progress.
        """
        node = history[0]
        ping = self._recover_pings.get(node, None)
        if (
            isinstance(ping, anyio.abc.Event)
            or isinstance(ping, int)
            and ping < self._valid_pings - self._nodes / 2
        ):
            if isinstance(ping, int):
                del self._recover_pings[node]
            await evt.set()
            return
        self._recover_pings[node] = e = anyio.create_event()
        await evt.set()

        # For pos=0 this is a no-op and times out immediately
        async with anyio.move_on_after(self._gap * (1 - 1 / (1 << pos))) as x:
            await e.wait()
        if self._recover_pings.get(node, None) is not e:
            # I have been superseded.
            return
        del self._recover_pings[node]

        if x.cancel_called:  # Timed out: thus, I send.
            if pos:
                # Complain if we're not the first node.
                self.logger.info("PingDelay: no signal %d", pos)
            await self._send_ping(history=history)

    def _get_next_ping_time(self):
        t = self._time_to_next_ping()
        self._next_ping_time = time.time() + self._cycle + self._gap * t

    def _time_to_next_ping(self):
        """Calculates the time until sending the next ping is a good idea,
        assuming that none arrive in the meantime, in cycles."""
        if not self._history:
            # we might be the only node
            return 1.9 - self.random / 5

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
                return 2 + self.random / 3

        return self.ping_delay(
            s - 1,
            lv,
            self._nodes - len(self._history),
            max(len(self._values), self._n_hosts),
        )

    def _skip_check(self):
        return False

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
          todo: True if the number of nodes is too low

        The default implementation uses ``0.5, 0.75, 0.875, …`` for nodes
        on the list, prioritizing the last node; some value between 0 and
        1/3 for (rare) random inserts; and some random value between 1.5
        and 2 as fall-back.

        Those random inserts happen more frequently when there are not
        enough nodes
        """

        if pos >= 0:
            # We are on the chain. Send ping depending on our position.
            return 1 - 1 / (1 << (length - pos)) / 2
            # this will never be 1 because we need to leave some time for
            # interlopers, below. Otherwise we could divide by l-1, as
            # l must be at least 2. s must also be at least 1.

        if todo > 0:
            # the chain is too short. Try harder to get onto it.

            # This is mockable for testing
            if self._skip_check():
                return 0
            f = todo
        else:
            f = total
        if self.random < 1 / f / total:
            # send early (try getting onto the chain)
            return self.random / 3
        else:
            # send late (fallback)
            return 1.5 + self.random / 2
