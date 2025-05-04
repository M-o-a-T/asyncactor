"""
The main Actor module.
"""

from __future__ import annotations

import anyio
import logging
import os
import time
from random import Random

from moat.util import CtxObj, create_queue

from .events import (
    DetagEvent,
    GoodNodeEvent,
    PingEvent,
    RawMsgEvent,
    RecoverEvent,
    SetupEvent,
    TagEvent,
    UntagEvent,
)
from .exceptions import ActorCollisionError, ActorTimeoutError
from .messages import HistoryMessage, InitMessage, Message, PingMessage, SetupMessage
from .nodelist import NodeList

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .abc import Transport

__all__ = [
    "Actor",
]


async def spawn(taskgroup, proc, *args, **kw):
    """
    Run a task within this object's task group.

    Returns:
        a cancel scope you can use to stop the task.
    """

    async def _run(proc, args, kw, *, task_status):
        """
        Helper for starting a task within a cancel scope.
        """
        with anyio.CancelScope() as sc:
            task_status.started(sc)
            await proc(*args, **kw)

    return await taskgroup.start(_run, proc, args, kw)


class Actor(CtxObj):
    """
    Some jobs need a single controller so that tasks don't step on each other's
    toes.

    This class doesn't elect a controller either. It simply provides a
    timeout-based keepalive and round-robin scheme to periodically select a
    single host in a group, which you may or may not use as a controller
    for a limited time.

    Actor messages are broadcasts; they consist of:

    * my name
    * my current value (must be ``None`` if not ready)
    * a history of the names in previous pings (with my name filtered out).

    Arguments:
      client: The Transport client to use
      name (str): This node's name. **Must** be unambiguous.
      cfg (dict): a dict containing additional configuration values.
      enabled (bool): if False, start the actor in passive monitoring mode.
      send_raw (bool): flag whether to forward raw-message events (debugging)

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
      version: this configuration's version number. Overridden by the
        network as soon as a higher-versioned message appears. You MUST
        increment this integer by 2 (or more) whenever you change a config
        value.

    Actor coordinates with its peers to broadcast one ping per cycle; the
    actual time between cycle starts may be up to ``2*gap`` longer.

    """

    _worker = None
    _reader = None
    _pinger = None
    _value = None
    _tg = None
    _ae = None

    def __init__(  # pylint: disable=dangerous-default-value
        self,
        client: Transport,
        name: str,
        cfg: dict | None = None,
        *,
        enabled: bool = True,
        send_raw: bool = False,
    ):
        self._client = client
        self._name = name
        self.logger = logging.getLogger(f"asyncactor.{self._name}.{client.topic[-1]}")

        self._version = SetupMessage(node=self._name, **(cfg or {}))
        self._version_job = None
        self._off_nodes = self._version.nodes
        self._self_seen = anyio.Event()

        self._evt_q = create_queue(1)
        self._rdr_q = create_queue(99)
        self._ping_q = create_queue(1)
        self._ready = False
        self._tagged = 0 if enabled else -1  # >0: "our" tag is progressing
        self._valid_pings = 0
        self._send_raw = send_raw

        self._values = {}  # map names to steps
        self._history = NodeList(self._nodes)  # those in the loop
        self._prev_history = None
        self._rand = Random()  # noqa:S311

        self._next_ping_time = 0
        self._recover_pings = {}
        self._count_not_on = 0

    def __repr__(self):
        return f"<Actor {self._name} {self._client!r}>"

    # Accessors for config values

    @property
    def _cycle(self) -> float:
        "'cycle' from config"
        return self._version.cycle

    @property
    def _gap(self) -> float:
        "'gap' from config"
        return self._version.gap

    @property
    def _nodes(self) -> int:
        "'nodes' from config"
        if self._tagged < 0:
            return self._off_nodes
        return self._version.nodes

    @property
    def _splits(self) -> int:
        "'splits' from config"
        return self._version.splits

    @property
    def _n_hosts(self) -> int:
        "'n_hosts' from config"
        return self._version.n_hosts

    @property
    def _force_in(self) -> bool:
        "'force_in' from config"
        return self._version.force_in

    @property
    def random(self):
        """A random float between 0 and 1 (uniform)"""
        return self._rand.random()

    @property
    def name(self):
        "my name"
        return self._name

    @property
    def cycle_time(self):
        "'cycle' from config"
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

    async def _ping(self):
        """
        This subtask generates :cls:`PingEvent` messages. It ensures that
        there's exactly one PingEvent per cycle, if possible.
        """
        while True:
            msg = await self._ping_q.get()
            with anyio.move_on_after(self._gap * 2):
                while True:
                    msg = await self._ping_q.get()
            if self._tagged <= 0:
                self._valid_pings += 1
                await self.post_event(PingEvent(msg))

    async def _ctx(self):
        async with anyio.create_task_group() as tg:
            try:
                evt = anyio.Event()
                self._tg = tg
                self._reader = await spawn(tg, self.read_task, evt)

                await evt.wait()
                self._worker = await spawn(tg, self._run)
                self._pinger = await spawn(tg, self._ping)
                yield self
            finally:
                self._tg = None
                tg.cancel_scope.cancel()

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

    async def read_task(self, evt: anyio.abc.Event = None):
        """
        Reader task.

        Simply calls :meth:`queue_msg` with each incoming message.
        """
        async with self._client.monitor() as mon:
            self.logger.debug("start listening")
            evt.set()
            async for msg in mon:
                await self.queue_msg(msg)

    async def queue_msg(self, msg):
        """
        Enqueue method.

        Call this from your override of :meth:`read_task`
        with each incoming message.
        """
        if self._send_raw:
            await self.post_event(RawMsgEvent(msg))
        msg = Message.read(msg)
        if type(msg) is InitMessage and msg.node == self._name:
            if self._self_seen.is_set():
                # We may see our own Init only once.
                raise ActorCollisionError

                # This may not hold if your bronze-age MQTT server doesn't
                # do subscription IDs. Sorry, but that's not supported.

            self._self_seen.set()

        await self._rdr_q.put(msg)

    async def _run(self):
        """ """
        await self._send_msg(InitMessage(node=self._name))
        await self._send_msg(self._version)

        with anyio.fail_after((self.random / 2 + 1.5) * self._gap + self._cycle):
            await self._self_seen.wait()
        await self._send_ping()

        t_dest = 0
        while True:
            t_left = t_dest - time.monotonic()
            if self._tagged == 0:
                t = max(self._next_ping_time - time.monotonic(), 0)
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
                        self._count_not_on = 0
                        self._tagged = 0

                    else:
                        raise RuntimeError("tagged", self._tagged)
                t_dest = t + time.monotonic()
            else:
                t = t_left

            msg = None
            with anyio.move_on_after(t):
                msg = await self._rdr_q.get()

            if msg is None:
                if self._tagged == 0:
                    await self._send_ping()
                continue

            if self._tagged == 1:
                # If we're about to be tagged and another message arrives,
                # skip this turn, for added safety.
                self._tagged = 0

            if await self.process_msg(msg):
                if self._tagged == 3:
                    await self.post_event(UntagEvent())
                    self._count_not_on = 0
                self._tagged = 0

    async def update_config(self, cfg):
        """
        Update the current configuration.

        It is an error to not supersede the old config.
        """
        v = SetupMessage(**cfg)
        if v.version <= self._version.version:
            raise ValueError(f"You need a version > {self._version.version}")
        self._version = v
        await self._send_msg(v)

    async def enable(self, length=None):
        """
        Enable this actor.

        Args:
          length (int): New max length of the history. Default: Leave alone.
        """
        if length is not None and self._version.nodes < length:
            self._version.nodes = length
            self._version.version += 1
            await self._send_msg(self._version)

        if self._tagged != -1:
            return
        self._tagged = 0
        self._history.clear()
        self._history.maxlen = self._nodes
        self._count_not_on = 1
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
            self._off_nodes = length

        self._history.maxlen = length
        self._tagged = -1

    async def process_msg(self, msg):
        """Process this incoming message.

        Returns ``True`` if the message is a valid, not-superseded ping.
        """

        if self._tagged < 0:
            self._get_next_ping_time()
            await self._ping_q.put(msg)
            return

        # We start off by sending a Ping. Thus our history is not empty.
        self.logger.debug("IN : %r", msg)

        prev_node = self._history[0]

        if type(msg) is PingMessage:
            # This is a recovery ping.
            ping = self._recover_pings.get(msg.node, None)
            if isinstance(ping, anyio.abc.Event):
                # We're waiting for this.
                ping.set()
            else:
                # This ping is not expected, but it might have arrived before its cause.
                # Record that fact so that we don't also send it.
                self._recover_pings[msg.node] = self._valid_pings

        elif type(msg) is InitMessage:
            return  # already processed

        elif type(msg) is SetupMessage:
            if msg.version > self._version.version:
                # Supersede my params
                self.logger.debug("new V%s, have V%s", msg.version, self._version.version)
                self._version = msg
                await self.post_event(SetupEvent(msg))

            elif msg.version < self._version.version:
                # Lower version seen: send my own version!
                if self._tagged > 1:
                    self.logger.debug(
                        "old V%s, have V%s, send", msg.version, self._version.version
                    )
                    await self._send_msg(self._version)
                else:
                    try:
                        pos = self._history.index(self._name)
                    except ValueError:
                        pass
                    else:
                        self.logger.debug(
                            "old V%s, have V%s, send %s", msg.version, self._version.version, pos
                        )
                        self._tg.start_soon(self._send_delay_version, pos)

            elif self._version_job is not None:
                self.logger.debug("cancel V%s", msg.version)
                self._version_job.cancel()
                self._version_job = None
            return

        elif type(msg) is HistoryMessage:
            pass
        else:
            raise RuntimeError("unknown message type")

        if msg.node == self._name:
            # my own message, returned
            return

        if msg.node == prev_node:
            # again, from that sender. Ideally that should not happen
            # because our timeout should be earlier, but Shit Happens.
            return

        self._values[msg.node] = this_val = msg.value

        if self._value is None and this_val is not None:
            # The other node is ready
            await self.post_event(
                GoodNodeEvent(
                    list(h for h in msg.history if self._values.get(h, None) is not None)
                )
            )

        if msg.history and (list(msg.history[1:2]) == self._history[0:1]):
            if type(msg) is HistoryMessage:
                # This is a recovery ping, after a split.
                # Ignore it: we already initiated recovery when sending the
                # notification, see below.
                return

            # Standard ping.
            self._prev_history = self._history
            self._history += msg.node
            self._get_next_ping_time()
            await self._ping_q.put(msg)
            return True

        # Colliding pings.
        same_prev = msg.history and (list(msg.history[1:2]) == self._history[1:2])

        prefer_new = self.has_priority(msg.node, prev_node)

        hist = NodeList(0, self._history)
        if prefer_new:
            nh = NodeList(self._nodes, msg.history)  # self._prev_history
            self._history = nh

            if self._tagged:
                if self._tagged == 3:
                    await self.post_event(DetagEvent(msg.node))
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
                h = NodeList(0, msg.history)
                if type(msg) is not HistoryMessage:
                    h += msg.node
                await self.post_event(RecoverEvent(pos, prefer_new, hist, h))

                evt = anyio.Event()
                self._tg.start_soon(self._send_delay_ping, pos, evt, hist)
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
        self.logger.debug("EVT: %r", event)
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
        if self._tagged < 0:
            return

        if history is not None:
            msg = HistoryMessage(value=self._values[history[0]])
        else:
            msg = PingMessage(node=self._name, value=self._value)
            self._values[self._name] = self._value
            if self._history:
                self._tagged = 1
            history = self._prev_history = self._history
            self._history += self._name
            self._get_next_ping_time()
        msg.history = history[0 : self._splits]
        await self._send_msg(msg)

    async def _send_msg(self, msg: Message):
        self.logger.debug("OUT: %r", msg)
        await self._client.send(msg.pack())

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
            and self._nodes > 1
            and ping < self._valid_pings - self._nodes / 2
        ):
            if isinstance(ping, int):
                del self._recover_pings[node]
            evt.set()
            return
        self._recover_pings[node] = e = anyio.Event()
        evt.set()

        # For pos=0 this is a no-op and times out immediately
        with anyio.move_on_after(self._gap * (1 - 1 / (1 << pos))) as x:
            await e.wait()
        if self._recover_pings.get(node, None) is not e:
            # I have been superseded.
            return
        del self._recover_pings[node]

        if x.cancel_called:  # Timed out: thus, I send.
            if pos:
                # Complain if we're not the first node.
                self.logger.info("PingDelay: no signal %d", pos)
            await self._send_ping(history)

    async def _send_delay_version(self, pos):
        """
        After recovery, each side needs to send one message. Depending on
        my position in the list of members on my side, I wait some time
        for earlier nodes to do that. If no message arrives I do it.

        This is complicated by the fact that multiple recovery efforts may
        or may not be in progress.
        """
        # For pos=0 this is a no-op and times out immediately
        try:
            with anyio.CancelScope() as xx:
                if self._version_job is not None:
                    self._version_job.cancel()
                self._version_job = xx
                await anyio.sleep(self._gap * (1 - 1 / (1 << pos)))
                # Timed out: thus, I send.
                await self._send_msg(self._version)

        finally:
            if self._version_job is xx:
                self._version_job = None

    def _get_next_ping_time(self):
        t = self._time_to_next_ping()
        self._next_ping_time = time.monotonic() + self._cycle + self._gap * t

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
            if self._nodes > 0 and lv > self._nodes:
                break
        if not self._ready and p > lv // 2:
            # No, the first active host is too far back.
            return 2 + self.random / 3

        return self.ping_delay(
            s - 1,
            lv,
            (self._nodes - len(self._history)) if self._nodes > 0 else 1,
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
          todo: >0 if the number of nodes is too low

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

        if self._force_in:
            # this is an actor that needs every participant to be on the chain.
            # Thus try harder to make that happen.
            if self._count_not_on >= total:
                return self.random / 4
            elif self.random > (self._count_not_on - 1) / total:
                return 0.5 + self.random / 2
            else:
                return self.random / 2

        if todo > 0:
            # the chain is too short. Try somewhat harder to get onto it.

            # This is mockable for testing
            if self._skip_check():
                return 0
            f = todo
        else:
            self._count_not_on += 1
            f = total

        if self.random < 1 / f / total:
            # send early (try getting onto the chain)
            return self.random / 3
        else:
            # send late (fallback)
            return 1.5 + self.random / 2
