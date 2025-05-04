"""
Events for actors to send
"""

from __future__ import annotations

from contextlib import suppress

from attrs import define

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .messages import Message
    from .nodelist import NodeList

    from typing import Any, ClassVar

__all__ = [
    "NodeEvent",
    "AuthPingEvent",
    "TagEvent",
    "UntagEvent",
    "DetagEvent",
    "RawMsgEvent",
    "PingEvent",
    "GoodNodeEvent",
    "RecoverEvent",
    "SetupEvent",
]


class NodeEvent:
    "Generic event"


class AuthPingEvent(NodeEvent):
    """
    Superclass for tag and ping: must arrive within :meth:`cycle_time_max` seconds of each other.

    Non-abstract subclasses of this must have ``name`` and ``value`` attributes.
    (TODO: enforce this)
    """


@define
class TagEvent(AuthPingEvent):  # [TNode]
    """
    This event says that for the moment, you're "it".

    Arguments:
      node(str): this node name.
      value (any): the value attached to me.
    """

    __qualname__: ClassVar[str] = "Tag"

    node: TNode
    value: Any


@define
class UntagEvent(NodeEvent):
    """
    Your tag cycle time has passed. You're no longer "it".
    """

    __qualname__: ClassVar[str] = "UnTag"


@define
class DetagEvent(UntagEvent):  # [TNode]
    """
    A ping from another node has arrived while you're "it".
    Unfortunately, it is "better" than ours.

    Arguments:
      node (str): The node that superseded us.
    """

    __qualname__: ClassVar[str] = "DeTag"

    node: TNode


@define
class RawMsgEvent(NodeEvent):
    """
    A message shows up. Not filtered. You must set "send_raw" when you
    create the actor.

    Arguments:
      msg (dict): The raw data
    """

    __qualname__: ClassVar[str] = "RawMsg"

    msg: Message


@define
class PingEvent(AuthPingEvent):
    """
    A ping from another node shows up: the node ``.node`` is "it".

    Arguments:
      msg (Message): The ping message sent by the currently-active actor.
    """

    __qualname__: ClassVar[str] = "Ping"

    msg: Message

    @property
    def node(self):
        """
        Name of the node. Shortcut to ``node.msg.node``.
        """
        try:
            return self.msg.node
        except AttributeError:
            return None

    @property
    def value(self):
        """
        Value of the node. Shortcut to ``node.msg.value``.
        """
        try:
            return self.msg.value
        except AttributeError:
            return None


@define
class GoodNodeEvent(NodeEvent):
    """
    A known-good node has been seen. We might want to get data from it.

    Arguments:
      nodes (list(str)): Nodes known to have a non-``None`` value.

    This event is seen while starting up, when our value is ``None``.
    """

    __qualname__: ClassVar[str] = "Good"

    nodes: NodeList


@define
class RecoverEvent(NodeEvent):
    """
    We need to recover from a network split.

    Arguments:
      prio: Our recovery priority. Zero is highest.
      replace: Flag whether the other side has superseded ours.
      local_nodes: A list of recent actors on our side.
      remote_nodes: A list of recent actors on the other side.
    """

    __qualname__: ClassVar[str] = "Recover"

    prio: int
    replace: bool
    local_nodes: NodeList
    remote_nodes: NodeList


@define
class SetupEvent(NodeEvent):
    """
    Parameters have been updated, most likely by the network.
    """

    __qualname__: ClassVar[str] = "Setup"

    version: int = 0

    cycle: float = 10
    gap: float = 1.5
    nodes: int = 5
    splits: int = 4
    n_hosts: int = 10
    force_in: bool = False

    def __init__(self, msg):
        self.__attrs_init__()
        for k in "version cycle gap nodes splits n_hosts".split():
            with suppress(AttributeError):
                setattr(self, k, getattr(msg,k))
