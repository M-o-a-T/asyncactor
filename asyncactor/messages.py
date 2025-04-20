"""
Actor message types
"""

from __future__ import annotations

import logging
from contextlib import suppress

from attrs import asdict, define

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .node import NodeList

    from typing import Any, ClassVar

__all__ = [
    "Message",
    "SetupMessage",
    "InitMessage",
    "PingMessage",
    "HistoryMessage",
]


logger = logging.getLogger(__name__)


class DataError(TypeError):
    """
    Missing message data
    """


_types = {}


def _reg(cls):
    _types[cls._type] = cls
    return cls


@define
class Message:
    """
    Superclass for actor messages.
    """

    _type: ClassVar[str | None] = None
    node = None  # must exist

    def __attrs_post_init__(self):
        if self._type is None:
            raise DataError("Duh?")

    def __repr__(self):
        return f"<{self._type}Msg {self.node}>"

    @staticmethod
    def read(msg):
        "build from data"
        t = msg.pop("t")
        cls = _types[t]  # pylint:disable=self-cls-assignment
        assert cls._type == t
        return cls(**msg)

    def pack(self):
        "build data"
        msg = asdict(self)
        msg["t"] = self._type
        return msg


@define
class _NodeMessage(Message):  # [TNode]
    node: TNode = None

    def __attrs_post_init__(self):
        if not self.node:
            raise ValueError("No node")


@_reg
@define
class SetupMessage(_NodeMessage):
    """
    Parameters. Not clocked. Strictly rising version number.
    """

    version: int = 0
    cycle: float = 10
    gap: float = 1.5
    nodes: int = 5
    splits: int = 4
    n_hosts: int = 10
    force_in: bool = False

    _type: ClassVar[str] = "vers"

    def __init__(self, **kw):
        self.__attrs_init__()

        for k, v in kw.items():
            with suppress(AttributeError):
                setattr(self, k, v)
        super().__attrs_post_init__()
        self._verify()

    def __attrs_post_init__(self):
        pass

    def _verify(self):
        "check parameters"
        if self.cycle < .1:
            raise ValueError("cycle must be >= .1")
        if self.gap < 0.01:
            raise ValueError("gap must be >= 0.01")
        if self.cycle < self.gap * 3:
            raise ValueError("cycle must be >= 3*gap")

    def __repr__(self):
        m = super().__repr__()
        return f"{m[:-1]} {self.version}{m[-1:]}"


@_reg
@define
class InitMessage(_NodeMessage):
    """
    Sent when a node starts up.
    MUST NOT appear twice for a given node.
    """

    _type: ClassVar[str] = "init"


@_reg
@define
class PingMessage(_NodeMessage):  # [TNode]
    """
    Your regular actor announcement.
    """

    value: Any = None
    history: NodeList[TNode] = ()  # NodeList

    _type: ClassVar[str] = "ping"

    def __repr__(self):
        m = super().__repr__()
        return f"{m[:-1]} {self.value} {':'.join(self.history)}{m[-1:]}"


@_reg
@define
class HistoryMessage(Message):  # [TNode]
    """
    Your regular actor announcement.
    """

    value:Any = None
    history: NodeList[TNode] = ()

    _type: ClassVar[str] = "hist"

    @property
    def node(self) -> TNode:
        "current node"
        return self.history[0]

    def __repr__(self):
        m = super().__repr__()
        return f"{m[:-1]} {':'.join(self.history)}{m[-1:]}"
