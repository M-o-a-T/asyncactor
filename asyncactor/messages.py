# Actor message types

__all__ = [
    "Message",
    "SetupMessage",
    "InitMessage",
    "PingMessage",
    "HistoryMessage",
]


class DataError(TypeError):
    """
    Missing message data
    """

    pass


_types = {}

import logging

logger = logging.getLogger(__name__)


def _reg(cls):
    _types[cls._type] = cls
    return cls


class Message:
    _type = None
    node = None  # must exist

    def __new__(cls, t=None, **kv):  # pylint:disable=unused-argument
        if cls is Message:
            return object.__new__(_types[t])
        elif t is not None and cls._type != t:
            raise DataError("Type delta")
        return object.__new__(cls)

    def __init__(self, **kv):
        for k, v in kv.items():
            setattr(self, k, v)
        if self._type is None:
            raise DataError("Duh?")

    def __repr__(self):
        return "<%sMsg %s>" % (self._type, self.node,)

    @classmethod
    def read(cls, msg):
        cls = _types[msg["t"]]  # pylint:disable=self-cls-assignment
        assert cls.type == msg["t"]
        return cls(**msg)

    def pack(self):
        msg = {"t": self._type}
        for k, v in vars(self).items():
            if not k.startswith("_"):
                msg[k] = v
        return msg


class _NodeMessage(Message):
    node = None

    def __init__(self, **kv):
        super().__init__(**kv)
        if not self.node:
            logger.error("No node: %r", kv)


@_reg
class SetupMessage(_NodeMessage):
    """
    Parameters. Not clocked. Strictly rising version number.
    """

    version: int = 0
    cycle: float = None
    gap: float = None
    nodes: int = None
    splits: int = None
    n_hosts: int = None
    _type = "vers"

    def verify(self):
        if self.cycle < 1:
            raise ValueError("cycle must be >= 1")
        if self.gap < 0.0:
            raise ValueError("gap must be >= 0.1")
        if self.cycle < self.gap * 3:
            raise ValueError("cycle must be >= 3*gap")

    def __repr__(self):
        m = super().__repr__()
        return "%s %s%s" % (m[:-1], self.version, m[-1:])


@_reg
class InitMessage(_NodeMessage):
    """
    Sent when a node starts up.
    MUST NOT appear twice for a given node.
    """

    _type = "init"


@_reg
class PingMessage(_NodeMessage):
    """
    Your regular actor announcement.
    """

    _type = "ping"
    value = None
    history = ()  # NodeList

    def __repr__(self):
        m = super().__repr__()
        return "%s %s %s%s" % (m[:-1], self.value, ":".join(self.history), m[-1:])


@_reg
class HistoryMessage(Message):
    """
    Your regular actor announcement.
    """

    _type = "hist"
    value = None
    history = ()  # NodeList

    @property
    def node(self):
        return self.history[0]

    def __repr__(self):
        m = super().__repr__()
        return "%s %s%s" % (m[:-1], ":".join(self.history), m[-1:])
