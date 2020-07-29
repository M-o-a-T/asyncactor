#
# Abstract classes for the underlying transport
#

from abc import ABCMeta, abstractmethod

from typing import Dict, Union, Tuple, List

Packable = Union[
    str, bytes, type(None), int, float, Dict[str, "Packable"], Tuple["Packable"], List["Packable"]
]


class Transport(metaclass=ABCMeta):
    """
    This abstract class describes the underlying transport which the Actor
    expects.

    The transport must be able to carry lists, dicts, None, and numbers
    (int, float) transparently. True/False may be transformed t0 0/1.
    """

    @abstractmethod
    def monitor(self) -> "MonitorStream":
        """Return a MonitorStream that async-iterates payloads sent to this channel"""

    @abstractmethod
    async def send(self, payload: Packable):
        """send this payload to this channel"""


class MonitorStream(metaclass=ABCMeta):
    """Async context manager and iterator that attaches to a channel and
    receives data
    """

    def __init__(self, transport: Transport):
        self.transport = transport

    @abstractmethod
    async def __aenter__(self):
        return self

    @abstractmethod
    async def __aexit__(self, *tb):
        pass

    def __aiter__(self):
        return self

    @abstractmethod
    async def __anext__(self) -> Packable:
        pass
