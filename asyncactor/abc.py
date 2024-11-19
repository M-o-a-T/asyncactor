"""
Abstract classes for the underlying transport
"""

from __future__ import annotations

from abc import ABCMeta, abstractmethod

import anyio

from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from typing import Self

Packable = Union[  # noqa:UP007
    str,
    bytes,
    bool,
    type(None),
    int,
    float,
    dict[str | int, "Packable"],
    tuple["Packable"],
    list["Packable"],
]


class Transport(metaclass=ABCMeta):
    """
    This abstract class describes the underlying transport which the Actor
    expects.

    The transport must be able to carry lists, dicts, None, and numbers
    (int, float) transparently. True/False may be transformed t0 0/1.
    """

    @abstractmethod
    def monitor(self) -> MonitorStream:
        """Return a MonitorStream that async-iterates payloads sent to this channel"""

    @abstractmethod
    async def send(self, payload: Packable):
        """send this payload to this channel"""

    async def receiver(self, *, task_status=anyio.TASK_STATUS_IGNORED):
        """
        A dummy receiver which the transport may override,
        otherwise it's the client's job
        """
        task_status.started()


class MonitorStream(metaclass=ABCMeta):
    """Async context manager and iterator that attaches to a channel and
    receives data
    """

    def __init__(self, transport: Transport):
        self.transport = transport

    def __aiter__(self) -> Self:
        return self

    @abstractmethod
    async def __anext__(self) -> Packable:
        pass
