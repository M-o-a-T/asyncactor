"""
Actor Exceptions.
"""

from __future__ import annotations


class ActorError(Exception):
    """
    Generic class for errors returned by Serf.
    """


class ActorTimeoutError(ActorError):
    """
    Exception raised when we get no timely reply.
    """


class ActorCollisionError(ActorError):
    """
    Exception raised when an actor sees another with the same name.
    """
