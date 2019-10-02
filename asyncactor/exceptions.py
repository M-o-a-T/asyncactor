class ActorError(Exception):
    """
    Generic class for errors returned by Serf.
    """

    pass


class ActorTimeoutError(ActorError):
    """
    Exception raised when we get no timely reply.
    """

    pass


class ActorCollisionError(ActorError):
    """
    Exception raised when an actor sees another with the same name.
    """

    pass
