# Events for actors to send

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

    Arguments:
      node(str): this node name.
      value (any): the value attached to me.
    """

    def __init__(self, node, value):
        self.node = node
        self.value = value

    def __repr__(self):
        return "<Tag %s %r>" % (self.node, self.value)


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
        return "<DeTag %s>" % (self.node,)


class RawMsgEvent(NodeEvent):
    """
    A message shows up. Not filtered. You must set "send_raw" when you
    create the actor.

    Arguments:
      msg (dict): The raw data
    """

    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
        return "<RawMsg %r>" % (self.msg,)


class PingEvent(AuthPingEvent):
    """
    A ping from another node shows up: the node ``.node`` is "it".

    Arguments:
      msg (Message): The ping message sent by the currently-active actor.
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
        try:
            return self.msg.node
        except AttributeError:
            return None

    @property
    def value(self):
        """
        Name of the node. Shortcut to ``msg['node']``.
        """
        try:
            return self.msg.value
        except AttributeError:
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
        return "<Good %s>" % (self.nodes,)


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


class SetupEvent(NodeEvent):
    """
    Parameters have been updated, most likely by the network.
    """

    version = None

    def __init__(self, msg):
        for k in "version cycle gap nodes splits n_hosts".split():
            try:
                setattr(self, k, getattr(msg, k))
            except AttributeError:
                pass

    def __repr__(self):
        return "<Setup v:%s>" % (self.version,)
