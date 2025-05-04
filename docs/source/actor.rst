===============
Actor selection
===============

Sometimes, distributed systems need to select a single actor among
themselves, and to synchronize state between them.

The :mod:`asyncactor.actor` module implements one way of doing this.
It is not a complete solution, but provides one major building block you
can use to achieve this in an asynchronous, leader-less context.


.. module:: asyncactor

Principle of operation
======================

AsyncActor is based on timeslots. At the start of each slot, one
participant is selected as the leader (or, like we said when we were
children: *you're "it"*).

Some time after the end of the slot, another participating actor broadcasts
a ``ping`` message. The first actor that does this starts the next slot.
Collisions are avoided by appropriately modelling the "some time after"
part; if that fails and messages cross each other, collisions are resolved
deterministically.

The resolution mechanism prefers new actors. Participating actors are
compared by their position in the cycle. With new actors, a ``value``
parameter that's set by :meth:`Actor.set_value` is used. If these values
happen to be equal, the collision is resolved by comparing the node names
(which must not be equal).

This algorithm intentionally does not require timestamps or similar means
of resolving a collision. If necessary, you can use them as (part of) the
value.

The resolution method may be changed, if necessary, by overriding
:meth:`Actor.has_priority`. Note that this method **must** be deterministic,
i.e. every recipient of a set of messages must select the same node as
the new leader, irrespective of the order these messages arrive in.

Depending on the parameters, the default implementation randomly selects a
number of participating actors and round-robins the "it" role between them.

A node's value does **not** determine which nodes are actors; it is only
used for conflict resolution in case of a collision.
There is one exception to this rule: a value of ``None`` indicates that a
node is not ready and should not be included if possible)

Occasionally, an actor that's not part of the cycle may butt in. This can
be changed by overriding :meth:`Actor.ping_delay`. You're free to base its
return value, which should be between zero and two, on whatever information
you have for your node.

An actor can be disabled; while it is, it will still generate
:class:`PingEvent` and :class:`RawMsgEvent` events but it won't try to
participate in the protocol. You can use this to selectively enable or
disable actors if you want e.g. to only let the highest-priority actors
be *it*.

Message Transport
+++++++++++++++++

AsyncActor messages can be transported by any method that doesn't lose
messages (except in case of a network split). MQTT with CBOR or MsgPack
encoding are good examples.

Messages *must not* be duplicated by the transport. In practice this means
that your MQTT server and library either must support subscription IDs, or
you need to be very careful not to create overlapping subscriptions. We
strongly recommend the former strategy because subscription IDs also avoid
redundant topic comparisons by the client.


API
===

.. autoclass:: Actor
   :members:

Events
++++++

An :class:`asyncactor.Actor` affords an async iterator which sends
events to its caller. Reading these events is mandatory, as they tell you
when your code is the active Actor.

You get a :class:`PingEvent` if some other actor is "it". The message is
delayed to protect against collisions, so that you should only get one of
these events per cycle.

.. autoclass:: PingEvent

A :class:`RawMsgEvent`, on the other hand, is triggered as soon as a
message from another participant arrives. These events may appear at any
time, particularly when resolving network splits; you may analyze their
contents, but shouldn't change your program flow at the time they arrive.

If you're "it", you get a :class:`TagEvent`.

.. autoclass:: TagEvent

At the end of the current cycle, you get an :class:`UntagEvent`.
You should gracefully (i.e. within at most ``gap`` seconds) stop your
activity, resp. that part that depends on you being "it".

.. autoclass:: UntagEvent

If there was a network split and the ``ping`` from the former other side
supersedes ours, you get an :class:`UntagEvent`. You should immediately
abort any activity that depends on you being "it".

You should not start a re-sync when you receive this event, as that's
indicated by a :class:`RecoverEvent`.

.. autoclass:: DetagEvent

When a network split is healed, some actors on both sides of the erstwhile
split get a :class:`RecoverEvent` that lists some nodes on the "other side"
which might be asked to provide information that needs to be resolved.

Coordinating recovery among the local actors is outside the scope of 
:class:`Actor`. However, this event includes a priority to help with that
task. It starts with zero (highest priority) and counts up to however many
local actors have been active lately, or to the ``splits`` config value,
whichever is lower.

.. autoclass:: RecoverEvent

Some protocols depend on some unique start-up value or require some data
before a node may participate. For instance, a node in a key-value storage
network needs the current state before it may serve clients.

The :class:`GoodNodeEvent` is sent if you didn't call
:meth:`Actor.set_value`, and a ``ping`` from a node with a value is
seen. This allows you to fetch, from the "good" node, whatever other data
you need to start operation.

How to do that is out of scope of this module. Typically you'd open a
direct TCP connection to the actor in question, and download the current
state.

.. autoclass:: GoodNodeEvent

.. autoclass:: RawMsgEvent

