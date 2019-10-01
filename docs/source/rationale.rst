+++++++++
Rationale
+++++++++

Distributed systems frequently need a "source of truth". This source can be
permanent (one node is selected as the master until it dies), distributed
(a consensus algorithm decides what the truth is), or ephemeral (the master
role frequently changes).

AsyncActor uses the third way.

