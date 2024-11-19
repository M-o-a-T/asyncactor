=================================================
AsyncActor: selecting nodes on a broadcast medium
=================================================

Using AsyncActor, you can select a participant in a broadcast medium that's
responsible for tasks which only one node should do, discover whether there
is a network split, and related tasks.

Key features:

* async; uses `AnyIO <https://github.com/agronholm/anyio/>`,
  thus is compatible with ``asyncio`` and ``trio``.

* timeslot-based, ~1 message per slot

* Back-ends for Serf and MQTT included, others are easy to code

Inherited from `Trio <https://github.com/python-trio/trio>`_:

* Contributor guide: https://trio.readthedocs.io/en/latest/contributing.html

* Code of conduct: Contributors are requested to follow our `code of
  conduct <https://trio.readthedocs.io/en/latest/code-of-conduct.html>`_
  in all project spaces.

.. toctree::
   :maxdepth: 2

   rationale.rst
   actor.rst
   history.rst

====================
 Indices and tables
====================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
* :ref:`glossary`
