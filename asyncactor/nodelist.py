"""
Node list
"""

from __future__ import annotations

__all__ = [
    "NodeList",
]


class NodeList(list):  # [TName]
    """
    This is an augmented :class: `list`, used to store unique node names,
    up to some maximum (if used).

    This is a simple implementation. It should not be used for large
    lists.

    Arguments:
      maxlen (int): The max length of the list. Use zero for "indefinite"
        (not recommended).
      mutable (bool): A flag whether "nodelist += foo" should modify "foo"
        in-place. If not (the default), a new list will be allocated.

    >>> n = NodeList(3)
    >>> n += "a"
    >>> n
    ['a']
    >>> n += "a"
    >>> n
    ['a']
    >>> n += "b"
    >>> n
    ['b', 'a']
    >>> n += "a"
    >>> n
    ['a', 'b']
    >>> n += "c"
    >>> n
    ['c', 'a', 'b']
    >>> n += "d"
    >>> n
    ['d', 'c', 'a']
    >>> n += "c"
    >>> n
    ['c', 'd']
    >>>
    """

    def __init__(self, maxlen, data=(), mutable=False):
        super().__init__(data)
        self.maxlen = maxlen
        self._mutable = mutable

    def __iadd__(self, name: TName, mutable=None):
        """Move 'name' to the front (or add it).

        This shortens the list by one if you replace a node that's not
        at the end (if maxlen is >0). The effect is that nodes are removed
        from the end gradually. This is useful when a network split results
        in stale nodes.
        """
        if mutable is None:
            mutable = self._mutable

        try:
            i = self.index(name)
        except ValueError:
            i = -1
        if mutable:
            self = type(self)(self.maxlen, self, mutable=self._mutable)  # pylint: disable=W0642  # noqa:PLW0642

        if i >= 0:
            self.pop(i)

        # We pop an additional item if
        # + the length is bounded
        # + there's something that can be removed
        # + we either
        # -- removed something (except from the end), or
        # -- the list is maxed out, i.e. we didn't remove anything
        if self.maxlen > 0 and len(self) > 0 and (0 <= i < len(self) or len(self) == self.maxlen):
            self.pop()
        self.insert(0, name)
        return self

    def __add__(self, name: TName):
        return self.__iadd__(name, mutable=False)
