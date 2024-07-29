"""
This module defines stuff that was removed in Python 3

Copyright 2024 ecmwf

Licensed under the Apache License, Version 2.0 (the "License")

http://www.apache.org/licenses/LICENSE-2.0
"""

import sys

if sys.version_info.major == 3:

    def cmp(x, y):
        """
        Replacement for built-in function cmp that was removed in Python 3
        Compare the two objects x and y and return an integer according to
        the outcome. The return value is negative if x < y, zero if x == y
        and strictly positive if x > y.
        """
        return (x > y) - (x < y)

    import functools
    unicode = str
    basestring = str
    reduce = functools.reduce

else:

    cmp = cmp
    unicode = unicode
    basestring = basestring
    reduce = reduce