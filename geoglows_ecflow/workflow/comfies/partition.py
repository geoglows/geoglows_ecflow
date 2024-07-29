#!/usr/bin/env python

"""
Copyright 2024 ecmwf

Licensed under the Apache License, Version 2.0 (the "License")

http://www.apache.org/licenses/LICENSE-2.0
"""

import sys
import random

def partition(seq, ngroups, seed=None):
    """
    Split sequence into N nearly-equal sized sublists.
    """
    lst = list(seq)
    if seed is not None:
        random.seed(seed)
        random.shuffle(lst, random.random)
    division = len(lst) / float(ngroups)
    return [lst[int(round(division*i)):int(round(division*(i+1)))] for i in range(ngroups)]