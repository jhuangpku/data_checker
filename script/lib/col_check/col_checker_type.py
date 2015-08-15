#!/usr/bin/env python
#coding:utf8
########################################################################
# 
# Copyright (c) 2015 4Paradigm.com, Inc. All Rights Reserved
# 
########################################################################
"""
File: col_checker_type.py
Author: huangjing(huangjing@4paradigm.com)
Date: 2015/08/15 16:02:53
Description:
"""

import sys

from col_checker_base import ColCheckerBase


class ColTypeChecker(object):
    """type checker"""
    def __init__(self):
        self._dict = {
            u"int":int,
            u"float":float,
        }


    def check_args(self, args):
        """
        Args:
            args: type list
        """
        if not isinstance(args, list):
            return 1
        for arg in args:
            if arg not in self._dict:
                return 1
        return 0

    def check_col(self, values, args):
        """
            Args:
            args:
        """
        if isinstance(values, list):
            value = values[0]
        else:
            value = values
        types = (self._dict[t] for t in args)
        return isinstance(value, types)

