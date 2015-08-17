#!/usr/bin/env python
#coding:utf8
########################################################################
# 
# Copyright (c) 2015 4Paradigm.com, Inc. All Rights Reserved
# 
########################################################################
"""
File: col_checker_time.py
Author: huangjing(huangjing@4paradigm.com)
Date: 2015/08/17 11:58:02
Description:time check
"""

import sys
import time
from col_checker_base import ColCheckerBase


class ColTimeChecker(object):
    """time checker"""
    def __init__(self):
        pass

    def check_args(self, args):
        """
        Args:
            args: time format
        """
        return 0

    def check(self, values, args):
        """
            Args:
            args:
        """
        time_format = args[0]
        if isinstance(values, list):
            value = values[0]
        else:
            value = values
        
        try:
            time.strftime("%Y-%m-%d %M:%H:%S" time.strptime(value, time_format))
        except ValueError:
            return False

        return True
