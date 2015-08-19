#!/usr/bin/env python
#coding:utf8
########################################################################
# 
# Copyright (c) 2015 4Paradigm.com, Inc. All Rights Reserved
# 
########################################################################
"""
File: preprocess_time.py
Author: huangjing(huangjing@4paradigm.com)
Date: 2015/08/17 11:29:02
Description: time preprocess
"""

import sys
import time

from preprocess_base import PreprocessBase


class PreprocessTime(PreprocessBase):
    """ trim unuseful character """
    def __init__(self):
        """init"""
        self._t_formats = []
        p1 = ["%Y", "%m", "%d"]
        p2 = ["%H", "%M", "%S"]
        sep = [":", ",", " "]
        for s1 in sep:
            for s2 in sep:
                for t in sep:
                    self._t_formats.append(s1.join(p1) + t + s2.join(p2))


    def check_args(self, args):
        """
        Args:
            args for preprocess 

        Return:
            0: success 
            1: fail
        """
        return 0

    def preprocess(self, values, args):
        """
        Args:
            values: depend col value list
            args: other args, the same with check_args 

        Return:
            value
        
        Exception:
            PreprocessError
        """
        if isinstance(values, list):
            value = values[0]
        else:
            value = values
        t_formats = self._t_formats
        if args is not None:
            t_formats.extend(args)
        for t_format in t_formats:
            try:
                return [time.strftime("%Y-%m-%d %H:%M:%S", \
                                 time.strptime(value, t_format))]
            except ValueError:
                continue

        return [value]




