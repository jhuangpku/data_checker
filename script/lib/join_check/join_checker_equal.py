#!/usr/bin/env python
#coding:utf8
########################################################################
# 
# Copyright (c) 2015 4Paradigm.com, Inc. All Rights Reserved
# 
########################################################################
"""
File: join_checker_equal.py
Author: huangjing(huangjing@4paradigm.com)
Date: 2015/08/17 08:49:42
Description: equal checker
"""

import sys

from join_checker_base import JoinCheckerBase

class EqualJoinChecker(JoinCheckerBase):
    """ base for all joinchecker class"""
    def __init__(self):
        """init"""
        pass

    def check_args(self, args):
        """
        Args:
            args: other arguments 

        Return:
            0: success 
            1: fail
        """
        return 0
    
    
    def check(self, dic, key, args):
        """
        Args:
            dic: table1 key dic
            key: one key of table2
        
        Return:
            match_key: if match return key of table1, else, return None
        
        """
        if key in dic:
            return key
        else:
            return None


