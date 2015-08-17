#!/usr/bin/env python
#coding:utf8
########################################################################
# 
# Copyright (c) 2015 4Paradigm.com, Inc. All Rights Reserved
# 
########################################################################
"""
File: join_checker_base.py
Author: huangjing(huangjing@4paradigm.com)
Date: 2015/08/05 14:49:26
Description:join checker base class
"""

#class JoinCheckerError(Exception):
#    """colchecker error"""
#    def __init__(self, msg):
#        self.msg = msg
#
#    def __str__(self):
#        return self.msg


class JoinCheckerBase(object):
    """ base for all joinchecker class"""
    def __init__(self):
        """init"""
        pass

    def check_args(self, args):
        """
        Args:
            args: list type, args for join checker

        Return:
            0: success 
            1: fail
        """
        return 0
    
    
    def check(self, dic, key, args):
        """
        Args:
            dic: dic type, every key is a tuple, it is the table1 key dic
            key: tuple type, one key of table2
            args: list type, other args, the same with check_args 

        Return:
            match_key: if match return key of table1, else, return None
        
        """
        return None


