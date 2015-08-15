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

class JoinCheckerError(Exception):
    """colchecker error"""
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg


class JoinCheckerBase(object):
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
    
    
    def check_join(self, values1, values2, args):
        """
        Args:
            values1: depend col value list in table1
            values2: depend col value list in table2
            args: other args, the same with check_args 

        Return:
            True: can match
            False: not match
        Exception:
            ColCheckerError
        """
        pass


