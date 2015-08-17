#!/usr/bin/env python
#coding:utf8
########################################################################
# 
# Copyright (c) 2015 4Paradigm.com, Inc. All Rights Reserved
# 
########################################################################
"""
File: col_checker_base.py
Author: huangjing(huangjing@4paradigm.com)
Date: 2015/08/05 14:40:58
Description:
"""

#class ColCheckerError(Exception):
#    """colchecker error"""
#    def __init__(self, msg):
#        self.msg = msg
#
#    def __str__(self):
#        return self.msg
#

class ColCheckerBase(object):
    """ base for all colchecker class"""
    def __init__(self):
        """init"""
        pass

    def check_args(self, args):
        """
        Args:
            args: list type, args for col_checker

        Return:
            0: success 
            1: fail
        """
        return 0
    
    def check(self, values, args):
        """
        Args:
            values: list type, depend col value list
            args: list type, other args, the same with check_args 

        Return:
            True: valid
            False: not valid
        """
        return True

