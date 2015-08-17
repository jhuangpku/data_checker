#!/usr/bin/env python
#coding:utf8
########################################################################
# 
# Copyright (c) 2015 4Paradigm.com, Inc. All Rights Reserved
# 
########################################################################
"""
File: preprocess_base.py
Author: huangjing(huangjing@4paradigm.com)
Date: 2015/08/05 14:24:49
Description: base for preprocess
"""

#class PreprocessError(Exception):
#    """preprocess error"""
#    def __init__(self, msg):
#        self.msg = msg
#
#    def __str__(self):
#        return self.msg



class PreprocessBase(object):
    """ base for all preprocess class
        try preprocess value into new value according to preprocess_method
        otherwise keep the original value
    """
    def __init__(self):
        """init"""
        pass
    
    def check_args(self, args):
        """
        Args:
            args: list type, args for preprocess 

        Return:
            0: success 
            1: fail
        """
        return 0

    def preprocess(self, value, args):
        """
        Args:
            value: depend col value, not a list !!!
            args: other args, the same with check_args 

        Return:
            value: not a list !!!
        
        """
        pass



