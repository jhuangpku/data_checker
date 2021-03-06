#!/usr/bin/env python
#coding:utf8
########################################################################
# 
# Copyright (c) 2015 4Paradigm.com, Inc. All Rights Reserved
# 
########################################################################
"""
File: preprocess_trim.py
Author: huangjing(huangjing@4paradigm.com)
Date: 2015/08/17 08:16:52
Description:trim unuseful character
"""
import sys

from preprocess_base import PreprocessBase


class PreprocessTrim(PreprocessBase):
    """ trim unuseful character """
    def __init__(self):
        """init"""
        pass

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
        if not isinstance(values, list):
            values = [values]
        
        return [v.strip('\n\r" ') for v in values]




