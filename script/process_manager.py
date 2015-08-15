#!/usr/bin/env python
#coding:utf8
########################################################################
# 
# Copyright (c) 2015 4Paradigm.com, Inc. All Rights Reserved
# 
########################################################################
"""
File: process_manager.py
Author: huangjing(huangjing@4paradigm.com)
Date: 2015/08/10 09:52:02
Description: process_manager
"""

import sys
sys.path.append("script/lib/col_check/")
sys.path.append("script/lib/join_check/")
sys.path.append("script/lib/preprocess/")

from factory import FactoryBase 
from factory import CreateObjError


class ProcesserManagerInitError(Exception):
    """init error """
    def __init__(self, module, class_name, reason):
        self.msg = "Can not Init class_name [%s] of module [%s] because of [%s]" \
            % (module, class_name, reason)

class ProcesserManagerLocateError(Exception):
    """ locate error """
    def __init__(self, module, class_name, reason):
        self.msg = "Can not Locate class_name [%s] of module [%s] because of [%s]" \
            % (module, class_name, reason)


class ProcesserManager(object):
    """ process manager """
    def __init__(self):
        self._manager = {}
        self._factory = FactoryBase()  
        self._manager["col_checker"] = {}
        self._manager["preprocesser"] = {}
        self._manager["join_checker"] = {}

    def init_class(self, section, class_name):
        """
        init class by class_name 
        
        Args:
            section:col_checker, preprocesser, join_checker 
            class_name: AA.BB

        Return:
            class_obj 
        
        Exception:
            ProcesserManagerInitError
        """
        if section not in self._manager:
            raise ProcesserManagerInitError(section, \
                                            class_name, \
                                            "No section %s" % (section))
        
        if class_name in self._manager[section]:
            return self._manager[section][class_name]

        # split class_name
        try:
            module_name, module_class = class_name.split(".")
        except ValueError as e:
            raise ProcesserManagerInitError(section, \
                                            class_name, \
                                            "%s" % (e))

        try:
            class_obj = self._factory.create_obj(module_name, module_class)
        except CreateObjError as e:
            raise ProcesserManagerInitError(section, \
                                            class_name, \
                                            "%s" % (e))
        
        self._manager[section][class_name] = class_obj
        return class_obj

    def locate(self, section, class_name):
        """
        locate class by section and class_name

        Args:
            section:
            class_name:

        Return:
            class_obj

        Exception:
            ProcesserManagerLocateError
        """
        try:
            return self._manager[section][class_name]
        except (KeyError, TypeError) as e:
            raise ProcesserManagerLocateError(section, \
                                              class_name, \
                                              "%s" % (e)) 

