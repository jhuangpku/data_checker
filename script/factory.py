#!/usr/bin/env python
# coding:utf8
########################################################################
# 
# Copyright (c) 2015 4Paradigm.com, Inc. All Rights Reserved
# 
########################################################################
 
"""
File: factory.py
Author: huangjing(huangjing@4Paradigm.com)
Date: 2015/08/05 15:30:46
Description: 生产各种各类的工厂
"""
import logging


class CreateObjError(Exception):
    """create obj error definition"""
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg


class FactoryBase(object):
    """class factory"""

    def __init__(self):
        pass

    def create_obj(self, module_name, module_class):
        """
        根据模块名和模块class生成对应的class
        Args:
            module_name: input, 模块名
            module_class: input, 模块中的class名
            result：初始化的类名

        Return: 
            class_obj
        
        Exception:
            CreateObjError
        """
        try:
            module = __import__(module_name)
        except ImportError as e:
            logging.warning("Can not import this module [%s] because of [%s]" % (module_name, e))
            raise CreateObjError("Can not import this module [%s] because of [%s]" \
                    % (module_name, e))
        try:
            class_obj = getattr(module, module_class)
        except AttributeError as e:
            logging.warning("Can not get module_class [%s] because of [%s]" % (module_class, e))
            raise CreateObjError("Can not get module_class [%s] because of [%s]" \
                    % (module_class, e)) 
        try:
            result = class_obj()
        except NameError as e:
            logging.warning("Can not init class [%s] in [%s] because of [%s]" % (module_class, \
                    module, e))
            raise CreateObjError("Can not init class [%s] in [%s] because of [%s]" \
                    % (module_class, module, e))
        
        return result


#class FeatureFactory(FactoryBase):
#    """ feature factory"""
#    def __init__(self):
#        pass
#
#    def create_obj(self, module_class):
#        """
#        据模块class生成对应的class
#
#        Args:
#            module_class: a.b (a:module_name b:class_name)
#
#        Return: 
#            feature_class_obj
#        
#        Exception:
#            FeFrameworkCreateObjError
#        """
#        module_name, module_class = module_class.split(".")
#        try:
#            result = super(FeatureFactory, self).create_obj(module_name, module_class)
#        except FeFrameworkCreateObjError as e:
#            logging.warning("create feature obj failed because of [%s]" % (e))
#            raise  FeFrameworkCreateObjError("create feature obj failed because of [%s]" % (e))
#        
#        return result
#
#
#class ComputeFactory(FactoryBase):
#    """ compute factory"""
#    def __init__(self):
#        pass
#
#    def create_obj(self, module_class):
#        """
#        据模块class生成对应的class
#
#        Args:
#            module_class: a.b (a:module_name b:class_name)
#
#        Return: 
#            feature_class_obj
#        
#        Exception:
#            FeFrameworkCreateObjError
#        """
#        module_name, module_class = module_class.split(".")
#        try:
#            result = super(ComputeFactory, self).create_obj(module_name, module_class)
#        except FeFrameworkCreateObjError as e:
#            logging.warning("create compute obj failed because of [%s]" % (e))
#            raise  FeFrameworkCreateObjError("create compute obj failed because of [%s]" % (e))
#        return result
#   
#
#
