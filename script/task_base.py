#!/usr/bin/env python
#coding:utf8
########################################################################
# 
# Copyright (c) 2015 4Paradigm.com, Inc. All Rights Reserved
# 
########################################################################
"""
File: task_base.py
Author: huangjing(huangjing@4paradigm.com)
Date: 2015/08/05 14:14:57
Description: task基类
"""
import time
import json

class TaskInitError(Exception):
    """init task error"""

class TaskExcuteError(Exception):
    """excute task error"""


class FieldList(object):
    """ field list type"""
    def __init__(self, json_list):
        """init from json_list"""
        self._fields = []
        for j in json_list:
            try:
                field = Field(j)
            except (TypeError, KeyError, ValueError) as e:
                raise e
            self._fields.append(field)

    def get_process_name(self):
        """
        get process names
        """
        names = []
        for field in self._fields:
            name = field.get_process_name()
            if name is not None:
                names.append(name)
        return names 

    def get_values(self, process_manager, cols):
        """
        get values
        """
        values = []
        for field in self._fields:
            values.extend(field.get_values(process_manager, cols))
        return values

class Field(object):
    """ field type"""
    def __init__(self, dic):
        """init field from dic"""
        try:
            self.field_no = dic["field_no"]
        except (TypeError, KeyError) as e:
            raise e
        try:
            self.field_no = int(self.field_no)
        except TypeError as e:
            raise e

        if self.field_no < 0:
            raise ValueError("Invalid field_no [%d], It should >= 0" % (self.field_no))
        
        self.process_args = None
        self.process_name = None

        if "args" in dic:
            self.process_args = dic["args"]
            if not isinstance(self.process_args, list):
                raise TypeError("Invalid process_class [%s], it should be list" \
                            % (str(self.process_args)))

        if "processer" in dic:
            self.process_name = dic["processer"]
            if not isinstance(self.process_name, unicode):
                raise TypeError("Invalid process_class, it should be string")

    def get_values(self, process_manager, cols):
        """
        get values according to process_manager
        
        Args:
            process_manager

        Return:
            list
        """
        try:
            value = cols[self.field_no]
        except IndexError as e:
            raise e
        
        if self.process_name is not None:
            try:
                process_class = process_manager.locate("preprocess", self.process_name)
            except ProcesserManagerLocateError as e:
                raise e
            value = process_class(value, self.process_args)
        if isinstance(value, list):
            return value
        else:
            return [value]
    
    def get_process_name(self):
        """ get process_name """
        if self.process_name is not None:
            return ["preprocess", self.process_name, self.process_args]
        else:
            return None

class StatusInfo(object):
    """ status info """
    def __init__(self, check_name):
        """
        init function

        Args:
            check_name 
        """
        self.check_name = check_name
        self.check_cnt = 0
        self.check_fail_cnt = 0
        self.expect_fail_cnt = 0
        self.fail_info = [] # fail reason, fail line 

    def __str__(self):
        """
        str 
        """
        str_list = []
        if self.check_fail_cnt == 0:
            str_list.append("Check task [%s] successful" % (self.check_name))
        else:
            str_list.append("Check task [%s] failed" % (self.check_name)) 
        str_list.append("Detail info:")
        str_list.append("------Check [%d] lines, failed [%d] lines, expect failed [%d] lines" \
                        % (self.check_cnt, self.check_fail_cnt, self.expect_fail_cnt))
        if len(self.fail_info) != 0:
            str_list.append("------Top Failed lines:")
            for (reason, line) in self.fail_info:
                str_list.append("------[%s]:line [%s]" % (reason, line))
        return "------" + "\n------".join(str_list)



class TaskBase(object):
    """Base for all kinds of task"""
    def __init__(self, line):
        """init function
        Args: 
            line: task command line 
        """
        line = line.rstrip("\n")
        
        try:
            self._json = json.loads(line)
        except ValueError as e:
            raise e
        self._task_name = repr(self._json)
        self._status_infos = []
        self._if_success = False
        self._time = time.strftime("%Y%m%d %H:%M:%S", time.localtime())


    def get_attribute(self, key, obj, value_type):
        """
        get attribute from obj and check validation according to value_type
        
        Args:
            key: str 
            obj: json like object
            value_type: type

        Return:
            value_type_obj
        
        Exception:
            KeyError, TypeError, ValueError
        """
        if value_type == "args":
            if key not in obj:
                return None
        
        try:
            value = obj[key]
        except KeyError as e:
            raise KeyError("key [%s] is not in obj [%s]" % (key, obj))
        if value_type == "ratio":
            try:
                return self._get_ratio(value)
            except (TypeError, ValueError) as e:
                raise e
        
        elif value_type == "fields":
            try:
                return self.get_fields(value)
            except (TypeError, ValueError, KeyError, IndexError) as e:
                raise e
        
        elif value_type == "code":
            try:
                "".encode(value)
            except LookupError as e:
                raise TypeError("Invalid coding [%s]" % (value))
        
        elif value_type == "args":
            if not isinstance(value, list):
                raise TypeError("Invalid args [%s]. It should be a list" % (value))
        
        else:
            try:
                ret = isinstance(value, value_type)
            except TypeError as e:
                raise TypeError("value [%s] doesnot match Value_type [%s]" % (str(value), str(value_type)))
        
            if ret is False:
                raise TypeError("value [%s] doesnot match Value_type [%s]" % (str(value), str(value_type)))
        
        return value

    def get_fields(self, value):
        """
        init field from value
        """
        try:
            return FieldList(value)
        except (TypeError, ValueError, KeyError) as e:
            raise e


    def _get_ratio(self, value):
        """
        check value is a validation of ratio type
        
        Args:
            value

        Return:
            ratio
        
        Exception:
            TypeError, ValueError
        """
        try:
            ratio = float(value)
        except TypeError as e:
            raise e

        if ratio <= 0 and ratio > 1:
            raise ValueError("Invalid ratio [%lf] it should between (0, 1]" % (ratio))

        return ratio
    
    
    
    def excute(self, process_manager):
        """ excute the task 
        Args: 
            checker_manager: manager which manage check function
        
        Return:
            0: success
            1: fail
        """
        return 0

    def write_status(self, file_handler, encode):
        """ write excute status into file_handler
        Args:
            file_handler: output stream

        Return:
            0: success 
        """
        str_list = []
        print self._if_success
        if self._if_success is False:
            str_list.append("[%s] Excute task [%s] failed" % (self._time, self._task_name))
        else:
            str_list.append("[%s] Excute task [%s] successful" % (self._time, self._task_name))
            str_list.append("Detail Info:")
            for status_info in self._status_infos:
                str_list.append("%s" % (status_info))
        file_handler.write("%s\n" % ("\n".join(str_list)).encode(encode)) 
        return 0        
    
    def get_process_name(self):
        """
        get all process class name
        """
        return []
    
    
    def __del__(self):
        """ delete """
        del self._task_name
        del self._json
        for t in self._status_infos:
            del t


#class TaskConf(object):
#    """task conf"""
#    def __init__(self, line):
#        """ 
#        Change line to taskconf object 
#
#        Args:
#            line: str 
#        
#        Exception:
#            ValueError
#        
#        """
#        line = line.rstrip("\n").strip()
#        self._conf_dict = {}
#        # load info in this line 
#        # split by ;
#        cols = line.split(";") 
#        for col in cols:
#            try:
#                key, value = col.split("=")
#            except ValueError as e:
#                raise ValueError("Can not split this part [%s] by [=] because of %s" % (col, e))
#            
#            if len(key.split("_")) > 1:
#                try:
#                    int(key.split("_")[-1])
#                except TypeError as e:
#                    self._conf_dict[key] = value
#                    continue
#           
#            key_suffix = key.split("_")[-1]
#            inner_key = "_".join(key.split("_")[0:-1])
#            if key_suffix not in self._conf_dict:
#                self._conf_dict[key_suffix] = {}
#            self._conf_dict[key_suffix][inner_key] = value
#        
#        # first split by "," then split by ":"
#        # "a:b,c,d:e" change to [[a,b],[c], [d, e]]
#        for key in self._conf_dict:
#            value = self._conf_dict[key]
#
#            if isinstance(self._conf_dict[key], str):
#                value = self.str_parser(value)
#                self._conf_dict[key] = value
#            else:
#                value_dict = self._conf_dict[key]
#
#
#
#    def str_parser(self, s):
#        """
#        Change str like "a:b,c,d:e" to [[a,b],[c],[d,e]]
#        
#        Args:
#            s: str 
#
#        Return:
#            list
#        """
#        item_list = s.split(",")
#        for index, item in enumerate(item_list):
#            item_list[index] = item.split(":")
#        return item_list
#
#    
#    def get_all_keys_with_dict(self):
#        """
#        return key list which value is a dict
#        
#        Args:
#
#        Return:
#            key list
#        """
#        keys = []
#        for key in self._conf_dict:
#            if isinstance(self._conf_dict[key], dict):
#                keys.append(key)
#        return keys
#    
#    
#    
#    def get_value_by_key(self, key):
#        """
#        get value by key
#
#        Args:
#            return 
#        
#        Exception:
#            KeyError: key does not exist
#            TypeError: type does not match
#        """
#        if key in self._conf_dict:
#            value = self._conf_dict[key]
#            if isinstance(value, list):
#                return value 
#            else:
#                raise TypeError("value for key [%s] is a dict instead of list" % (key))
#        else:
#            raise KeyError("key [%s] is not in dict" % (key))
#            
#
#    def get_dict_by_key(self, key):
#        """
#        get dict value by key
#
#        Args:
#            return 
#        
#        Exception:
#            KeyError: key does not exist
#            TypeError: type does not match
#        """
#        if key in self._conf_dict:
#            value = self._conf_dict[key]
#            if isinstance(value, dict):
#                return value 
#            else:
#                raise TypeError("value for key [%s] is a dict instead of list" % (key))
#        else:
#            raise KeyError("key [%s] is not in dict" % (key))
#    
#
#
#    def check_keys(self, key_list):
#        """
#        Check whether key is in self._conf_dict for every key in key_list
#        Args:
#            key_list: key or [key]
#
#        Return:
#            True
#            False
#        """
#        if not is_instance(key_list, list):
#            key_list = [key_list]
#
#        for key in key_list:
#            if key not in self._conf_dict:
#                logging.warning("Can not find key [%s]" % (key))
#                return False
#        
#            if is_instance(self._conf_dict[key], dict):
#                logging.warning("Value for this key [%s] is a dict instead of list" % (key))
#                return False
#        return True
#            
#    def __del__(self):
#        """delete"""
#        del self._conf_dict
#

