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


class TaskInitError(Exception):
    """init task error"""

class TaskExcuteError(Exception):
    """excute task error"""


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
            str_list.append("Check task [%s] failed") % (self.check_name) 
        str_list.append("Check [%d] lines, failed [%d] lines, expect failed [%d] lines" % (self.check_cnt, self.check_fail_cnt, self.expect_fail_cnt))
        str_list.append("Top Failed lines:")
        for (reason, line) in self.fail_info:
            str_list.append("[%s]:line [%s]" % (reason, line))
        return "------" + "\n------".join(str_list)


class TaskConf(object):
    """task conf"""
    def __init__(self, line):
        """ 
        Change line to taskconf object 

        Args:
            line: str 
        
        Exception:
            ValueError
        
        """
        line = line.rstrip("\n").strip()
        self._conf_dict = {}
        # load info in this line 
        # split by ;
        cols = line.split(";") 
        for col in cols:
            try:
                key, value = col.split("=")
            except ValueError as e:
                raise ValueError("Can not split this part [%s] by [=] because of %s" % (col, e))
            
            if len(key.split("_")) > 1:
                try:
                    int(key.split("_")[-1])
                except TypeError as e:
                    self._conf_dict[key] = value
                    continue
           
            key_suffix = key.split("_")[-1]
            inner_key = "_".join(key.split("_")[0:-1])
            if key_suffix not in self._conf_dict:
                self._conf_dict[key_suffix] = {}
            self._conf_dict[key_suffix][inner_key] = value
        
        # first split by "," then split by ":"
        # "a:b,c,d:e" change to [[a,b],[c], [d, e]]
        for key in self._conf_dict:
            value = self._conf_dict[key]

            if isinstance(self._conf_dict[key], str):
                value = self.str_parser(value)
                self._conf_dict[key] = value
            else:
                value_dict = self._conf_dict[key]



    def str_parser(self, s):
        """
        Change str like "a:b,c,d:e" to [[a,b],[c],[d,e]]
        
        Args:
            s: str 

        Return:
            list
        """
        item_list = s.split(",")
        for index, item in enumerate(item_list):
            item_list[index] = item.split(":")
        return item_list

    
    def get_all_keys_with_dict(self):
        """
        return key list which value is a dict
        
        Args:

        Return:
            key list
        """
        keys = []
        for key in self._conf_dict:
            if isinstance(self._conf_dict[key], dict):
                keys.append(key)
        return keys
    
    
    
    def get_value_by_key(self, key):
        """
        get value by key

        Args:
            return 
        
        Exception:
            KeyError: key does not exist
            TypeError: type does not match
        """
        if key in self._conf_dict:
            value = self._conf_dict[key]
            if isinstance(value, list):
                return value 
            else:
                raise TypeError("value for key [%s] is a dict instead of list" % (key))
        else:
            raise KeyError("key [%s] is not in dict" % (key))
            

    def get_dict_by_key(self, key):
        """
        get dict value by key

        Args:
            return 
        
        Exception:
            KeyError: key does not exist
            TypeError: type does not match
        """
        if key in self._conf_dict:
            value = self._conf_dict[key]
            if isinstance(value, dict):
                return value 
            else:
                raise TypeError("value for key [%s] is a dict instead of list" % (key))
        else:
            raise KeyError("key [%s] is not in dict" % (key))
    


    def check_keys(self, key_list):
        """
        Check whether key is in self._conf_dict for every key in key_list
        Args:
            key_list: key or [key]

        Return:
            True
            False
        """
        if not is_instance(key_list, list):
            key_list = [key_list]

        for key in key_list:
            if key not in self._conf_dict:
                logging.warning("Can not find key [%s]" % (key))
                return False
        
            if is_instance(self._conf_dict[key], dict):
                logging.warning("Value for this key [%s] is a dict instead of list" % (key))
                return False
        return True
            
    def __del__(self):
        """delete"""
        del self._conf_dict


class TaskBase(object):
    """Base for all kinds of task"""
    def __init__(self, line):
        """init function
        Args: 
            line: task command line 
        """
        line = line.rstrip("\n")
        self._task_name = line
        try:
            self._task_conf = TaskConf(line)
        except ValueError as e:
            raise e
        self._status_infos = []
        self._if_success = False
        self._time = time.strftime("%Y%m%d %H:%M:%S", time.time())

    def excute(self, checker_manager):
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
        if self._if_success is False:
            str_list.append("[%s] Excute task [%s] failed" % (self._time, self._task_name))
        else:
            str_list.append("[%s] Excute task [%s] successful" % (self._time, self._task_name))
            str_list.append("Detail Info:")
            for status_info in self._status_infos:
                str_list.append("%s" % (status_info))
        file_handler.write("%s" % ("\n".join(str_list)).encode(encode)) 
        return 0        
    
    def get_process_name(self):
        """
        get all process class name
        """
        return []
    
    
    def __del__(self):
        """ delete """
        del self._task_name
        del self._task_conf
        for t in self._status_infos:
            del t
