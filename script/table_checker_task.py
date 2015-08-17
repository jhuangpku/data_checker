#!/usr/bin/env python
#coding:utf8
########################################################################
# 
# Copyright (c) 2015 4Paradigm.com, Inc. All Rights Reserved
# 
########################################################################
"""
File: table_checker_task.py
Author: huangjing(huangjing@4paradigm.com)
Date: 2015/08/06 09:14:45
Description: one task for data checker
"""

import sys
sys.path.append("util")
import time

import log

from task_base import TaskBase 
from task_base import StatusInfo
from task_base import TaskInitError
from task_base import TaskExcuteError
from process_manager import ProcesserManagerLocateError 

class TableCheckerTaskInitError(TaskInitError):
    """Init error for TableCheckerTask """

class TableCheckerTaskExcuteError(TaskExcuteError):
    """Excute error for TableCheckerTask """


class TableCheckerSubTask(object):
    """sub task for the total task"""
    def __init__(self, task_name, checker, args, fields):
        """init sub task
        
        Args:
            conf_dic: {name:value}, value is a list of list [[a,b,c],[b,c],[a]]
            task_name: task_name, str
        
        """
        self.task_name = task_name
        self.col_checker = checker
        self.args = args
        self.fields = fields
            
    def get_process_name(self):
        """
        Get process name 
        """
        names = self.fields.get_process_name()
        names.append(["col_checker", self.col_checker, self.args])
        return names

    
    def excute(self, process_manager, cols):
        """
        excute sub Task 

        Args:
            process_manager

        Return:
            0: successful
            1: fail 

        Exception:
            ProcesserManagerLocateError
        """
        try:
            table_checker = process_manager.locate("col_checker", self.col_checker)
        except ProcesserManagerLocateError as e:
            raise e
        try:
            values = self.fields.get_values(process_manager, cols)
        except ProcesserManagerLocateError as e:
            raise e
        return table_checker.check(values, self.args)
        

class TableCheckerTask(TaskBase):
    """ task conf for table checker"""
    def __init__(self, line):
        """
        Change line to taskconf object

        Args:
            line: str

        Exception:
            TableCheckerTaskInitError
        """
        line = line.rstrip("\n")
        # 1. load json
        try:
            super(TableCheckerTask, self).__init__(line) 
        except ValueError as e:
            raise TableCheckerTaskInitError("%s" % (e))

        # 2. get info from json 
        try:
            self._f_name  = self.get_attribute("filename", self._json, unicode)
            self._decode  = self.get_attribute("decode",   self._json, "code")
            self._col_cnt = self.get_attribute("col_cnt",  self._json, int)
            self._sep     = self.get_attribute("sep",      self._json, unicode)
            sub_tasks = self.get_attribute("col_check_task", self._json, list)
        except (KeyError, ValueError, TypeError) as e:
            raise TableCheckerTaskInitError("%s" % (e))
        
        self._status_infos.append(StatusInfo("col_cnt_checker"))
        
        # 3. get sub_task
        self._sub_tasks = []
        for sub_task_dic in sub_tasks:
            try:
                checker = self.get_attribute("col_checker", sub_task_dic, unicode)
                fields  = self.get_attribute("fields", sub_task_dic, "fields")
                args    = self.get_attribute("args", sub_task_dic, "args")
            except (KeyError, ValueError, TypeError) as e:
                raise TableCheckerTaskInitError("%s" % (e))
            
            sub_task = TableCheckerSubTask(str(sub_task_dic), checker, args, fields)
            self._sub_tasks.append(sub_task)
            self._status_infos.append(StatusInfo(sub_task.task_name))


    def get_process_name(self):
        """
        get all process class name
        """
        names = []
        for sub_task in self._sub_tasks:
            names.extend(sub_task.get_process_name())
        return names
    
    
    def excute(self, process_manager):
        """
        begin to excute
        
        Args:
            None 

        Return:
            0 
        Exception:
            TableCheckerTaskExcuteError
        """
        self._time = time.strftime("%Y%m%d %H:%M:%S", time.localtime())
        try:
            with open(self._f_name, "r") as f:
                for line in f:
                    line = line.rstrip("\n").decode(self._decode)
                    cols = line.split(self._sep)
                    status_info = self._status_infos[0]
                    status_info.check_cnt += 1
                    if len(cols) != self._col_cnt:
                        # append this line into debug info 
                        status_info.check_fail_cnt += 1
                        if len(status_info.check_fail_cnt < 10):
                            status_info.fail_info.append(["col_cnt does not match \
                                                    expect [%d] actual [%d]" % \
                                                         (self._col_cnt, len(cols)), \
                                                          line.rstrip("\n")])
                    for index, sub_task in enumerate(self._sub_tasks):
                        try:
                            ret = sub_task.excute(process_manager, cols)
                        except ProcesserManagerLocateError as e:
                            raise TableCheckerTaskExcuteError("%s" % (e))
                        status_info = self._status_infos[index + 1]
                        status_info.check_cnt += 1
                        if ret != 0:
                            status_info.check_fail_cnt += 1
                            if len(status_info.check_fail_cnt < 10):
                                status_info.fail_info.append([sub_task.task_name, line.rstrip("\n")])
                    
        except IOError as e:
            raise TableCheckerTaskExcuteError("%s" % (e)) 
            return 1
        self._if_success = True
        return 0
    
    def write_status(self, fstream, encode):
        """
        write excute info into fstream
        Args:
            fstream: output fstream
        Return:
            0
        """
        return super(TableCheckerTask, self).write_status(fstream, encode) 
         
