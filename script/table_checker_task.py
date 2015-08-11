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


class TableCheckerTaskInitError(TaskInitError):
    """Init error for TableCheckerTask """

class TableCheckerTaskExcuteError(TaskExcuteError):
    """Excute error for TableCheckerTask """


class TableCheckerSubTask(object):
    """sub task for the total task"""
    def __init__(self, task_name, conf_dic):
        """init sub task
        
        Args:
            conf_dic: {name:value}, value is a list of list [[a,b,c],[b,c],[a]]
            task_name: task_name, str
        
        """
        try:
            self.col_checker = conf_dic["col_checker"][0][0]
            self.fields = conf_dic["fields"]
        except (KeyError, ValueError, TypeError) as e:
            raise e 
       
        try:
            self.col_checker_args = conf_dic["col_checker_args"][0]
        except KeyError:
            self.col_checker_args = None
        
        self.task_name = "Checker_no=%s checker=%s" % (task_name, self.col_checker)

        # check arguments:
        # [[field_no, process_fun, process_args],[]]
        for field in self.fields:
            if len(field) == 0:
                raise ValueError("every item in fields must be a list with length >= 1 [%s]" \
                                 % (str(self.fields)))
            try:
                field[0] = int(field[0])
            except TypeError as e:
                raise e

    def get_process_name(self):
        """
        Get process name 
        """
        names = []
        names.append(["col_checker", self.col_checker, self.col_checker_args])
        for field in self.fields:
            if len(field) == 2:
                names.append(["preprocess", field[1], None])
            elif len(field) == 3:
                names.append(["preprocess", field[1], field[2]])
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
        
        values = []
        for field in self.fields:
            n = len(field)
            field_no = field[0]
            value = cols[field_no]
            args = None
            if n >= 2:
                try:
                    preprocess_class = process_manager.locate("preprocess", field[1])
                except ProcesserManagerLocateError as e:
                    raise e
                if n >= 3:
                    args = field[2]
                value = preprocess_class.preprocess(value, args)
            values.append(value)
        
        return table_checker.check(values, self.col_checker_args)


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
        # 1. load task conf
        try:
            super(TaskBase, self).__init__(line) 
        except ValueError as e:
            raise TableCheckerTaskInitError("%s" % (e))

        # 2. get info from task conf 
        try:
            self._f_name = self._task_conf.get_value_by_key("filename")[0][0]
            self._decode = self._task_conf.get_value_by_key("decode")[0][0]
            self._col_cnt = int(self._task_conf.get_value_by_key("cnt")[0][0])
            self._sep     = self._task_conf.get_value_by_key("sep")[0][0]
        except (KeyError, ValueError, TypeError) as e:
            raise TableCheckerTaskInitError("%s" % (e))
        
        try:
            "".decode(self._decode)
        except LookupError as e:
            raise TableCheckerTaskInitError("%s" % (e))

        self._status_infos.append(StatusInfo("col_cnt_checker"))
        # get sub_task
        self._sub_tasks = []
        sub_task_keys = self._task_conf.get_all_keys_with_dict()
        for key in sub_task_keys:
            sub_task_value = self._task_conf.get_dict_by_key(key)
            try:
                sub_task = TableCheckerSubTask(key, sub_task_value) 
            except (KeyError, ValueError, TypeError) as e:
                raise TableCheckerTaskInitError("sub_task [%s] failed because of [%s]" \
                                                % (str(sub_task_value), e))
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
    
    
    def excute(self, process_manager)
        """
        begin to excute
        
        Args:
            None 

        Return:
            0 
        Exception:
            TableCheckerTaskExcuteError
        """
        self._time = time.strftime("%Y%m%d %H:%M:%S", time.time())
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
        return super(TaskBase, self).write_status(fstream, encode) 
         
