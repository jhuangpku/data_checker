#!/usr/bin/env python
#coding:utf8
########################################################################
# 
# Copyright (c) 2015 4Paradigm.com, Inc. All Rights Reserved
# 
########################################################################
"""
File: table_join_checker_task.py
Author: huangjing(huangjing@4paradigm.com)
Date: 2015/08/07 13:33:37
Description: one task for table join checker
"""

import sys
sys.path.append("util")
import log
import time 

from task_base import TaskBase
from task_base import StatusInfo
from task_base import TaskInitError
from task_base import TaskExcuteError

from process_manager import ProcesserManagerLocateError

class TableJoinCheckerTaskInitError(TaskInitError):
    """Init error for TableJoinCheckerTask"""

class TableJoinCheckerTaskExcuteError(TaskExcuteError):
    """Excute error for TableJoinCheckerTask"""



class TableJoinCheckerTask(TaskBase):
    """task conf for table join checker"""
    def __init__(self, line):
        """
        Change line to taskconf object

        Args:
            line: str

        Exception:
            TableJoinCheckerTaskInitError
        """
        line = line.rstrip("\n")
        # 1. load task conf
        try:
            super(TableJoinCheckerTask, self).__init__(line)
        except ValueError as e:
            raise TableJoinCheckerTaskInitError("%s" % (e))
        
        # 2. get info from task conf
        try:
            self._join_checker = self.get_attribute("join_checker", self._json, unicode)
            self._join_checker_args = self.get_attribute("join_checker_args", self._json, "args")
            file_dics = self.get_attribute("files", self._json, list)
        except (KeyError, ValueError, TypeError) as e:
            raise TableJoinCheckerTaskInitError("%s" % (e))
        
        # 3. parser files
        self._files = []
        self._fields = []
        self._decodes = []
        self._seps = []
        self._ratios = []
        for file_dic in file_dics:
            try:
                file   = self.get_attribute("filename", file_dic, unicode)
                fields = self.get_attribute("fields", file_dic, "fields")
                decode = self.get_attribute("decode", file_dic, "code")
                sep    = self.get_attribute("sep"   , file_dic, unicode)
                ratio  = self.get_attribute("ratio", file_dic, "ratio")
            except (KeyError, ValueError, TypeError) as e:
                raise TableJoinCheckerTaskInitError("Init taskconf failed because of [%s]" % (e))
            self._files.append(file)  
            self._fields.append(fields)
            self._decodes.append(decode)
            self._seps.append(sep)
            self._ratios.append(ratio)
        
        self._status_infos.append(StatusInfo(self._join_checker))


    def get_process_name(self):
        """return process_name"""
        names = [["join_checker", self._join_checker, self._join_checker_args]]
        for fields in self._fields:
            names.extend(fields.get_process_name())
        return names


    def excute(self, process_manager):
        """
        begin to excute 

        Args:
            process_manager 

        Return:
            0
        Exception:
            TableJoinCheckerTaskExcuteError
        """
        self._time = time.strftime("%Y%m%d %H:%M:%S", time.localtime())
        try:
            join_table_checker = process_manager.locate("join_checker", self._join_checker) 
        except ProcesserManagerLocateError as e:
            raise TableJoinCheckerTaskExcuteError("%s" % (e))
        file_dic = {}
        try:
            with open(self._files[0], "r") as f:
                for line in f:
                    line = line.rstrip("\n").decode(self._decodes[0])
                    cols = line.split(self._seps[0])
                    file_dic_key = self._fields[0].get_values(process_manager, cols)
                    file_dic_key = tuple(file_dic_key)
                    file_dic[file_dic_key] = 0
        except IOError as e:
            raise TableJoinCheckerTaskExcuteError("%s" % (e))
       

        try:
            with open(self._files[1], "r") as f:
                for line in f:
                    cols = line.rstrip("\n").split(self._seps[1])
                    try:
                        file_dic_key = self._fields[1].get_values(process_manager, cols)
                    except ProcesserManagerLocateError as e:
                        file_dic = {}
                        raise TableJoinCheckerTaskExcuteError("%s" % (e))
                    match_key = join_table_checker.check(file_dic, tuple(file_dic_key), self._join_checker_args) 
                    if match_key is not None:
                        file_dic[match_key] = 1
        except IOError as e:
            file_dic = {}
            raise TableJoinCheckerTaskExcuteError("%s" % (e))
        

        status_info = self._status_infos[0]
        for key in file_dic:
            status_info.check_cnt += 1
            if file_dic[key] == 0:
                status_info.check_fail_cnt += 1
                if status_info.check_fail_cnt < 10:
                    status_info.fail_info.append(["Can not find match key", key])
        file_dic = {} 
        # according to sample_ratio to see if it is a valid match 
        status_info.expect_fail_cnt = status_info.check_cnt * (1 - self._ratios[1])
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
        return super(TableJoinCheckerTask, self).write_status(fstream, encode) 
   
