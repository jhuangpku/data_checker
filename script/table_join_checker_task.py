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
            super(TaskBase, self).__init__(line)
        except ValueError as e:
            raise TableJoinCheckerTaskInitError("%s" % (e))
        
        # 2. get info from task conf
        try:
            self._join_checker = self._task_conf.get_value_by_key("join_checker")[0][0]
        except (KeyError, ValueError, TypeError) as e:
            raise TableJoinCheckerTaskInitError("%s" % (e))
        
        try:
            self._join_checker_args = self._task_conf.get_value_by_key("join_checker_args")[0]
        except (KeyError, ValueError, TypeError) as e:
            self._join_checker_args = None
        
        # 3. only support two files now
        dics = []
        try:
            dic1 = self._task_conf.get_dict_by_key("1")
            dic2 = self._task_conf.get_dict_by_key("2")
        except KeyError:
            raise TableJoinCheckerTaskInitError("%s" % (e))
        dics.append(dic1)
        dics.append(dic2)
        
        try:
            self._files = [dic["filename"][0][0] for dic in dics]
            self._fields = [dic["field"] for dic in dics]
            self._decodes = [dic["decode"][0][0] for dic in dics]
            self._seps = [dic["sep"][0][0] for dic in dics]
            self._ratios = [dic["ratio"][0][0] for dic in dics]
        except (KeyError, ValueError, TypeError) as e:
            raise TableJoinCheckerTaskInitError("Init taskconf failed because of [%s]" % (e))
        
        for code in self._decodes: 
            try:
                "".decode(code)
            except LookupError as e:
                raise TableJoinCheckerTaskInitError("%s" % (e))
        
        for field in self._fields:
            if len(field) == 0:
                raise TableJoinCheckerTaskInitError("every item in fields must be a \
                                                    list with length >= 1 [%s]" %(str(self._fields)))
            try:
                field[0] = int(field[0])
            except TypeError as e:
                raise TableJoinCheckerTaskInitError("%s" % (e))
        
        for r in self._ratios:
            try:
                r = float(r)
            except TypeError:
                raise TableJoinCheckerTaskInitErrorr("%s" % (e))
            if r <= 0 or r > 1:
                raise TableJoinCheckerTaskInitError("Invalid sample ratio [%lf]") % (r))


        self._status_infos.append(StatusInfo(self._join_checker))


    def get_process_name(self):
        """return process_name"""
        names = [["join_checker", self._join_checker, self._join_checker_args]]
        for field in self._fields:
            if len(field) == 2:
                names.append(["preprocess", field[1], None])
            elif len(field) == 3:
                names.append(["preprocess", field[1], field[2]])
        return names


    def _get_values(self, process_manager, fields):
        """
        """
        values = []
        for field in fields:
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
        
        return values

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
        self._time = time.strftime("%Y%m%d %H:%M:%S", time.time())
        try:
            join_table_checker = process_manager.locate("join_table_checker", self._join_checker) 
        except ProcesserManagerLocateError as e:
            raise TableJoinCheckerTaskExcuteError("%s" % (e))
        
        file_dic = {}
        try:
            with open(self._files[0], "r") as f:
                for line in f:
                    line = line.rstrip("\n").decode(self._decodes[0])
                    cols = line.split(self._seps[0])
                    file_dic_key = self._get_values(process_manager, self._fields[0])
                    file_dic_key = tuple(file_dic_key)
                    file_dic[file_dic_key] = 0
        except IOError as e:
            raise TableJoinCheckerTaskExcuteError("%s" % (e))

        try:
            with open(self._files[1], "r") as f:
                for line in f:
                    cols = line.rstrip("\n").split(self._seps[1])
                    try:
                        file_dic_key = self._get_values(process_manager, self._fields[1])
                    except ProcesserManagerLocateError as e:
                        file_dic = {}
                        raise TableJoinCheckerTaskExcuteError("%s" % (e))
                    match_key = join_table_checker.check(file_dic, file_dic_key, self._join_checker_args) 
                    if match_key is not None:
                        file_dic_key[match_key] = 1
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
        status_info.expect_fail_cnt = status_info.check_cnt * (1 - self._ratio[1])
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
   
