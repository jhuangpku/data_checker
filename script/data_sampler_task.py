#!/usr/bin/env python
#coding:utf8
########################################################################
# 
# Copyright (c) 2015 4Paradigm.com, Inc. All Rights Reserved
# 
########################################################################
"""
File: data_sampler_task.py
Author: huangjing(huangjing@4paradigm.com)
Date: 2015/08/05 15:06:26
Description: one task for data sampler 
"""

import sys
import random
import time

from task_base import TaskBase
from task_base import StatusInfo
from task_base import TaskInitError
from task_base import TaskExcuteError

class DataSamplerTaskInitError(TaskInitError):
    """ init error for DataSamplerTask """


class DataSamplerTaskExcuteError(TaskExcuteError):
    """ init error for DataSamplerTask """



class DataSamplerTask(TaskBase):
    """ task conf for sample data """
    def __init__(self, line):
        """
        Change line to taskconf object

        Args:
            line: str

        Exception:
            DataSamplerTaskInitError
        """
        line = line.rstrip("\n")
        # 1. load task conf
        try:
            super(TaskBase, self).__init__(line) 
        except ValueError as e:
            raise DataSamplerTaskInitError("%s" % (e))

        # 2. get info from task conf 
        try:
            self._f_in_name  = self._task_conf.get_value_by_key("input_file")[0][0]
            self._f_out_name = self._task_conf.get_value_by_key("output_file")[0][0]
            self._ratio      = self._task_conf.get_value_by_key("ratio")[0][0]
            self._sep        = self._task_conf.get_value_by_key("sep")[0][0]
            self._encode     = self._task_conf.get_value_by_key("encode")[0][0]
            self._decode     = self._task_conf.get_value_by_key("decode")[0][0]
        except (KeyError, ValueError, TypeError) as e:
            raise DataSamplerTaskInitError("%s" % (e))
        
        # 3. check argument
        try:
            self._ratio = float(self._ratio)
        except TypeError as e:
            raise DataSamplerTaskInitError("%s" % (e))

        if self._ratio <= 0 or self._ratio > 1:
            raise DataSamplerTaskInitError("sample_ratio [%lf] must be in (0, 1]" % (self._ratio))
        
        try:
            "".encode(self._encode)
            "".decode(self._decode)
        except LookupError as e:
            raise DataSamplerTaskInitError("%s" % (e))
        
        
        # 4. init default args
        self._buf_size = 512
        self._stauts_infos.append(StatusInfo(self._task_name))
        
        def excute(self, process_manager = None):
            """
            begin to sample
            
            Args:
                None 

            Return:
                0 

            Exception:
                DataSamplerTaskExcuteError
            """
            self._time = time.strftime("%Y%m%d %H:%M:%S", time.time())
            f_out = open(self._f_out_name, "w")
            status = self._status_infos[0]
            try:
                with open(self._f_in_name, "r") as f:
                    buf = f.read(self._buf_size)
                    buf = buf.decode(self._decode)
                    last_line = u""
                    while (buf):
                        buf = last_line + buf
                        lines = buf.split(self._sep)
                        #print lines
                        #print "aa", lines
                        for l in lines[0:-1]:
                            status.check_cnt += 1
                            r = random.random()
                            if r < self._ratio:
                                l = l.replace(u"\n", u"\\n")
                                f_out.write("%s\n" % (l.encode(self._encode)))
                            else:
                                status.check_fail_cnt += 1
                        buf = self._f_in.read(self._buf_size)
                        buf = buf.decode(self._decode)
                        last_line = lines[-1]
                    
                    if last_line != "" and last_line != "\n":
                        status.check_cnt += 1
                        r = random.random()
                        if r < self._ratio:
                            l = l.replace(u"\n", u"\\n")
                            f_out.write("%s\n" % (last_line.encode(self._encode)))
                        else:
                            status.check_fail_cnt += 1
            except IOError as e:
                f_out.close()
                raise DataSamplerTaskExcuteError("%s" % (e))
            
            status.expect_fail_cnt = int(status.check_cnt * (1 - self._ratio))
            self._if_success = True
            f_out.close()
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
