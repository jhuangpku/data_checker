#!/usr/bin/env python
#coding:utf8
########################################################################
# 
# Copyright (c) 2015 4Paradigm.com, Inc. All Rights Reserved
# 
########################################################################
"""
File: task_manager.py
Author: huangjing(huangjing@4paradigm.com)
Date: 2015/08/11 15:43:45
Description: task manager class
"""
import sys
import logging
sys.path.append("script/util")

import log

from task_base import TaskInitError
from task_base import TaskExcuteError
from process_manager import ProcesserManager
from process_manager import ProcesserManagerInitError
from data_sampler_task import DataSamplerTask
from table_checker_task import TableCheckerTask 
from table_join_checker_task import TableJoinCheckerTask


class TaskManagerInitError(Exception):
    """ init TaskManager error """

class TaskManagerBase(object):
    """task manager base"""
    def __init__(self, task_file, command, status_file, status_encode):
        """
        init task manager

        Args:
            task_file: task_file
            command:command_str
            status_file:status_file
        
        Exception:
            TaskManagerInitError
        """
        # 1.init status file
        self._status_file = None
        try:
            self._status_file = open(status_file, "a")
        except IOError as e:
            self._status_file = sys.stderr
            #raise TaskManagerInitError("%s" % (e))
        
        # 2.init status_code
        self._status_encode = status_encode
        try:
            "".encode(status_encode)
        except LookupError as e:
            raise TaskManagerInitError("%s" % (e))
        
        # 3.init command 
        if command != "":
            self._commands = [command]
        else:
            try:
                f = open(task_file, "r")
            except IOError as e:
                raise TaskManagerInitError("%s" % (e))
            self._commands = f.readlines() 
        
        # 4.init task
        try:
            self._tasks = self._get_tasks()
        except TaskInitError as e:
            raise TaskManagerInitError("%s" % (e))
        
        # 5.init process_manager
        try:
            self._process_manager = self._get_process_manager()
        except ProcesserManagerInitError as e:
            raise TaskManagerInitError("%s" % (e))
    
    def _get_tasks(self):
        """
        init all task according to self._commands
        
        return 
            [task]
        
        Exception:
            TaskInitError
        """
        return []
    
    def _get_process_manager(self):
        """
        init all task according to self._commands
        
        return 
            ProcesserManager 
        
        Exception:
            ProcesserManagerInitError
        """
        process_manager = ProcesserManager()
        process_names = []
        for task in self._tasks:
            process_name = task.get_process_name()
            process_names.extend(process_name)
        
        for (module, class_name, args) in process_names:
            try:
                class_obj = process_manager.init_class(module, class_name)
            except ProcesserManagerInitError as e:
                logging.warning("Init class_obj [%s] module[%s] failed" % (class_name, module))
                raise e
            logging.info("Init class_obj [%s] module[%s] successful" % (class_name, module))
            ret = class_obj.check_args(args)
            if ret != 0:
                logging.warning("Check args [%s] for class_obj [%s] module [%s] failed" % \
                                (str(args), class_name, module))
                raise ProcesserManagerInitError("Check args [%s] for class_obj [%s] module [%s] failed" % \
                                                (str(args), class_name, module))
            logging.info("Check args [%s] for class_obj [%s] module [%s] successful" % \
                                                (str(args), class_name, module))

        return process_manager
    
    def excute(self):
        """
        excute all tasks 
        
        Return:
            0: every task excute successful
            1: at least 1 task excute failed
        
        """
        ret = 0 
        for task in self._tasks:
            try:
                task.excute(self._process_manager)
            except TaskExcuteError as e:
                logging.warning("Excute task [%s] failed because of [%s]" % (task._task_name, e))
                task.write_status(self._status_file, self._status_encode)
                ret = 1
                continue    
            task.write_status(self._status_file, self._status_encode)
            logging.info("Excute task [%s] successful" % (task._task_name))
        return ret


    def __del__(self):
        """del"""
        if self._status_file is not None:
            self._status_file.close()
        for t in self._tasks:
            del t
        del self._process_manager



class DataSamplerManager(TaskManagerBase):
    "manager for data sampler"
    def _get_tasks(self):
        """
        init all task according to self._commands
        
        return 
            [task]
        
        Exception:
            TaskInitError
        """
        tasks = []
        for command in self._commands:
            try:
                task = DataSamplerTask(command)
            except TaskInitError as e:
                logging.warning("Init task with command [%s] failed because of [%s]" % (command, e))
                raise e
            logging.info("Init task with command [%s] successful" % (command))
            tasks.append(task)
        return tasks    
    
class TableCheckerManager(TaskManagerBase):
    "manager for table checker"
    def _get_tasks(self):
        """
        init all task according to self._commands
        
        return 
            [task]
        
        Exception:
            TaskInitError
        """
        tasks = []
        for command in self._commands:
            try:
                task = TableCheckerTask(command)
            except TaskInitError as e:
                logging.warning("Init task with command [%s] failed because of [%s]" % (command, e))
                raise e
            logging.info("Init task with command [%s] successful" % (command))
            tasks.append(task)
        return tasks
       
class TableJoinCheckerManager(TaskManagerBase):
    "manager for table checker"
    def _get_tasks(self):
        """
        init all task according to self._commands
        
        return 
            [task]
        
        Exception:
            TaskInitError
        """
        tasks = []
        for command in self._commands:
            try:
                task = TableJoinCheckerTask(command)
            except TaskInitError as e:
                logging.warning("Init task with command [%s] failed because of [%s]" % (command, e))
                raise e
            logging.info("Init task with command [%s] successful" % (command))
            tasks.append(task)
        return task



