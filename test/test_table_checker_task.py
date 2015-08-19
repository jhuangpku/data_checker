#!/usr/bin/env python
#coding:utf8
########################################################################
# 
# Copyright (c) 2015 4Paradigm.com, Inc. All Rights Reserved
# 
########################################################################
"""
File: test_table_checker_task.py
Author: huangjing(huangjing@4paradigm.com)
Date: 2015/08/18 09:51:56
Description: table checker
"""
import sys
sys.path.append("../script/")
sys.path.append("../script/util/")
sys.path.append("../script/lib/col_check/")
sys.path.append("../script/lib/preprocess/")
sys.path.append("../script/lib/join_check/")
import unittest
import json

from task_base import FieldList 
from process_manager import ProcesserManager
from process_manager import ProcesserManagerInitError
from process_manager import ProcesserManagerLocateError
from table_checker_task import TableCheckerSubTask
from table_checker_task import TableCheckerTask
#from data_sampler_task import DataSamplerTask
#from data_sampler_task import DataSamplerTaskInitError 
#from data_sampler_task import DataSamplerTaskExcuteError

def assemble_field_list(field_args):
    """ assemble field list """
    json_dics = []
    for (a, b, c) in field_args:
        dic = {}
        dic["field_no"] = a
        if b is not None:
            dic["processer"] = b
        if c is not None:
            dic["args"] = c
        json_dics.append(dic)
    return json_dics
    
class TestTableCheckerSubTask(unittest.TestCase):
    """ test for table checker """
    def setUp(self):
        """ init a great process manager"""
        self._p_m = ProcesserManager() 
        self._p_m.init_class("col_checker", "col_checker_time.ColTimeChecker")
        self._p_m.init_class("col_checker", "col_checker_type.ColTypeChecker")
        self._p_m.init_class("preprocesser", "preprocess_time.PreprocessTime")
        self._p_m.init_class("preprocesser", "preprocess_trim.PreprocessTrim")
        #self._p_m.init_class("join_checker", "") 
   

    def tearDown(self):
        del self._p_m

    def test_get_process_name(self):
        """ test for get process name """
        task_name = "test1"
        checker = "test1_checker"
        args = ["1", "2"]
        field_args = [ \
                      ["1", None, ["C1"]],\
                      ["2", None, None],\
                      ["3", "P3", None],\
                      ["4", "P4", ["C4"]],\
                      ["5", "P5", ["C5.1", "C5.2"]]\
                      ]
        
        fields = FieldList(assemble_field_list(field_args))
        table_checker = TableCheckerSubTask(task_name, checker, args, fields)
        names = table_checker.get_process_name() 
        real_names = [["preprocesser", p[1], p[2]] for p in field_args if p[1] is not None]
        real_names.append(["col_checker", checker, args])
        # check process name              
        for name in names:
            self.assertIn(name, real_names)
    
    def test_excute_type_checker(self):
        """test for excute"""
        
        task_name = "test1"
        checker = "col_checker_type.ColTypeChecker"
        args = [u"int"]
        field_args = [["0", None, None]]
        fields = FieldList(assemble_field_list(field_args))
        table_checker = TableCheckerSubTask(task_name, checker, args, fields) 
        # 1. check for empty pm
        p_manager = ProcesserManager()
        self.assertRaises(ProcesserManagerLocateError, table_checker.excute, p_manager, [])
        
        cols = ["13"]
        self.assertTrue(table_checker.excute(self._p_m, cols))
        cols = ["dsj13"]
        self.assertFalse(table_checker.excute(self._p_m, cols))
        
        field_args = [["0", "preprocess_trim.PreprocessTrim", None]]
        fields = FieldList(assemble_field_list(field_args))
        table_checker = TableCheckerSubTask(task_name, checker, args, fields) 
        cols = ["13"]
        self.assertTrue(table_checker.excute(self._p_m, cols))
        cols = ['  13  "']
        self.assertTrue(table_checker.excute(self._p_m, cols))
        cols = ['"13df"']
        self.assertFalse(table_checker.excute(self._p_m, cols))

        args = [u"int", u"float"]
        table_checker = TableCheckerSubTask(task_name, checker, args, fields) 
        cols = ['"13.3"']
        self.assertTrue(table_checker.excute(self._p_m, cols))

    def test_excute_time_checker(self):
        """ test for excute time checker """

        task_name = "test2"
        checker = "col_checker_time.ColTimeChecker"
        
        args = [u"%Y%m%d"]
        field_args = [["0", None, None]]
        fields = FieldList(assemble_field_list(field_args))
        table_checker = TableCheckerSubTask(task_name, checker, args, fields) 
        cols = ["20150302"]
        self.assertTrue(table_checker.excute(self._p_m, cols))
        cols = ["dsj13"]
        self.assertFalse(table_checker.excute(self._p_m, cols))
        
        args = [u"%Y-%m-%d %H:%M:%S"]
        field_args = [["0", "preprocess_time.PreprocessTime", [u"%Y-%m-%d"]]]
        fields = FieldList(assemble_field_list(field_args))
        table_checker = TableCheckerSubTask(task_name, checker, args, fields) 
        cols = ["2015-03-02"]
        self.assertTrue(table_checker.excute(self._p_m, cols))
        cols = ["20150302"]
        self.assertFalse(table_checker.excute(self._p_m, cols))



def assemble_sub_task(checker, args, fields):
    dic = {}
    dic["fields"] = assemble_field_list(fields)
    if args is not None:
        dic["args"] = args
    dic["col_checker"] = checker
    return dic

def assemble_parameter(filename, col_cnt, sep, col_check_task):
    dic = {}
    dic["filename"] = filename
    dic["decode"] = "utf8"
    dic["col_cnt"] = col_cnt
    dic["sep"] = sep
    dic["col_check_task"] = col_check_task
    return json.dumps(dic) + "\n"

class TestTableCheckerTask(unittest.TestCase):
    """ test for table checker """
    def setUp(self):
        """ init a great process manager"""
        self._p_m = ProcesserManager() 
        self._p_m.init_class("col_checker", "col_checker_time.ColTimeChecker")
        self._p_m.init_class("col_checker", "col_checker_type.ColTypeChecker")
        self._p_m.init_class("preprocesser", "preprocess_time.PreprocessTime")
        self._p_m.init_class("preprocesser", "preprocess_trim.PreprocessTrim")
        #self._p_m.init_class("join_checker", "") 
        
        col_check_task = []
        col_check_task.append(assemble_sub_task("col_checker_time.ColTimeChecker", \
                                                ["%Y-%m-%d %H:%M:%S"], [["0", None, None]]))
        col_check_task.append(assemble_sub_task("col_checker_type.ColTypeChecker", \
                                                ["int"], [["1", None, None]]))
        col_check_task.append(assemble_sub_task("col_checker_type.ColTypeChecker", \
                                                ["int"], [["2", "preprocess_trim.PreprocessTrim", None]]))
        col_check_task.append(assemble_sub_task("col_checker_type.ColTypeChecker", \
                                                ["float"], [["3", "preprocess_trim.PreprocessTrim", None]]))
        col_check_task.append(assemble_sub_task("col_checker_time.ColTimeChecker", \
                                                ["%Y-%m-%d %H:%M:%S"], [["4", "preprocess_time.PreprocessTime", None]]))
        col_check_task.append(assemble_sub_task("col_checker_time.ColTimeChecker", \
                                                ["%Y-%m-%d %H:%M:%S"], [["5", "preprocess_time.PreprocessTime", ["%Y:%M:%d"]]]))
        
        line = assemble_parameter("./data/table_checker/file_in", 6, ",", col_check_task)
        self._checker = TableCheckerTask(line)
        
    def tearDown(self):
        del self._p_m
        del self._checker


    def test_get_process_name(self):
        """ test for get process name """
        names = self._checker.get_process_name() 
        real_names = [
            ["col_checker", "col_checker_time.ColTimeChecker", ["%Y-%m-%d %H:%M:%S"]], \
            ["col_checker", "col_checker_type.ColTypeChecker", ["int"]], \
            ["col_checker", "col_checker_type.ColTypeChecker", ["int"]], \
            ["col_checker", "col_checker_type.ColTypeChecker", ["float"]], \
            ["col_checker", "col_checker_time.ColTimeChecker", ["%Y-%m-%d %H:%M:%S"]], \
            ["col_checker", "col_checker_time.ColTimeChecker", ["%Y-%m-%d %H:%M:%S"]], \
            ["preprocesser" , "preprocess_trim.PreprocessTrim",  None], \
            ["preprocesser" , "preprocess_trim.PreprocessTrim",  None], \
            ["preprocesser" , "preprocess_time.PreprocessTime",  None], \
            ["preprocesser" , "preprocess_time.PreprocessTime",  ["%Y:%M:%d"]], \
            ]
        for name in names:
            self.assertIn(name, real_names)

    def test_excute(self):
        self._checker.excute(self._p_m)
        status_infos = self._checker._status_infos
        line_cnt = 10
        reals = [8, 9, 9, 9, 9, 10, 10]
        for index, s in enumerate(status_infos):
            self.assertEqual(s.check_cnt, line_cnt)
            self.assertEqual(s.check_fail_cnt, line_cnt - reals[index])


if __name__ == '__main__':
    unittest.main()

