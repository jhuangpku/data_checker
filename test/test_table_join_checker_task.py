#!/usr/bin/env python
#coding:utf8
########################################################################
# 
# Copyright (c) 2015 4Paradigm.com, Inc. All Rights Reserved
# 
########################################################################
"""
File: test_table_join_checker_task.py
Author: huangjing(huangjing@4paradigm.com)
Date: 2015/08/19 10:31:22
Description:test for table join checker
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
from table_join_checker_task import TableJoinCheckerTask

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
 
def assemble_parameter(join_checker, args, file1, file2, fields1, fields2):
    """ assemble input parameters """
    dic = {}
    dic["join_checker"] = join_checker
    if args is not None:
        dic["args"] = args
    file_dic1 = {}
    file_dic1["filename"] = file1
    file_dic1["fields"] = assemble_field_list(fields1)
    file_dic1["decode"] = "utf8"
    file_dic1["sep"] = ","
    file_dic1["ratio"] = "0.5"
    
    file_dic2 = {}
    file_dic2["filename"] = file2
    file_dic2["fields"] = assemble_field_list(fields2)
    file_dic2["decode"] = "utf8"
    file_dic2["sep"] = "\t"
    file_dic2["ratio"] = "0.5"

    dic["files"] = [file_dic1, file_dic2]
    
    return json.dumps(dic) + "\n"

class TestTableJoinCheckerTask(unittest.TestCase):
    """ test for table join checker """
    def setUp(self):
        """ init a great process manager"""
        self._p_m = ProcesserManager() 
        self._p_m.init_class("preprocesser", "preprocess_time.PreprocessTime")
        self._p_m.init_class("preprocesser", "preprocess_trim.PreprocessTrim")
        self._p_m.init_class("join_checker", "join_checker_equal.EqualJoinChecker") 
    
    def tearDown(self):
        del self._p_m

    def test_get_process_name(self):
        """ test for get process name"""
        join_checker = "join_checker"
        args = ["1", "2"]
        file1 = "file1"
        file2 = "file2"
        fields1 = [ \
                      ["1", None, ["C1"]],\
                      ["2", None, None],\
                      ["3", "P3", None],\
                      ["4", "P4", ["C4"]],\
                      ["5", "P5", ["C5.1", "C5.2"]]\
                      ]
        fields2 = [["2", None, None]]
        line = assemble_parameter(join_checker, args, file1, file2, fields1, fields2)
        checker = TableJoinCheckerTask(line)
        names = checker.get_process_name()
        real_names1 = [["preprocesser", p[1], p[2]] for p in fields1 if p[1] is not None]
        
        real_names2 = [["preprocesser", p[1], p[2]] for p in fields2 if p[1] is not None]
        real_names=real_names1
        real_names.extend(real_names2)
        real_names.append(["join_checker", "join_checker", args])
        for name in names:
            self.assertIn(name, real_names)
    
    def test_excute(self):
        """ test for excute """
        join_checker = "join_checker_equal.EqualJoinChecker"
        args = None
        file1 = "./data/table_join_checker/file1.in"
        file2 = "./data/table_join_checker/file2.in"
        fields1 = [["0", None, None]]
        fields2 = [["0", None, None]]
        line = assemble_parameter(join_checker, args, file1, file2, fields1, fields2)
        checker = TableJoinCheckerTask(line)
        checker.excute(self._p_m)
        status_info = checker._status_infos[0]
        self.assertEqual(status_info.check_cnt, 7)
        self.assertEqual(status_info.check_fail_cnt, 3)



if __name__ == '__main__':
    unittest.main()

