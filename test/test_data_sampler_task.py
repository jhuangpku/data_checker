#!/usr/bin/env python
#coding:utf8
########################################################################
# 
# Copyright (c) 2015 4Paradigm.com, Inc. All Rights Reserved
# 
########################################################################
"""
File: test_data_checker.py
Author: root(root@4paradigm.com)
Date: 2015/07/31 10:44:48
Description: test for data_checker.py
"""
import sys
sys.path.append("../script/")
import unittest
import json

from data_sampler_task import DataSamplerTask
from data_sampler_task import DataSamplerTaskInitError 
from data_sampler_task import DataSamplerTaskExcuteError

def assemble_parameter(file_in, file_out, ratio, sep, decode, encode):
    """assemble_parameter """
    dic = {}
    dic["input_file"] = "./data/data_sampler/" + file_in
    dic["output_file"] = "./data/data_sampler_output/" + file_out
    dic["ratio"] = ratio
    dic["sep"] = sep
    dic["encode"] = encode
    dic["decode"] = decode
    return (json.dumps(dic) + "\n")

def get_output_lines(filename):
    """ read filename """
    file = "./data/data_sampler_output/" + filename
    return [l.rstrip("\n") for l in open(file, "r")]

class TestDataSamplerTask(unittest.TestCase):
    """ test for data sampler task """
    def test_init(self):
        """ test for init """
        line = assemble_parameter("file_in.big", "file_out.big", 0.8, "\n", "utf8", "gbk")
        data_sampler = DataSamplerTask(line)
        self.assertEqual(data_sampler._f_in_name, "./data/data_sampler/file_in.big")
        self.assertEqual(data_sampler._f_out_name, "./data/data_sampler_output/file_out.big")
        self.assertAlmostEqual(data_sampler._ratio, 0.8)
        self.assertEqual(data_sampler._sep, "\n")
        self.assertEqual(data_sampler._decode, "utf8")
        self.assertEqual(data_sampler._encode, "gbk")
    
    
    def test_excute_bad_filename(self):
        """ test for file which not exist """
        # 1.output dir does not exist
        line = assemble_parameter("file_in.big", "not_exist/not_exist", 1.0, "\n", "utf8", "utf8") 
        data_sampler = DataSamplerTask(line)
        self.assertRaises(DataSamplerTaskExcuteError, data_sampler.excute)
        
        # 2.input file does not exist
        line = assemble_parameter("not_exist", "file_out.big", 1.0, "\n", "utf8", "utf8")
        data_sampler = DataSamplerTask(line)
        self.assertRaises(DataSamplerTaskExcuteError, data_sampler.excute)

    def test_excute(self):
        """ test for file """ 
        array = [u'我', u'们']
        # 1.gbk decode, utf8 encode
        line = assemble_parameter("file_in.gbk", "file_out.gbk", 1.0, "\n", "gbk", "utf8")
        data_sampler = DataSamplerTask(line)
        data_sampler.excute()
        output = get_output_lines("file_out.gbk")        
        for index, l in enumerate(output):
            l = l.decode("utf8")
            self.assertEqual(array[index], l)
        
        # 2. gbk decode gbk encode
        line = assemble_parameter("file_in.gbk", "file_out.gbk", 1.0, "\n", "gbk", "gbk")
        data_sampler = DataSamplerTask(line)
        data_sampler.excute()
        output = get_output_lines("file_out.gbk")        
        for index, l in enumerate(output):
            l = l.decode("gbk")
            self.assertEqual(array[index], l)

        # 3. special sep
        line = assemble_parameter("file_in.sep", "file_out.sep", 1.0, "\001\002", "utf8", "utf8")
        data_sampler = DataSamplerTask(line)
        data_sampler.excute()
        output = get_output_lines("file_out.sep")        
        for index, l in enumerate(output):
            l = l.decode("utf8")
            self.assertEqual(array[index], l)

        # 4. escape
        array = [u'我\\n', u'们\\n']
        line = assemble_parameter("file_in.escape", "file_out.escape", 1.0, "\001\002", "utf8", "utf8")
        data_sampler = DataSamplerTask(line)
        data_sampler.excute()
        output = get_output_lines("file_out.escape")        
        for index, l in enumerate(output):
            l = l.decode("utf8")
            self.assertEqual(array[index], l)

        # 5. big file
        array = range(0, 100000)
        line = assemble_parameter("file_in.big", "file_out.big", 1.0, "\n", "utf8", "utf8")
        data_sampler = DataSamplerTask(line)
        data_sampler.excute()
        line_cnt = data_sampler._status_infos[0].check_cnt
        sample_line_cnt = line_cnt - data_sampler._status_infos[0].check_fail_cnt
        self.assertEqual(line_cnt, len(array))
        self.assertEqual(sample_line_cnt, len(array))
        output = get_output_lines("file_out.big")        
        for index, l in enumerate(output):
            l = l.decode("utf8")
            self.assertEqual(array[index], int(l))

        # 6. ratio
        array = range(0, 100000)
        line = assemble_parameter("file_in.big", "file_out.big", 0.5, "\n", "utf8", "utf8")
        data_sampler = DataSamplerTask(line)
        data_sampler.excute()
        line_cnt = data_sampler._status_infos[0].check_cnt
        sample_line_cnt = line_cnt - data_sampler._status_infos[0].check_fail_cnt
        self.assertEqual(line_cnt, len(array))
        self.assertLess(sample_line_cnt, len(array))
        output = get_output_lines("file_out.big")        
        for index, l in enumerate(output):
            l = l.decode("utf8")
            self.assertIn(int(l), array)



if __name__ == '__main__':
    unittest.main()

