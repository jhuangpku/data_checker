#!/usr/bin/env python
#coding:utf8
########################################################################
# 
# Copyright (c) 2015 4Paradigm.com, Inc. All Rights Reserved
# 
########################################################################
"""
File: sample_data.py
Author: HuangJing(HuangJing@4paradigm.com)
Date: 2015/07/30 16:22:49
Description: sample data 
ToDo:
    add cache mechanism
    [can not handler easily] sample_file的key很容易就写重复了，需要加上检查机制
    [done]spetial problem
"""

import sys
import getopt
import ConfigParser
import glob
import random

import logging

import log

G_DEFAULT_RATIO = 0.1


class DataSamplerInitError(Exception):
    """ Init error for DataSampler"""
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg


class DataSamplerConfError(Exception):
    """conf error"""
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg


class DataSampler(object):
    """
    data sampler
    """

    def __init__(self, file_in, file_out, sample_ratio, sep = "\n", encode = "utf8"):
        """
        init data sampler 
        
        Args:
            file_in: input file name
            file_out: output file name 
            sample_ratio: sample ratio
        
        Return:
            None
        
        Exception:
            DataSamplerInitError
        """
        self.buf_size = 512
        self.sep = sep
        self.encode = encode
        self.line_cnt = 0
        self.sample_line_cnt = 0
        self.f_in_name = file_in 
        self.f_out_name = file_out
        try:
            self.f_in = open(file_in, "r")
        except IOError as e:
            raise DataSamplerInitError("Sample_ratio init failed because of [%s]" % (e))

        try:
            self.f_out = open(file_out, "w")
        except IOError as e:
            raise DataSamplerInitError("Sample_ratio init failed because of [%s]" % (e))

        try:
            self.ratio = float(sample_ratio)
        except TypeError as e:
            raise DataSamplerInitError("Sample_ratio init failed because of [%s]" % (e))

        if self.ratio <= 0 or self.ratio > 1:
            raise DataSamplerInitError("Sample_ratio init failed because of [Sample_ratio [%lf] must in (0, 1]]" % (self.ratio))

    def sample(self):
        """
        begin to sample
        
        Args:
            None 

        Return:
            None 

        Exception:
            None
        """
        buf = self.f_in.read(self.buf_size)
        last_line = u""
        while (buf):
            buf = last_line + buf.decode(self.encode)
            lines = buf.split(self.sep)
            #print lines
            #print "aa", lines
            for l in lines[0:-1]:
                self.line_cnt += 1
                r = random.random()
                if r < self.ratio:
                    self.f_out.write("%s\n" % (l.encode(self.encode)))
                    self.sample_line_cnt += 1
            
            buf = self.f_in.read(self.buf_size)
            last_line = lines[-1]
        
        if last_line != "" and last_line != "\n":
            self.line_cnt += 1
            r = random.random()
            if r < self.ratio:
                self.f_out.write("%s\n" % (last_line.encode(self.encode)))
                self.sample_line_cnt += 1
            


    def __str__(self):
        return "file=%s,sample_file=%s,ratio=%f,line_cnt=%d, sample_line_cnt=%d" % \
                (self.f_in_name, self.f_out_name, self.ratio, self.line_cnt, self.sample_line_cnt)


def handler_dir_paras(value_str):
    """ 
    handler one line sample dir info

    Args:
        value_str: str

    return:
        {}

    Exception:
        DataSamplerConfError
    """
    paras = {}
    values = value_str.split(",")
    tmp_dic = {}
    for value in values:
        k, v = value.split("=")
        k = k.lstrip(" ").rstrip(" ")
        v = v.lstrip(" ").rstrip(" ")
        tmp_dic[k] = v
    
    if "file_in" not in tmp_dic:
        raise DataSamplerConfError("line [%s] in sample file conf must have file_in" %(value_str))
    else:
        key = tmp_dic["file_in"]
        
    if "file_out" not in tmp_dic:
        raise DataSamplerConfError("line [%s] in sample file conf must have file_out" %(value_str))
    
    
    # get every file in this key dir
    for k in glob.glob(key):
        #print k
        paras[k] = {}
        filename = k.split("/")[-1]
        paras[k]["file_out"] = tmp_dic["file_out"] + "/" + filename
        if "ratio" in tmp_dic:
            paras[k]["ratio"] = tmp_dic["ratio"]
    
        if "sep" in tmp_dic:
            paras[k]["sep"] = tmp_dic["sep"]

        if "encode" in tmp_dic:
            paras[k]["encode"] = tmp_dic["encode"]
    
    return paras




def handler_file_paras(value_str):
    """ 
    handler one line sample file info

    Args:
        value_str: str

    return:
        {}

    Exception:
        DataSamplerConfError
    """
    paras = {}
    values = value_str.split(",")
    tmp_dic = {}
    for value in values:
        k, v = value.split("=")
        k = k.lstrip(" ").rstrip(" ")
        v = v.lstrip(" ").rstrip(" ")
        tmp_dic[k] = v
    
    if "file_in" not in tmp_dic:
        raise DataSamplerConfError("line [%s] in sample file conf must have file_in" %(value_str))
    else:
        key = tmp_dic["file_in"]
        paras[key] = {}

    if "file_out" not in tmp_dic:
        raise DataSamplerConfError("line [%s] in sample file conf must have file_out" %(value_str))
    else:
        paras[key]["file_out"] = tmp_dic["file_out"] 
    
    if "ratio" in tmp_dic:
        paras[key]["ratio"] = tmp_dic["ratio"]
    
    if "sep" in tmp_dic:
        paras[key]["sep"] = tmp_dic["sep"]

    if "encode" in tmp_dic:
        paras[key]["encode"] = tmp_dic["encode"]
    
    #print values, key, paras[key]
    return paras


def read_files_paras(config):
    """ read sample parameter
    Args:
        config

    Return:
        {}
    """
    paras = {}
    for key in config.options("sample_file"):
        value_str = config.get("sample_file", key)
        if key[0:3] == "dir":
            try:
                para = handler_dir_paras(value_str)
            except DataSamplerConfError as e:
                raise e
            paras.update(para)

        elif key[0:4] == "file":
            try:
                para = handler_file_paras(value_str)
            except DataSamplerConfError as e:
                raise e
            paras.update(para)
    return paras

               
    



def main(config_file):
    """ main function
    Args:
        config_file: config file 
        cache_file: cache file 

    Return:
        0: successful
        1: fail
    
    Exception:
        None
    """
    
    # read config
    config = ConfigParser.ConfigParser() 
    config.read(config_file)
    if not config:
        logging.fatal("Read config_file failed [%s]" % (config_file))
        return 1
    logging.info("Read config_file successful [%s]" % (config_file))

    # init log 
    log_file  = config.get("log", "dir")
    log_level = eval(config.get("log", "level"))
    log.init_log(log_file, log_level)
   
    # get output info
    sample_info_file = config.get("sample_info", "file")
    try:
        info_file_handler = open(sample_info_file, "w")
    except IOError:
        logging.warning("Can not open file [%s]" %(sample_info_file))
        return 1


    # get all file which need to sample 
    # filename -> filename_out, ratio, line_seprater, coding  
    files_paras = read_files_paras(config)
    #print files_paras 

    # hander
    for file_in in files_paras:
        file_out = files_paras[file_in]["file_out"]
        if "ratio" in files_paras[file_in]:
            ratio = files_paras[file_in]["ratio"]
        else:
            ratio = G_DEFAULT_RATIO
        if "sep" in files_paras[file_in]:
            sep = files_paras[file_in]["sep"]
        else:
            sep = "\n"
        if "encode" in files_paras[file_in]:
            encode = files_paras[file_in]["encode"]
        else:
            encode = "utf8"

        try:
            data_sampler = DataSampler(file_in, file_out, ratio, sep, encode)
        except DataSamplerInitError as e:
            info_file_handler.close()
            logging.warning("Fail to init data_sampler for [%s] because of [%s]" % (file_in, e))
            return 1

        data_sampler.sample()
        info_file_handler.write("%s\n" % (data_sampler))
        logging.info("Sample %s successful" % (file_in))

    info_file_handler.close()
    return 0
 

if __name__ == "__main__":
    usage_str = "sample_data.py -c config_file(default: ../conf/sample_data.conf)"
    opts, args = getopt.getopt(sys.argv[1:], "c:")
    config_file = "conf/sample_data.conf"
    cache_file = ""
    for o, a in opts:
        if o == "-c":
            config_file = a
        else:
            print usage_str
            exit(1)
    
    exit(main(config_file))
