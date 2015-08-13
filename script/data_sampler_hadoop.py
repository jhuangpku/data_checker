#!/usr/bin/env python
#coding:utf8
########################################################################
# 
# Copyright (c) 2015 4Paradigm.com, Inc. All Rights Reserved
# 
########################################################################
"""
File: data_sampler.py
Author: huangjing(huangjing@4paradigm.com)
Date: 2015/08/07 17:45:36
Description: data sampler main function
"""
import sys
sys.path.append("util")
import ConfigParser
import getopt 
#import logging

#import log
from data_sampler_task import DataSamplerTaskHadoop 
from data_sampler_task import DataSamplerTaskInitError

def main(config_file, command):
    """main function

    Args:
        config_file: don't use it yet
        command: str

    Return:
        0: success
        1: fail
    """
    # init DataSampler
    try:
        data_sampler = DataSamplerTaskHadoop(command)
    except DataSamplerTaskInitError as e:
        sys.stderr.write("Init data sampler failed because of [%s], command [%s]" % (e, command))
        return 1

    sys.stderr.write("Init data sampler successful command [%s]" % (command))

    # excute DataSampler 
    ret = data_sampler.excute()
    if ret == 0:
        sys.stderr.write("Excute data sampler successful")
    else:    
        sys.stderr.write("Excute data sampler failed")
    data_sampler.write_status(sys.stderr, "utf8")
    del data_sampler
    return ret

if __name__ == "__main__":
    usage_str = """data_sampler_hadoop.py -c config_file --command command_str(included by "")
                """
                        
    opts, args = getopt.getopt(sys.argv[1:], "c:", ["command="])
    config_file = ""
    command = ""
    for o, a in opts:
        if o == "-c":
            config_file = a
        elif o == "--command":
            command = a.strip('"')
        else:
            print usage_str
            exit(2)
    if command == "":
        print usage_str
        exit(2)

    exit(main(config_file, command))




