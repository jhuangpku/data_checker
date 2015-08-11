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
import logging

import log
from task_manager import DataSamplerManager
from task_manager import TaskManagerInitError

def main(config_file, task_file, command, status_file):
    """main function

    Args:
        config_file: config location
        task_file: task location, if it is "", get location from config_file
        command: str, if it is "", then use task_file, otherwise use this command
        status_file: status file location

    Return:
        0: success
        1: fail
    """
    # read config 
    config = ConfigParser.ConfigParser()
    config.read(config_file)
    if not config:
        logging.fatal("Read config_file failed [%s]" %(config_file))
        return 1
    logging.info("Read config_file successful [%s]" % (config_file))
    
    # init log 
    log_file  = config.get("data_sampler", "log")
    log_level = eval(config.get("data_sampler", "level"))
    log.init_log(log_file, log_level)

    # init task_file 
    if command == "" and task_file == "":
        task_file = config.get("data_sampler", "task_file")
    
    # init status_file 
    if status_file == "":
        status_file = config.get("data_sampler", "status_file")
    
    # init DataSampler
    try:
        data_sampler = DataSamplerManager(task_file, command, status_file)
    except TaskManagerInitError as e:
        logging.fatal("Init data sampler failed because of [%s], task_file [%s]\
                      command [%s], status_file [%s]" % (e, task_file, command, status_file))
        return 1

    logging.info("Init data sampler successful task_file [%s] command [%s], status_file [%s]" % \
                 (task_file, command, status_file))

    # excute DataSampler 
    ret = data_sampler.excute()
    if ret == 0:
        logging.info("Excute data sampler successful")
    else:    
        logging.fatal("Excute data sampler failed")
    del data_sampler
    return ret

if __name__ == "__main__":
    usage_str = """data_sampler.py -c config_file(default: conf/global.conf) 
                           -t task_file(default: get location from config_file) 
                           -s status_file(default: get locationi from status_file)
                           --command command_str(included by ""), if it is set task file is useless
                """
                        
    opts, args = getopt.getopt(sys.argv[1:], "c:t:s:", ["command="])
    config_file = "conf/global.conf"
    task_file = ""
    command = ""
    status_file = ""
    for o, a in opts:
        if o == "-c":
            config_file = a
        elif o == "-t":
            task_file = a
        elif o == "--command":
            command = a.strip('"')
        elif o == "-s":
            status_file = a
        else:
            print usage_str
            exit(2)

    exit(main(config_file, task_file, command, status_file))




