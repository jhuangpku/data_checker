#!/usr/bin/env python
#coding:utf8
########################################################################
# 
# Copyright (c) 2015 4Paradigm.com, Inc. All Rights Reserved
# 
########################################################################
"""
File: table_checker.py
Author: huangjing(huangjing@4paradigm.com)
Date: 2015/08/10 09:26:25
Description:table checker main function
"""

import sys
sys.path.append("util")
import ConfigParser
import getopt 
import logging 

import log
from task_manager import TableCheckerManager
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
    log_file  = config.get("table_checker", "log")
    log_level = eval(config.get("table_checker", "level"))
    log.init_log(log_file, log_level)

    # init task_file 
    if command == "" and task_file == "":
        task_file = config.get("table_checker", "task_file")
    
    # init status_file 
    if status_file == "":
        status_file = config.get("table_checker", "status_file")
    
    # init table_checker
    try:
        table_checker = TableCheckerManager(task_file, command, status_file)
    except TaskManagerInitError as e:
        logging.fatal("Init table checker failed because of [%s], task_file [%s]\
                      command [%s], status_file [%s]" % (e, task_file, command, status_file))
        return 1

    logging.info("Init table checker successful task_file [%s] command [%s], status_file [%s]" % \
                 (task_file, command, status_file))


    # excute every task in table_checker 
    ret = table_checker.excute()
    if ret == 0:
        logging.info("Excute table checker successful")
    else:
        logging.fatal("Excute table checker failed")
    del table_checker 
    return ret


if __name__ == "__main__":
    usage_str = """table_checker.py -c config_file(default: conf/global.conf) 
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




