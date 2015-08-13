#!/usr/bin/env python
#coding:utf8
########################################################################
# 
# Copyright (c) 2015 4Paradigm.com, Inc. All Rights Reserved
# 
########################################################################
"""
File: task_generator.py
Author: huangjing(huangjing@4paradigm.com)
Date: 2015/08/13 08:40:23
Description: task generator
"""

import sys
sys.path.append("util")
import ConfigParser
import getopt 
import logging
import json
import log

#class JsonCommand(object):
#    """ json like command """
#    def __init__(self, json_dic, command_prefix):
#        """init
#        
#        Args:
#            json_dic: json_like dic
#            command_prefix:"xxx.py -c config_file"
#        
#        Todo:
#            distinguish hadoop and unhadoop
#        """
#        self._json_dic = json_dic
#        self._command_prefix = "python " + command_prefix + "--command_file="
#
#    def task_str(self):
#        return json.dump(self._json_dic)
#
#    def command_str(self):
#        return self._command_prefix + '"' + json.dump(self._json_dic) + '"'


def divide_options(options):
    """ change [1,2,3,1.1,1.2,2.1] to [[1,1.1,1.2], [2,2.1,2.2], ...]"""
    options = [float(o) for o in options]
    options.sort()
    options = [str(o) for o in options]
    prefix = ""
    new_options = []
    new_list = []
    for o in options:
        if prefix == "":
            prefix = o[0]
            new_list.append(o)
        elif o[0] != prefix:
            new_options.append(new_list)
            new_list = []
            prefix = o[0]
            new_list.append(o)
        else:
            new_list.append(o)
    new_options.append(new_list)
    return new_options


#def handler
def get_opt_blocks(config, section):
    """ get options """
    blocks = []
    options = config.options(section)
    # divide options according to prefix
    option_blocks = divide_options(options)

    for option_block in option_blocks:
        main_option = config.get(section, option_block[0])
        sub_options = []
        for s in option_block[1:]:
            sub_options.append(config.get(section, s))
        blocks.append([main_option, sub_options])


def get_fields(value):
    """get fields list"""
    field_list = []
    fields = value.split("|")
    for field in fields:
        items = field.split(":")
        dic = {}
        dic["col_no"] = int(items[0])
        if len(items) >= 2:
            dic["processer"] = items[1]
        if len(items) >= 3:
            dic["args"] = items[2].split(",")
        field_list.append(dic)
    return field_list

def str_handler(l):
    """handler sub str line"""
    l = l.rstrip("\n").strip(" ;")
    cols = l.split(";")
    dic = {}
    for col in cols:
        key, value = cols.split("=")
        if key == "fields":
            fields_list = get_fields(value)
            dic[key] = fields_list
        else:
            values = values.split(",")
            if len(values) == 1:
                values = values[0]
            dic[key] = value
    return dic



def get_json_dics(config, section):
    """ get lines """
    line_blocks = get_opt_blocks(config, section)
    for block in line_blocks:
        task_dic = str_handler(block[0])
        if section == "data_sampler":
            continue
        elif section == "table_check":
            key = "col_check_task"
        elif section == "table_join_check":
            key = "files"
        task_dic[key] = []
        for l in block[1]:
            sub_task_dic = str_handler(l)
            task_dic[key].append(sub_task_dic)
    return task_dic

def get_table_checker_commands(config, info_dics):
    json_dics = get_json_dics(config, "table_checker")
    for json_dic in json_dics:
        json_dic["filename"] = info_dics[json_dic["filename"]]["output_file"]
        json_dic["decode"] = info_dics[json_dic["filename"]]["encode"]
        if sep not in json_dic:
            json_dic["sep"] = G_DEFAULT_ARGS["table_checker"]["sep"]
        try:
            tmp = TableCheckerTask(json.dump(json_dic))
        except TableCheckerTaskInitError as e:
            logging.fatal("%s" % (e))
            return None
    return json_dics

def get_table_join_checker_commands(config, info_dics):
    json_dics = get_json_dics(config, "table_join_checker")
    for n_json_dic in json_dics:
        for json_dic in n_json_dic["files"]:
            json_dic["filename"] = info_dics[json_dic["filename"]]["output_file"]
            json_dic["decode"] = info_dics[json_dic["filename"]]["encode"]
            json_dic["ratio"] = info_dics[json_dic["filename"]]["ratio"]
            if "sep" not in json_dic:
                json_dic["sep"] = G_DEFAULT_ARGS["table_checker"]["sep"]
            
        try:
            tmp = TableJoinCheckerTask(json.dump(n_json_dic))
        except TableJoinCheckerTaskInitError as e:
            logging.fatal("%s" % (e))
            return None
    return json_dics



def get_data_sampler_commands(config):
    """ data_sampler_commands"""
    # get all json_dics 
    json_dics = get_json_dics(config, "data_sampler")
    new_json_dics = []
    info_dics = {} # input_file_name:dic_index
    # as for data_sampler we must check following things
    for json_dic in json_dics:
        input_file = json_dic["input_file"]
        # hdfs, change output
        if input_file[0:4] == "hdfs":
            new_json_dic = deep.copy(json_dic)
            new_json_dic["output_file"] = "%s/%s.sample" % (new_json_dic["output_file"], \
                                                            f.rstrip("/").split("/")[-1])
            if input_file in info_dics:
                new_json_dics[info_dics[input_file]] = new_json_dic
            else:
                new_json_dics.append(new_json_dic)
                info_dics[input_file] = len(new_json_dics) - 1 
        # not hdfs
        else:
            for f in glob.glob(input_file):
                new_json_dic = deep.copy(json_dic)
                new_json_dic["input_file"] = f
                new_json_dic["output_file"] = "%s/%s.sample" % (new_json_dic["output_file"], f)
                # replace
                if f in info_dics:
                    new_json_dics[info_dics[f]] = new_json_dic 
                else:
                    new_json_dics.append(new_json_dic)
                    info_dics[f] = len(new_json_dics) - 1
    info_dics = {}
    # for no set args, set default args
    for json_dic in new_json_dics:
        del json_dic["output_dir"]
        if "decode" not in json_dic:
            json_dic["decode"] = G_DEFAULT_ARGS["data_sampler"]["decode"]
        if "encode" not in json_dic:
            json_dic["encode"] = G_DEFAULT_ARGS["data_sampler"]["encode"]
        if "sep" not in json_dic:
            json_dic["sep"] = G_DEFAULT_ARGS["data_sampler"]["sep"]
        if "ratio" not in json_dic:
            json_dic["ratio"] = G_DEFAULT_ARGS["data_sampler"]["ratio"]
        try:
            tmp = DataSamplerTask(json.dump(json_dic))
        except DataSamplerTaskInitError as e:
            logging.fatal("%s" % (e))
            return (None, None)

        f = json_dic["input_file"]
        info_dics[f] = {}
        info_dics[f]["ratio"] = json_dic["ratio"]
        info_dics[f]["output_file"] = json_dic["output_file"]
        info_dics[f]["encode"] = json_dic["encode"]
    # get info_dics from jsons_dics 
    return new_json_dics, info_dics


def get_commands(task_file, config_file) 
    """
        get commands from task_file

        Args:
            task_file: read task file to get commands
            config_file: config_file_name use for command_str

        Return:
            [[JsonCommand], [JsonCommand], [JsonCommand]]
    """
    #data_sampler_commands = []
    #table_checker_commands = []
    #table_join_checker_commands = []
    config = ConfigParser.ConfigParser()
    config.read(config_file)
    if not config:
        logging.fatal("Read task_file failed [%s]" %(task_file))
        return None
    logging.info("Read task_file successful [%s]" % (task_file))
    
    # read the data_sampler task and check validation 
    data_sampler_commands, data_sampler_dic = get_data_sampler_commands(config)
    if data_sampler_commands is None:
        logging.fatal("Check data_sampler when reading task_file failed [%s]" % (task_file))
        return None
    logging.info("Check data_sampler successful [%s]" % (task_file))
    # get table_checker task and check validation
    table_checker_commands = get_table_checker_commands(config, data_sampler_dic)
    if data_sampler_commands is None:
        logging.fatal("Check table_checker when reading task_file failed [%s]" % (task_file))
        return None
    logging.info("Check table_checker successful [%s]" % (task_file))

    # get table_join_checker task and check validation
    table_join_checker_commands = get_table_checker_commands(config, data_sampler_dic)
    if data_sampler_commands is None:
        logging.fatal("Check table_join_checker when reading task_file failed [%s]" % (task_file))
        return None
    logging.info("Check table_join_checker successful [%s]" % (task_file))

    return [data_sampler_commands, table_checker_commands, table_join_checker_commands] 

def main(config_file, task_file, command_file):
    """main function

    Args:
        config_file: config location
        task_file: task location, if it is "", get location from config_file
        command_file: command location if it is "", get from config_file

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
    log_file  = config.get("task_generator", "log")
    log_level = eval(config.get("task_generator", "level"))
    log.init_log(log_file, log_level)

    # init task_file 
    if task_file == "":
        task_file = config.get("task_generator", "task_file")
    
    # init command_file 
    if command_file == "":
        command_file = config.get("task_generator", "command_file")
    
    output_task_files = []
    for section in ["data_sampler", "table_checker", "table_join_checker"]
        filename = config.get(section, "task_file")
        output_task_files.append(filename)

    # read task_file and handler information 
    commands_list = get_commands(task_file, config_file) 
    if commands_list is None:
        logging.fatal("Get commands from [%s] failed" % (task_file))
    logging.info("Get commands from [%s] successful" % (task_file))
    
    for output_task_file, commands_list in zip(output_task_files, commands_list):
        write_task(commands_list, output_task_file)
    
    write_commands(commands_list, command_file)




if __name__ == "__main__":
    usage_str = """task_generator.py -c config_file(default: conf/global.conf) 
                           -t task_file(default: get location from config_file) 
                           --command_file command_file if it is set, command_file location in config is useless
                """
                        
    opts, args = getopt.getopt(sys.argv[1:], "c:t:", ["command_file="])
    config_file = "conf/global.conf"
    task_file = ""
    command_file = ""
    for o, a in opts:
        if o == "-c":
            config_file = a
        elif o == "-t":
            task_file = a
        elif o == "--command_file":
            command = a
        else:
            print usage_str
            exit(2)

    exit(main(config_file, task_file, command_file))




