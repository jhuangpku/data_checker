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
sys.path.append("script/util")
import log
import ConfigParser
import getopt 
import json
import glob
import copy

import logging
from data_sampler_task import DataSamplerTask
from data_sampler_task import DataSamplerTaskInitError
from table_checker_task import TableCheckerTask
from table_checker_task import TableCheckerTaskInitError
from table_join_checker_task import TableJoinCheckerTask 
from table_join_checker_task import TableJoinCheckerTaskInitError

G_DEFAULT_ARGS = {
    "data_sampler":{
        "ratio"   : 0.1,
        "sep"     : "\n",
        "decode"  : "utf8",
        "encode"  : "utf8"
    },
    
    "table_checker":{
        "sep"     : "\t",
    },
    
    "table_join_checker":{
        "sep"     : "\t",
    }
}
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

def parse_escape_char(value):
    escape_chars = {
        u"\\n":u"\n",
        u"\\t":u"\t",
        u"\\001":u"\001",
        u"\\002":u"\002",
    }
    for e in escape_chars:
        c = escape_chars[e]
        value = value.replace(e, c)
    
    return value

def get_fields(value):
    """get fields list"""
    field_list = []
    fields = value.split("|")
    for field in fields:
        items = field.split(":")
        dic = {}
        dic["field_no"] = int(items[0])
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
        key, value = col.split("=")
        value = parse_escape_char(value)
        if key == "fields":
            fields_list = get_fields(value)
            dic[key] = fields_list
        elif key == "col_cnt":
            value = int(value)
            dic[key] = value
        elif key == "ratio":
            value = float(value)
            dic[key] = value
        elif key == "args":
            values = value.split(",")
            dic[key] = values
        elif key == "sep":
            value = parse_escape_char(value)
            dic[key] = value
        else:
            values = value.split(",")
            if len(values) == 1:
                values = values[0]
            dic[key] = values
    
    return dic



def divide_options(options):
    """ change [1,2,3,1.1,1.2,2.1] to [[1,1.1,1.2], [2,2.1,2.2], ...]"""
    if len(options) == 0:
        raise ValueError("Invalid option options length must > 0")
    for index, o in enumerate(options):
        try:
            options[index] = int(o)
        except ValueError:
            try:
                options[index] = float(o)
            except ValueError as e:
                raise ValueError("Invalid option [%s]. It must be a int or float" % (o))
        
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
    # make sure every new_options start with int 
    for item in new_options:
        try:
            int(item[0])
        except ValueError as e:
            raise ValueError("Invalid option group [%s]. Every option group \
                             should be started with int tag  [%s]" \
                             % (str(item), e))
    return new_options


def get_opt_blocks(config, section):
    """ get options """
    blocks = []
    try:
        options = config.options(section)
    except ConfigParser.NoSectionError as e:
        raise e
    # divide options according to prefix
    try:
        option_blocks = divide_options(options)
    except ValueError as e:
        raise e
    
    for option_block in option_blocks:
        try:
            main_option = config.get(section, option_block[0])
            sub_options = []
            for s in option_block[1:]:
                sub_options.append(config.get(section, s))
            blocks.append([main_option, sub_options])
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError) as e:
            raise e
    return blocks


def get_json_dics(config, section):
    """ get lines """
    try:
        line_blocks = get_opt_blocks(config, section)
    except (ConfigParser.NoOptionError, ConfigParser.NoSectionError, ValueError) as e:
        raise e
    json_dics = []
    for block in line_blocks:
        task_dic = str_handler(block[0])
        #print "df", task_dic
        key = ""
        if section == "table_checker":
            key = "col_check_task"
        elif section == "table_join_checker":
            key = "files"
        if key is not "":
            task_dic[key] = []
            for l in block[1]:
                sub_task_dic = str_handler(l)
                task_dic[key].append(sub_task_dic)
        json_dics.append(task_dic)
        #print block 
        #print task_dic
    return json_dics

def get_table_checker_commands(config, info_dics):
    try:
        json_dics = get_json_dics(config, "table_checker")
    except (ConfigParser.NoOptionError, ConfigParser.NoSectionError, ValueError) as e:
        logging.warning("%s" % (e))
        return None 
    for json_dic in json_dics:
        if "filename" not in json_dic:
            logging.warning("Invalid json_dic [%s], It must contain key ['filename']" % \
                            (str(json_dic)))
        filename = json_dic["filename"]
        if filename not in info_dics:
            logging.warning("Invalid filename [%s], it should be contained in data_sampler" \
                            % (filename))
            return None

        json_dic["filename"] = info_dics[filename]["output_file"]
        json_dic["decode"] = info_dics[filename]["encode"]
        
        if "sep" not in json_dic:
            json_dic["sep"] = G_DEFAULT_ARGS["table_checker"]["sep"]
        
        try:
            tmp = TableCheckerTask(json.dumps(json_dic))
        except TableCheckerTaskInitError as e:
            logging.fatal("%s" % (e))
            return None
    return json_dics

def get_table_join_checker_commands(config, info_dics):
    try:
        json_dics = get_json_dics(config, "table_join_checker")
    except (ConfigParser.NoOptionError, ConfigParser.NoSectionError, ValueError) as e:
        logging.warning("%s" % (e))
        return None 
    
    for n_json_dic in json_dics:
        if "files" not in n_json_dic:
            logging.warning("Invalid json_dic [%s], it should contain key ['files']" % (n_json_dic))
            return None
        for json_dic in n_json_dic["files"]:
            if "filename" not in json_dic:
                logging.warning("Invalid json_dic [%s], It must contain key ['filename']" % \
                            (str(json_dic)))
            filename = json_dic["filename"]
            if filename not in info_dics:
                logging.warning("Invalid filename [%s], it should be contained in data_sampler" \
                                % (filename))
                return None
            json_dic["filename"] = info_dics[filename]["output_file"]
            json_dic["decode"] = info_dics[filename]["encode"]
            json_dic["ratio"] = info_dics[filename]["ratio"]
            if "sep" not in json_dic:
                json_dic["sep"] = G_DEFAULT_ARGS["table_join_checker"]["sep"]
            
        try:
            tmp = TableJoinCheckerTask(json.dumps(n_json_dic))
        except TableJoinCheckerTaskInitError as e:
            logging.fatal("%s" % (e))
            return None
    
    return json_dics



def get_data_sampler_commands(config):
    """ data_sampler_commands"""
    # get all json_dics 
    try:
        json_dics = get_json_dics(config, "data_sampler")
    except (ConfigParser.NoOptionError, ConfigParser.NoSectionError, ValueError) as e:
        logging.warning("%s" % (e))
        return None 
    
    new_json_dics = []
    info_dics = {} # input_file_name:dic_index
    # as for data_sampler we must check following things
    for json_dic in json_dics:
        if "input_file" not in json_dic or "output_dir" not in json_dic:
            logging.warning("Invalid json_dic [%s], it should contain key ['input_file']" % (json_dic))
            return None
        input_file = json_dic["input_file"]
        # hdfs, change output
        if input_file[0:4] == "hdfs":
            new_json_dic = copy.deepcopy(json_dic)
            new_json_dic["output_file"] = "%s/%s.sample" % (new_json_dic["output_dir"], \
                                                            f.rstrip("/").split("/")[-1])
            if input_file in info_dics:
                new_json_dics[info_dics[input_file]] = new_json_dic
            else:
                new_json_dics.append(new_json_dic)
                info_dics[input_file] = len(new_json_dics) - 1 
        # not hdfs
        else:
            f_list = glob.glob(input_file)
            if len(f_list) == 0:
                logging.warning("No file match pattern [%s]" % (input_file))
                return None
            for f in f_list:
                new_json_dic = copy.deepcopy(json_dic)
                new_json_dic["input_file"] = f
                new_json_dic["output_file"] = "%s/%s.sample" % (new_json_dic["output_dir"], f.split("/")[-1])
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
            #print json.dumps(json_dic)
            tmp = DataSamplerTask(json.dumps(json_dic))
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


def get_commands(task_file): 
    """
        get commands from task_file

        Args:
            task_file: read task file to get commands
            config_file: config_file_name use for command_str

        Return:
            [[Json_dic], [Json_dic], [Json_dic]]
    """
    config = ConfigParser.ConfigParser()
    config.read(task_file)
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
    #print data_sampler_commands
    #print data_sampler_dic
    # get table_checker task and check validation
    table_checker_commands = get_table_checker_commands(config, data_sampler_dic)
    if table_checker_commands is None:
        logging.fatal("Check table_checker when reading task_file failed [%s]" % (task_file))
        return None
    logging.info("Check table_checker successful [%s]" % (task_file))

    # get table_join_checker task and check validation
    table_join_checker_commands = get_table_join_checker_commands(config, data_sampler_dic)
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
    try:
        log_file  = config.get("task_generator", "log")
        log_level = eval(config.get("task_generator", "level"))
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError) as e:
        logging.fatal("%s" % (e))
        return 1
    log.init_log(log_file, log_level)

    # init task_file 
    if task_file == "":
        try:
            task_file = config.get("task_generator", "task_file")
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError) as e:
            logging.fatal("%s" % (e))
            return 1
    logging.info("Set task file [%s] successful" % (task_file)) 
    
    # init command_file 
    if command_file == "":
        try:
            command_file = config.get("task_generator", "command_file")
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError) as e:
            logging.fatal("%s" % (e))
            return 1
    logging.info("Set command file [%s] successful" % (command_file)) 
    
    # init output task_file
    output_task_files = []
    for section in ["data_sampler", "table_checker", "table_join_checker"]:
        try:
            filename = config.get(section, "task_file")
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError) as e:
            logging.fatal("%s" % (e))
            return 1
        output_task_files.append(filename)
    logging.info("Get output task file [%s] successful" % (str(output_task_files)))

    # read task_file and handler information 
    commands_list = get_commands(task_file)
    #print commands_list
    if commands_list is None:
        logging.fatal("Get commands from [%s] failed" % (task_file))
        return 1
    logging.info("Get commands from [%s] successful" % (task_file))
    
    # write commands and generate task file
    tags = ["data_sampler", "table_checker", "table_join_checker"]
    for output_task_file, commands, tag in zip(output_task_files, commands_list, tags):
        ret = write_task(commands, output_task_file, tag)
        if ret != 0:
            logging.fatal("Write [%s] task [%s] failed" % (tag, output_task_file))
            return 1
        logging.info("Write [%s] task [%s] successful" % (tag, output_task_file))
        ret = write_commands(commands, command_file, tag, config_file)
        if ret != 0:
            logging.fatal("Write [%s] commands [%s] failed" % (tag, command_file))
            return 1
        logging.info("Write [%s] commands [%s] successful" % (tag, command_file))

    return 0 


def write_task(commands, filename, tag):
    """write task"""
    try:
        with open(filename, "w") as f:
            for c in commands:
                f.write("%s\n" % (json.dumps(c)))
    except IOError as e:
        logging.warning("%s" % (e))
        return 1
    return 0

def write_commands(commands, filename, tag, config_file):
    """write commands"""
    prefix = 'python %s.py -c %s --command="' % (tag, config_file)

    try:
        with open(filename, "a") as f:
            for c in commands:
                if "input_file" in c and c["input_file"][0:4] == "hdfs":
                    f.write("%s\t%s\t" % (c["input_file"], c["output_file"]))
                    del c["input_file"]
                    del c["output_file"]
                    del c["sep"]
                    f.write("%s\n" % (json.dumps(c)))
                else:
                    f.write('%s%s"\n' % (prefix, json.dumps(c)))
    except IOError as e:
        logging.warning("%s" % (e))
        return 1
    return 0




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




