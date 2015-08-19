#!/usr/bin/env python
#coding:utf8
########################################################################
# 
# Copyright (c) 2015 4Paradigm.com, Inc. All Rights Reserved
# 
########################################################################
"""
File: make_data.py
Author: huangjing(huangjing@4paradigm.com)
Date: 2015/08/18 08:09:42
Description: make data for unittest  
"""

def gen_data_sampler_data(file_dir):
    """
    make data for data sampler
    """
    # 1. testing for code
    file_in = file_dir + "/file_in.gbk"
    f = open(file_in, "w")
    array = [u'我', u'们'] 
    for a in array:
        f.write("%s\n" % (a.encode("gbk")))      
    f.close()

    # 2. testing for sep
    file_in = file_dir + "/file_in.sep"
    f = open(file_in, "w")
    array = [u'我', u'们']
    for a in array:
        f.write("%s\001\002" % (a.encode("utf8")))
    f.close()  

    # 3. testing for \n escape
    file_in = file_dir + "/file_in.escape"
    f = open(file_in, "w")
    array = [u'我', u'们']
    for a in array:
        f.write("%s\n\001\002" % (a.encode("utf8")))
    f.write("\n")
    f.close()  

    # 4. testing for big File
    file_in = file_dir + "/file_in.big"
    f = open(file_in, "w")
    array = range(0, 100000)
    for a in array:
        f.write("%d\n" % (a))
    f.close()

def reverse_list(cols):
    """change [[1,2], [3,4]] to [[1,3], [2,4]] and str them"""
    row = len(cols)
    column = len(cols[0])
    new_cols = []
    for i in range(0, column):
        new_col = [0] * row
        new_cols.append(new_col)
    for i, col in enumerate(cols):
        for j, item in enumerate(col):
            new_cols[j][i] = str(item)
    return new_cols

def gen_table_checker_data(file_dir):
    """
    make data for table checker
    """
    file_in = file_dir + "/file_in"
    f = open(file_in, "w")
    sep = ","
    cols = []
    # make the first col
    time_str = "2015-03-02 13:21:31"
    col = [time_str] * 10
    col[0] = "2015-03-02 13-21-31"
    cols.append(col)
    # make the second col 
    col = range(0, 10)
    col[2] = " 3 "
    cols.append(col)
    # make the third col 
    col = range(0, 10)
    col = [" " + str(c) + " " for c in col]
    col[7] = '" 9 "'
    col[8] = " 8.1 "
    cols.append(col)
    # make the fourth col
    col = range(0, 10)
    col = [c * 1.1 for c in col]
    col[9] = "sdf"
    col[8] = " 3.1 "
    cols.append(col)
    # make the fifth col
    col = [time_str] * 10 
    col[0] = "2015 03 02 12 00 00"
    cols.append(col)
    # make the sixth col 
    col = ["2015:03:02"] * 10
    col[3] = "2015:03:02 10:21:13"
    cols.append(col)
    cols = reverse_list(cols)
    for index, col in enumerate(cols):
        if index == 3 or index == 7:
            f.write("%s%s%s\n" % ((sep).join(col), sep, str(index)))
        else:
            f.write("%s\n" % sep.join(col))
    f.close()



def gen_table_join_checker_data(file_dir):
    """
    make data for table join checker
    """
    pass


if __name__ == "__main__":
    gen_data_sampler_data("./data/data_sampler/")
    gen_table_checker_data("./data/table_checker/")
    gen_table_join_checker_data("./data/table_join_checker/")




