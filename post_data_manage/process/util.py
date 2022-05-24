import os
import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def dir_check(path):
    """
    check weather the dir of the given path exists, if not, then create it
    """
   
    dir = path if os.path.isdir(path) else os.path.split(path)[0]
    if not os.path.exists(dir): os.makedirs(dir)
    return path
def write_list_list(fp, list_, model="a", sep=","):
    dir = os.path.dirname(fp)
    if  not os.path.exists(dir): os.makedirs(dir)
    f = open(fp,mode=model,encoding="utf-8")
    count=0
    lines=[]
    for line in list_:
        a_line=""
        for l in line:
            l=str(l)
            a_line=a_line+l+sep
        a_line = a_line.rstrip(sep)
        lines.append(a_line+"\n")
        count=count+1
        if count==10000:
            f.writelines(lines)
            count=0
            lines=[]
    f.writelines(lines)
    f.close()