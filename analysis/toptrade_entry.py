# -*- coding: utf-8 -*-
"""
Created on Tue Nov 08 23:14:26 2016

@author: cy111966
"""

import toptrade as tp
import sql
import sys
import logging
import traceback

if __name__=='__main__':
    """
    python toptrade_entry.py 1 top_list 2016-11-08
    
    """
    try:
        flag = sys.argv[1]
        table_name = sys.argv[2]
        fuc_param = sys.argv[3]
        sql_flag = 'append'
        df={}
        #print("---------")
        if flag=='1':
            df = tp.top_list(date=fuc_param)
        if flag=='2':
            df = tp.cap_list(days=int(fuc_param))
        if flag=='3':
            df = tp.inst_tops(days=int(fuc_param))
        if flag=='6':
            df = tp.broker_top(days=int(fuc_param))
        if flag=='4':
            df = tp.inst_detail()
        if flag=='5':
            df = tp.stock_basics()
            sql_flag = sys.argv[4]
        print(df)
        res = tp.save_db(data=df,table_name=table_name,con=sql.engine,flag=sql_flag)
    except Exception,e:
          logging.error(sys.argv)
          logging.error(traceback.print_exc())