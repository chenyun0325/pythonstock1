# -*- coding: utf-8 -*-
"""
Created on Wed Oct 26 15:18:20 2016

@author: cy111966
"""
from __future__ import division
import tushare as ts
import pandas as pd
import numpy as np
import tradecons as cons
import logging as log
import sql
import sys
import toptrade as tp
import traceback



def get_stock_hist_data(code=None,start=None,end=None,sh_df=None,sz_df=None,cyb_df=None,table_name=None):
    
    """
       获取股票历史综合信息(区分交易量性质,大盘对比数据)
       Parameters
       ------
       symbols : string, array-like object (list, tuple, Series).
       
       start:string
                  开始日期 format：YYYY-MM-DD 为空时取到API所提供的最早日期数据
       end:string
                  结束日期 format：YYYY-MM-DD 为空时取到最近一个交易日数据
       return
       ------
       DataFrame
            属性:
    """
   
    
    fs_colums= cons.FS_COLUMNS_EXTEND
    #创建分时空df
    fs_df =pd.DataFrame(columns=fs_colums)
   
    #获取数据库数据
    param ={}
    param['code']=code
    param['begintime']=start
    param['endtime']=end
    try:
         df_db= pd.read_sql_query('SELECT * FROM db_test.tick_data_total where code=%(code)s and date>=%(begintime)s and date<=%(endtime)s',sql.engine,params =param)
    except Exception,e:
         df_db = None
         log.error(traceback.print_exc())
    if df_db is not None:
       if len(df_db.index)>0:
        df_date=df_db['date']
        mix_v = np.min(df_date).encode('utf-8')
        max_v = np.max(df_date).encode('utf-8')
        flag = compare_date(mix_v,max_v,start,end)
        if (flag ==0)|(flag ==-1):
           return #退出?
        if flag ==1:
           end = mix_v
        if flag ==2:
           start = max_v

    #获取个股历史行情数据
    stock_daily = ts.get_hist_data(code,start=start,end=end,retry_count=10)

    fs_df_merge =pd.DataFrame(columns=fs_colums)
        
    index = 0
        
    index_dic={}
    #循环个股dataframe---->根据时间和code获取分时数据
    for i,row in stock_daily.iterrows():
            
            
        tradedate=i
            
        index_dic[index]=i
           
        index = index+1
           
        fs_daily_df = get_handler_stock_fs_data(code,tradedate)
        #新增一行
        fs_df=fs_df.append(fs_daily_df,ignore_index=True)
            
        fs_df_merge=fs_df_merge.append(fs_daily_df,ignore_index=True)
            
            
    #处理个股和指数列合并 ,行数不一致处理?
    p_merge_on=fs_df_merge['date']

    #处理索引
    p_merge_on=p_merge_on.rename(index_dic)

    stock_daily.insert(0,'date',p_merge_on)

    p_change=stock_daily['p_change']
    p_index_sh=sh_df['p_change']
    p_index_sz=sz_df['p_change']
    p_index_cyb=cyb_df['p_change']
    p_dif_sh=p_change-p_index_sh
    p_dif_sz=p_change-p_index_sz
    p_dif_cyb=p_change-p_index_cyb
    
    fs_df=fs_df.rename(index_dic)
    #新增列
    fs_df.insert(0,'p_change',p_change)
    fs_df.insert(0,'p_dif_cyb',p_dif_cyb)
    fs_df.insert(0,'p_dif_sz',p_dif_sz)
    fs_df.insert(0,'p_dif_sh',p_dif_sh)
        
    fs_df_mix=pd.merge(fs_df,stock_daily,on=['date'],how='right')
    print(fs_df_mix)
    return fs_df_mix
        
def  get_handler_stock_fs_data(code = None,date=None):

     """
       获取与处理股票历史分笔数据
       Parameters
       ------
       code : string, array-like object (list, tuple, Series).
       
       date: string,日期
       
       return
       ------
       DataFrame
            属性:
     """ 
     fs_colums= cons.FS_COLUMNS
    
     time =cons.TIME
     stock_code =cons.STOCK_CODE

    #开始取数据
     fsdf = ts.get_tick_data(code,date=date,retry_count =10 )
     


     sale_v,buy_v,diff_v,s_p,b_p,dif_p,sale_m,buy_m,diff_m,s_m_p,b_m_p,dif_m_p =0,0,0,0,0,0,0,0,0,0,0,0
     try:
        fsdf_b = fsdf[fsdf.type=='买盘']

        fsdf_s = fsdf[fsdf.type=='卖盘']
        
     
     #成交量
        sale_v = np.sum(fsdf_s['volume'])
        buy_v = np.sum(fsdf_b['volume'])
        diff_v = buy_v - sale_v
        total_v = buy_v + sale_v
        s_p = sale_v / total_v
        b_p = buy_v /total_v
        dif_p = diff_v/total_v
     
     #成交金额
        sale_m = np.sum(fsdf_s['amount'])
        buy_m = np.sum(fsdf_b['amount'])
        diff_m = buy_m - sale_m
        total_m = buy_m + sale_m
        s_m_p = sale_m / total_m
        b_m_p = buy_m /total_m
        dif_m_p = diff_m/total_m
    

     #处理大单数据---by cy 2016-11-9
        dd_sale_v,dd_buy_v,dd_diff_v ,dd_total_v,dd_sale_m,dd_buy_m,dd_diff_m,dd_total_m,dd_diff_m_var= 0,0,0,0,0,0,0,0,0
        dd_df = get_handler_stock_fs_dd(code =code,date =date)
        if dd_df is not None:
        #计算金额
          dd_m = dd_df['price']*dd_df['volume']
          dd_p_dif =dd_df['price']-dd_df['preprice']
          dd_m_dif = dd_p_dif*dd_df['volume']
          dd_df.insert(0,'dd_m',dd_m)
          dd_df.insert(0,'dd_p_dif',dd_p_dif)
          dd_df.insert(0,'dd_m_dif',dd_m_dif)
        
          dd_df_b = dd_df[dd_df.type=='买盘']

          dd_df_s = dd_df[dd_df.type=='卖盘']
        
          dd_diff_m_var=np.sum(dd_df['dd_m_dif'])#价格变动计算的资金总和
        #大单成交量
          dd_sale_v = np.sum(dd_df_s['volume'])
          dd_buy_v = np.sum(dd_df_b['volume'])
          dd_diff_v = dd_buy_v - dd_sale_v
          dd_total_v = dd_buy_v + dd_sale_v
        #成交金额
          dd_sale_m = np.sum(dd_df_s['dd_m'])
          dd_buy_m = np.sum(dd_df_b['dd_m'])
          dd_diff_m = dd_buy_m - dd_sale_m
          dd_total_m = dd_buy_m + dd_sale_m
     except Exception,e:
        log.error(traceback.print_exc())
     #构建单条记录
     record=[[sale_v,buy_v,diff_v,s_p,b_p,dif_p,sale_m,buy_m,diff_m,s_m_p,b_m_p,dif_m_p,dd_sale_v,dd_buy_v,dd_diff_v ,dd_total_v,dd_sale_m,dd_buy_m,dd_diff_m,dd_total_m,dd_diff_m_var]]
   
     #创建分时df-用于返回
     fs_df =pd.DataFrame(record,columns=fs_colums,index=[date])
     
     #新增列
     fs_df.insert(0,stock_code,code)
     fs_df.insert(0,time,date)
     
     return fs_df
     

def  get_handler_stock_fs_dd(code =None,date =None,vol=400):
     dd_df = ts.get_sina_dd(code = code,date = date,vol = vol)
     return dd_df

#todo
#时间比较错误修正
def compare_date(mix_v=None,max_v=None,begin_v=None,end_v=None):
    flag =-1
    if (mix_v<=begin_v)&(max_v>=end_v):
        flag = 0#不需要执行
    if (mix_v>=begin_v)&(max_v>=end_v):
        flag = 1  #begin--->mix
    if (mix_v<=begin_v)&(max_v<=end_v):
        flag = 2  #max--->end
    return flag
        
    
if __name__=='__main__':
    """
    python stocktrade_v1.py x 2016-11-01 2016-11-09 tick_date_test
    
    """
    sh='sh' #指数代码
    sz='sz'
    cyb='cyb'
    sql_flag='append'
    code_list = sys.argv[1]
    start = sys.argv[2]
    end = sys.argv[3]
    table_name = sys.argv[4] 
    sh_df = ts.get_hist_data(sh,start=start,end=end,retry_count =10)
    sz_df = ts.get_hist_data(sz,start=start,end=end,retry_count=10)
    cyb_df = ts.get_hist_data(cyb,start=start,end=end,retry_count=10)
    bases = ts.get_stock_basics()
    indexs = bases.index
    for code in indexs:
        try:
            df = get_stock_hist_data(code = code,start=start,end=end,sh_df=sh_df,sz_df=sz_df,cyb_df=cyb_df,table_name=table_name)
            df.sort('date',inplace=True)
            res = tp.save_db(data=df,table_name=table_name,con=sql.engine,flag=sql_flag) 
        except Exception,e:
            log.error(code)
            log.error(sys.argv)
            log.error(traceback.print_exc())
            log.error(e)
            continue
        
#    df['ma_p_5']=df['close'].rolling(window=5).mean()
#    df['ma_p_10']=df['close'].rolling(window=10).mean()
#    df['ma_p_20']=df['close'].rolling(window=20).mean()
#    print(df)
    
     

     


     
        
    