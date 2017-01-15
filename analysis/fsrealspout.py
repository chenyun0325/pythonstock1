import storm
import random
import tushare as ts
from __future__ import division
import multiprocessing
import time
# Define some sentences
SENTENCES = """
the cow jumped over the moon
an apple a day keeps the doctor away
four score and seven years ago
snow white and the seven dwarfs
i am at two with nature
""".strip().split('\n')

class FsRealSpout(storm.Spout):
    # Not much to do here for such a basic spout
    def initialize(self, conf, context):
        self._conf = conf
        self._context = context

        storm.logInfo("Spout instance starting...")

    # Process the next tuple
    def nextTuple(self):
        # 停止一段时间(设置状态位)
        time.sleep(15)
        batch =10
        bases = ts.get_stock_basics()
        code_list = bases.index
        total = code_list.__len__()
        batch_size = total // batch
        pool = multiprocessing.Pool(processes=batch)
        results =[]
        for i in range(batch + 1):
            begin_index = i * batch_size
            end_index = (i + 1) * batch_size
            if end_index > total:
                end_index = total
            batch_data = code_list.tolist().__getslice__(begin_index, end_index)
            res = pool.apply_async(ts.get_realtime_quotes, (batch_data,))
            results.append(res)
            # get_stock_hist_data_batch(code_list = batch_data,start=start,end=end,sh_df=sh_df,sz_df=sz_df,cyb_df=cyb_df,table_name=table_name)
        pool.close()
        pool.join()
        #等待执行完毕
        for item in results:
            for i,row in item.iterrows():
             code = row['code']
             sentence = random.choice(SENTENCES)
             storm.logInfo("Emiting %s" % sentence)
             storm.logInfo("Emiting code:%s row:%s" %(code,row))
             storm.emit([code,row])

# Start the spout when it's invoked
FsRealSpout().run()
