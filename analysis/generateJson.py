import tushare as ts
if __name__ == '__main__':
    bs=ts.get_stock_basics()
    join_str=','.join(bs.index.tolist())
    print join_str