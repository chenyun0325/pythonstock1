# -*- coding: utf-8 -*-
import os
from datetime import datetime
from apscheduler.schedulers.background import BlockingScheduler

def my_job():
    with open('apscheduler_test.txt','a+') as f:
        print >> f,'tick! the time is:%s' % datetime.now()
    pf=open('param.txt','r+')
    for line in pf.readlines():
        print 'line:%s' % line
    pf.close()
if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_job(my_job,'interval',seconds=3,id="my_job_id")
    print ('precc Ctrl+{0} to exit'.format('break' if os.name=='nt' else 'C'))
    try:
        scheduler.start()
    except (KeyboardInterrupt,SystemExit):
        print 'clear job ........'
        scheduler.remove_job('my_job_id')
