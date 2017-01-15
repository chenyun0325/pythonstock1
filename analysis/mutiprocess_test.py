import multiprocessing
import time

def func(msg1 =None,msg2=None):
    for i in range(10):
        print '%s----%s----%s'%(i,msg1,msg2)
        time.sleep(1)
    return "done"+str(msg1)

def main():
    pool = multiprocessing.Pool(processes=4)
    cpu_count =multiprocessing.cpu_count()
    print cpu_count
    result =[]
    for i in range(11,20):
        res=pool.apply_async(func,(i,i,))
        result.append(res)
    pool.close()
    pool.join()
    for x in result:
        print res.get()
    if res.successful():
        print 'suc'


if __name__ == '__main__':
    main()