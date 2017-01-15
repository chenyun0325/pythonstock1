def test():
    x = 0
    try:
        x=1
        x/0
    except Exception,e:
        print e
    finally:
        return x
if __name__ == '__main__':
    y=test()
    print y