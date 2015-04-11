# -*- coding: utf-8 -*-

""" description
utils
"""

__author__ = 'wangfei'
__date__ = '2015/03/06'


def shorten(s, width=80):
    """
    >>> shorten('a very very very very long sentence', 20)
    'a very very ..(23)..'
    """
    if not isinstance(s, str):
        s = str(s)

    length = len(s)
    if length < width:
        return s

    cut_length = length - width + 6
    x = len(str(cut_length))
    cut_length += x

    # 长度调整
    if x != len(str(cut_length)):
        cut_length += 1

    end_pos = length - cut_length
    return s[:end_pos] + '..(%d)..' % cut_length


class Singleton(object):
    """单例模式
    用法:
        class MyClass(Singleton):
            a = 1
            
        one = MyClass()
        two = MyClass()
        
        two.a = 3
        
        print id(one) == id(two)
        # result: True
    用途:
    用单例类ShareObject/GlobalObject 来代替 share.py&settings.py 存储全局变量&全局配置
    """
    def __new__(cls, *args, **kw):
        if not hasattr(cls, '_instance'): 
            cls._instance = super(Singleton, cls).__new__(cls, *args, **kw) # 调用父类的__new__方法生成实例
            
        return cls._instance
    
        
if __name__ == '__main__':
    import doctest
    doctest.testmod()
