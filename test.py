# -*- encoding: utf -*-
"""
Create on 2021/1/18 20:55
@author: Xiao Yijia
"""

if __name__ == '__main__':
    test_list = ["a", "b", "c", "d"]
    for i, item in enumerate(test_list):
        print(i)

    print(test_list[-1:])
    print(test_list[:1])
    print(type(test_list[-1:]))
