# -*- encoding: utf -*-
"""
Create on 2021/1/30 21:49
@author: Xiao Yijia
"""


def list_split_test():
    test_list = ["a", "b", "c", "d"]

    print(test_list[1:3])

    for i, item in enumerate(test_list):
        print(i)

    print(test_list[-1:])
    print(test_list[:1])
    print(type(test_list[-1:]))


if __name__ == '__main__':
    list_split_test()
