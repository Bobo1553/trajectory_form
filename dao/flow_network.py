# -*- coding: utf-8 -*-
"""
Create on 2021/2/8 19:53
@author=Xiao Yijia
"""
from util.utils import Utils


class FlowNetwork(object):

    def __init__(self):
        self.list_file, self.list_saver = None, None
        self.matrix_file, self.matrix_saver = None, None

    def init_output_saver(self):
        self.list_file, self.list_saver = Utils.init_output_saver("", "")
        self.matrix_file, self.matrix_saver = Utils.init_output_saver("", None)

    def export_matrix(self, od_matrix, port_name_list,):
        height, width = od_matrix.shape
        self.matrix_saver.writerow([''] + port_name_list)
        for i in range(height):
            self.matrix_saver.writerow([port_name_list[i]] + od_matrix[i, :].tolist())

    def export_info(self, od_matrix, header, port_name_list, ):
        self.list_saver.writerow(header)
        height, width = od_matrix.shape
        for i in range(height):
            for j in range(width):
                if od_matrix[i, j] != 0:
                    self.list_saver.writerow([port_name_list[i], port_name_list[j], od_matrix[i, j]])
