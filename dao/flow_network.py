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

    def init_output_saver(self, list_file_name, matrix_file_name, list_header, ):
        self.list_file, self.list_saver = Utils.init_output_saver(list_file_name, list_header)
        self.matrix_file, self.matrix_saver = Utils.init_output_saver(matrix_file_name, None)

    def export_matrix(self, od_matrix, port_name_list, ):
        height, width = od_matrix.shape
        self.matrix_saver.writerow([''] + port_name_list)
        for i in range(height):
            self.matrix_saver.writerow([port_name_list[i]] + od_matrix[i, :].tolist())

    def export_info(self, od_matrix, port_name_list, ):
        height, width = od_matrix.shape
        for i in range(height):
            for j in range(width):
                if od_matrix[i, j] != 0:
                    self.list_saver.writerow([port_name_list[i], port_name_list[j], od_matrix[i, j]])

    def close(self):
        if self.list_file:
            self.list_file.close()
            self.list_file = None
            self.list_saver = None

        if self.matrix_file:
            self.matrix_file.close()
            self.matrix_file = None
            self.matrix_saver = None
