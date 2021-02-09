# -*- coding: utf-8 -*-
"""
Create on 2021/2/8 18:02
@author=Xiao Yijia
"""
import math

import numpy as np

from dao.ais_reader import AISReader
from dao.flow_network import FlowNetwork
from util.calculate_weight import calculate_change_dead_weight

# input output
input_sp_file_name = r""
directed_count_list_name = r""
directed_count_matrix_name = r""
directed_weight_list_name = r""
directed_weight_matrix_name = r""
undirected_count_list_name = r""
undirected_count_matrix_name = r""
undirected_weight_list_name = r""
undirected_weight_matrix_name = r""
header = []

# threshold parameter
change_weight_threshold = 100

# const
sp_index = 0
sp_offset = 0
sp_label_index = 0


class FlowNetworkService(object):

    def __init__(self):
        self.source_sp_center = None

        self.directed_count_network = FlowNetwork()
        self.directed_weight_network = FlowNetwork()
        self.undirected_count_network = FlowNetwork()
        self.undirected_weight_network = FlowNetwork()

        self.mark_dict = {}
        self.weight_matrix = None
        self.count_matrix = None
        self.port_name_list = []

    def form_flow_network(self, input_sp_file_name, directed_count_list_name, directed_count_matrix_name,
                          directed_weight_list_name, directed_weight_matrix_name, undirected_count_list_name,
                          undirected_count_matrix_name, undirected_weight_list_name, undirected_weight_matrix_name,
                          header):
        self.init_input_output(input_sp_file_name, directed_count_list_name, directed_count_matrix_name,
                               directed_weight_list_name, directed_weight_matrix_name, undirected_count_list_name,
                               undirected_count_matrix_name, undirected_weight_list_name, undirected_weight_matrix_name,
                               header)
        self.form_flow_network_transaction()
        self.finish_deal()

    def init_input_output(self, input_sp_file_name, directed_count_list_name, directed_count_matrix_name,
                          directed_weight_list_name, directed_weight_matrix_name, undirected_count_list_name,
                          undirected_count_matrix_name, undirected_weight_list_name, undirected_weight_matrix_name,
                          header):
        self.source_sp_center = AISReader(input_sp_file_name, sp_index, sp_offset, label_index=sp_label_index)
        self.source_sp_center.start_fetch_data()

        self.directed_count_network.init_output_saver(directed_count_list_name, header, directed_count_matrix_name, )
        self.directed_weight_network.init_output_saver(directed_weight_list_name, header, directed_weight_matrix_name, )
        self.undirected_count_network.init_output_saver(undirected_count_list_name, header,
                                                        undirected_count_matrix_name, )
        self.undirected_weight_network.init_output_saver(undirected_weight_list_name, header,
                                                         undirected_weight_matrix_name, )

    def form_flow_network_transaction(self):
        self.init_matrix()

        self.fill_matrix()

        self.export_matrix()

    def init_matrix(self):
        self.mark_dict = {}

        self.mark_dict, offset = self.source_sp_center.fetch_unique_mark(self.mark_dict, 0)

        count = len(self.mark_dict.keys())

        self.weight_matrix = np.zeros((count, count))
        self.count_matrix = np.zeros((count, count))

        self.get_port_name_list(count)

    def get_port_name_list(self, count):
        self.port_name_list = [0 for _ in range(count)]
        for key, value in self.mark_dict:
            self.port_name_list[value] = key

    def fill_matrix(self):
        while self.source_sp_center.has_next_data():
            print("deal the trajectory with index: {}".format(self.source_sp_center.index))
            port_list, _ = self.source_sp_center.fetch_data()
            self.fill_matrix_of_single_trajectory(port_list)

    def fill_matrix_of_single_trajectory(self, port_list):
        start_point, start_label = port_list[0]
        end_point, end_label = port_list[-1]

        change_weight = calculate_change_dead_weight(start_point, end_point)

        if math.fabs(change_weight) > change_weight_threshold:
            start_index, end_index = self.mark_dict[start_label], self.mark_dict[end_label]
            self.weight_matrix[start_index, end_index] += change_weight
            self.count_matrix[start_index, end_index] += 1

    def export_matrix(self):
        self.directed_count_network.export_info(self.count_matrix, self.port_name_list)
        self.directed_count_network.export_matrix(self.count_matrix, self.port_name_list)
        self.directed_weight_network.export_info(self.weight_matrix, self.port_name_list)
        self.directed_weight_network.export_matrix(self.weight_matrix, self.port_name_list)

        self.form_undirected_matrix()

        self.undirected_count_network.export_info(self.count_matrix, self.port_name_list)
        self.undirected_count_network.export_matrix(self.count_matrix, self.port_name_list)
        self.undirected_weight_network.export_info(self.weight_matrix, self.port_name_list)
        self.undirected_weight_network.export_matrix(self.weight_matrix, self.port_name_list)

    def finish_deal(self):
        self.source_sp_center.close()
        self.directed_count_network.close()
        self.directed_weight_network.close()
        self.undirected_count_network.close()
        self.undirected_weight_network.close()

    def form_undirected_matrix(self):
        height, width = self.weight_matrix.shape
        for i in range(height):
            for j in range(i, width):
                self.weight_matrix[i, j] += self.weight_matrix[j, i]
                self.weight_matrix[j, i] = 0
                self.count_matrix[i, j] += self.count_matrix[j, i]
                self.count_matrix[j, i] = 0


if __name__ == '__main__':
    flow_network_service = FlowNetworkService()
    flow_network_service.form_flow_network(
        input_sp_file_name, directed_count_list_name, directed_count_matrix_name, directed_weight_list_name,
        directed_weight_matrix_name, undirected_count_list_name, undirected_count_matrix_name,
        undirected_weight_list_name, undirected_weight_matrix_name, header
    )
