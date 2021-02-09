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
input_sp_file_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step4\FinalStillPoint.csv"
directed_import_count_list_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step5\DirectedImportCountList.csv"
directed_import_count_matrix_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step5\DirectedImportCountMatrix.csv"
directed_export_count_list_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step5\DirectedExportCountList.csv"
directed_export_count_matrix_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step5\DirectedExportCountMatrix.csv"
directed_import_weight_list_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step5\DirectedImportWeightList.csv"
directed_import_weight_matrix_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step5\DirectedImportWeightMatrix.csv"
directed_export_weight_list_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step5\DirectedExportWeightList.csv"
directed_export_weight_matrix_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step5\DirectedExportWeightMatrix.csv"
undirected_import_count_list_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step5\UndirectedImportCountList.csv"
undirected_import_count_matrix_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step5\UndirectedImportCountMatrix.csv"
undirected_export_count_list_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step5\UndirectedExportCountList.csv"
undirected_export_count_matrix_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step5\UndirectedExportCountMatrix.csv"
undirected_import_weight_list_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step5\UndirectedImportWeightList.csv"
undirected_import_weight_matrix_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step5\UndirectedImportWeightMatrix.csv"
undirected_export_weight_list_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step5\UndirectedExportWeightList.csv"
undirected_export_weight_matrix_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step5\UndirectedExportWeightMatrix.csv"
header = ["source_port", "target_port", "value"]

# threshold parameter
change_weight_threshold = 100

# const
sp_index = 0
sp_offset = 1
sp_label_index = 13


class FlowNetworkService(object):

    def __init__(self):
        self.source_sp_center = None

        self.directed_import_count_network = FlowNetwork()
        self.directed_export_count_network = FlowNetwork()
        self.directed_import_weight_network = FlowNetwork()
        self.directed_export_weight_network = FlowNetwork()
        self.undirected_import_count_network = FlowNetwork()
        self.undirected_export_count_network = FlowNetwork()
        self.undirected_import_weight_network = FlowNetwork()
        self.undirected_export_weight_network = FlowNetwork()

        self.mark_dict = {}
        self.import_weight_matrix = None
        self.export_weight_matrix = None
        self.import_count_matrix = None
        self.export_count_matrix = None
        self.port_name_list = []

    def form_flow_network(self, input_sp_file_name, directed_import_count_list_name, directed_import_count_matrix_name,
                          directed_export_count_list_name, directed_export_count_matrix_name,
                          directed_import_weight_list_name, directed_import_weight_matrix_name,
                          directed_export_weight_list_name, directed_export_weight_matrix_name,
                          undirected_import_count_list_name, undirected_import_count_matrix_name,
                          undirected_export_count_list_name, undirected_export_count_matrix_name,
                          undirected_import_weight_list_name, undirected_import_weight_matrix_name,
                          undirected_export_weight_list_name, undirected_export_weight_matrix_name, header):
        self.init_input_output(input_sp_file_name, directed_import_count_list_name, directed_import_count_matrix_name,
                               directed_export_count_list_name, directed_export_count_matrix_name,
                               directed_import_weight_list_name, directed_import_weight_matrix_name,
                               directed_export_weight_list_name, directed_export_weight_matrix_name,
                               undirected_import_count_list_name, undirected_import_count_matrix_name,
                               undirected_export_count_list_name, undirected_export_count_matrix_name,
                               undirected_import_weight_list_name, undirected_import_weight_matrix_name,
                               undirected_export_weight_list_name, undirected_export_weight_matrix_name, header)
        self.form_flow_network_transaction()
        self.finish_deal()

    def init_input_output(self, input_sp_file_name, directed_import_count_list_name, directed_import_count_matrix_name,
                          directed_export_count_list_name, directed_export_count_matrix_name,
                          directed_import_weight_list_name, directed_import_weight_matrix_name,
                          directed_export_weight_list_name, directed_export_weight_matrix_name,
                          undirected_import_count_list_name, undirected_import_count_matrix_name,
                          undirected_export_count_list_name, undirected_export_count_matrix_name,
                          undirected_import_weight_list_name, undirected_import_weight_matrix_name,
                          undirected_export_weight_list_name, undirected_export_weight_matrix_name, header):
        self.source_sp_center = AISReader(input_sp_file_name, sp_index, sp_offset, label_index=sp_label_index)
        self.source_sp_center.start_fetch_data()

        self.directed_import_count_network.init_output_saver(directed_import_count_list_name,
                                                             directed_import_count_matrix_name, header, )
        self.directed_export_count_network.init_output_saver(directed_export_count_list_name,
                                                             directed_export_count_matrix_name, header, )
        self.directed_import_weight_network.init_output_saver(directed_import_weight_list_name,
                                                              directed_import_weight_matrix_name, header, )
        self.directed_export_weight_network.init_output_saver(directed_export_weight_list_name,
                                                              directed_export_weight_matrix_name, header, )

        self.undirected_import_count_network.init_output_saver(undirected_import_count_list_name,
                                                               undirected_import_count_matrix_name, header, )
        self.undirected_export_count_network.init_output_saver(undirected_export_count_list_name,
                                                               undirected_export_count_matrix_name, header, )
        self.undirected_import_weight_network.init_output_saver(undirected_import_weight_list_name,
                                                                undirected_import_weight_matrix_name, header, )
        self.undirected_export_weight_network.init_output_saver(undirected_export_weight_list_name,
                                                                undirected_export_weight_matrix_name, header, )

    def form_flow_network_transaction(self):
        self.init_matrix()

        self.fill_matrix()

        self.export_matrix()

    def init_matrix(self):
        self.mark_dict = {}

        self.mark_dict, offset = self.source_sp_center.fetch_unique_mark(self.mark_dict, 0)

        count = len(self.mark_dict.keys())

        self.import_weight_matrix = np.zeros((count, count))
        self.export_weight_matrix = np.zeros((count, count))
        self.import_count_matrix = np.zeros((count, count))
        self.export_count_matrix = np.zeros((count, count))

        self.get_port_name_list(count)

    def get_port_name_list(self, count):
        self.port_name_list = [0 for _ in range(count)]
        for key, value in self.mark_dict.items():
            self.port_name_list[value] = key

    def fill_matrix(self):
        while self.source_sp_center.has_next_data():
            print("deal the trajectory with index: {}".format(self.source_sp_center.index))
            port_list, _ = self.source_sp_center.fetch_data()
            self.fill_matrix_of_single_trajectory(port_list)

    def fill_matrix_of_single_trajectory(self, port_list):
        start_point, start_label = port_list[0]
        end_point, end_label = port_list[-1]

        # change_weight = calculate_change_dead_weight(start_point, end_point)
        change_weight = 1000
        if math.fabs(change_weight) > change_weight_threshold:
            start_index, end_index = self.mark_dict[start_label], self.mark_dict[end_label]
            if change_weight < 0:
                self.import_weight_matrix[start_index, end_index] += change_weight
                self.import_count_matrix[start_index, end_index] += 1
            else:
                self.export_weight_matrix[start_index, end_index] += -change_weight
                self.export_count_matrix[start_index, end_index] += 1

    def export_matrix(self):
        self.directed_import_count_network.export_info(self.import_count_matrix, self.port_name_list)
        self.directed_import_count_network.export_matrix(self.import_count_matrix, self.port_name_list)
        self.directed_export_count_network.export_info(self.export_count_matrix, self.port_name_list)
        self.directed_export_count_network.export_matrix(self.export_count_matrix, self.port_name_list)
        self.directed_import_weight_network.export_info(self.import_weight_matrix, self.port_name_list)
        self.directed_import_weight_network.export_matrix(self.import_weight_matrix, self.port_name_list)
        self.directed_export_weight_network.export_info(self.export_weight_matrix, self.port_name_list)
        self.directed_export_weight_network.export_matrix(self.export_weight_matrix, self.port_name_list)

        self.form_undirected_matrix()

        self.undirected_import_count_network.export_info(self.import_count_matrix, self.port_name_list)
        self.undirected_import_count_network.export_matrix(self.import_count_matrix, self.port_name_list)
        self.undirected_export_count_network.export_info(self.export_count_matrix, self.port_name_list)
        self.undirected_export_count_network.export_matrix(self.export_count_matrix, self.port_name_list)
        self.undirected_import_weight_network.export_info(self.import_weight_matrix, self.port_name_list)
        self.undirected_import_weight_network.export_matrix(self.import_weight_matrix, self.port_name_list)
        self.undirected_export_weight_network.export_info(self.export_weight_matrix, self.port_name_list)
        self.undirected_export_weight_network.export_matrix(self.export_weight_matrix, self.port_name_list)

    def finish_deal(self):
        self.source_sp_center.close()
        self.directed_import_count_network.close()
        self.directed_export_count_network.close()
        self.directed_import_weight_network.close()
        self.directed_export_weight_network.close()
        self.undirected_import_count_network.close()
        self.undirected_export_count_network.close()
        self.undirected_import_weight_network.close()
        self.undirected_export_weight_network.close()

    def form_undirected_matrix(self):
        height, width = self.import_weight_matrix.shape
        for i in range(height):
            for j in range(i, width):
                self.import_weight_matrix[i, j] += self.import_weight_matrix[j, i]
                self.import_weight_matrix[j, i] = 0
                self.import_count_matrix[i, j] += self.import_count_matrix[j, i]
                self.import_count_matrix[j, i] = 0


if __name__ == '__main__':
    flow_network_service = FlowNetworkService()
    flow_network_service.form_flow_network(
        input_sp_file_name, directed_import_count_list_name, directed_import_count_matrix_name,
        directed_export_count_list_name, directed_export_count_matrix_name,
        directed_import_weight_list_name, directed_import_weight_matrix_name,
        directed_export_weight_list_name, directed_export_weight_matrix_name,
        undirected_import_count_list_name, undirected_import_count_matrix_name,
        undirected_export_count_list_name, undirected_export_count_matrix_name,
        undirected_import_weight_list_name, undirected_import_weight_matrix_name,
        undirected_export_weight_list_name, undirected_export_weight_matrix_name, header
    )
