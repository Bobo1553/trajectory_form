# -*- coding: utf-8 -*-
"""
Create on 2021/2/8 0:34
@author=Xiao Yijia
"""
import numpy as np

from dao.ais_reader import AISReader
from dao.trajectory import Trajectory

# input
input_sp_file_name = r""
input_rp_file_name = r""

# output
directed_info_file = r""
directed_line_file = r""
undirected_info_file = r""
undirected_line_file = r""
info_header = []

# threshold parameter
line_count_threshold = 10

# const
sp_index = 0
sp_offset = 0
sp_label_index = 0

rp_index = 0
rp_offset = 0
rp_label_index = 0


class TrajectoryService(object):

    def __init__(self):
        self.source_sp_center = None
        self.source_rp_center = None

        self.target_directed_trajectory = Trajectory()
        self.target_undirected_trajectory = Trajectory()

        self.mark_dict = {}
        self.position_array = None
        self.line_matrix = None

    def form_trajectory(self):
        self.init_input_output()
        self.form_trajectory_transaction()
        self.finish_deal()

    def init_input_output(self, input_sp_file_name, input_rp_file_name, directed_info_file,
                          directed_line_file, undirected_info_file, undirected_line_file, info_header, ):
        # input
        self.source_sp_center = AISReader(input_sp_file_name, sp_index, sp_offset, label_index=sp_label_index)
        self.source_rp_center = AISReader(input_rp_file_name, rp_index, rp_offset, label_index=rp_label_index)

        self.source_sp_center.start_fetch_data()
        self.source_rp_center.start_fetch_data()

        self.target_directed_trajectory.init_output_saver(directed_info_file, info_header, directed_line_file, None,
                                                          None, )
        self.target_undirected_trajectory.init_output_saver(undirected_info_file, info_header, undirected_line_file,
                                                            None, None, )

    def form_trajectory_transaction(self):
        self.init_matrix()

        self.fill_matrix()

        self.export_matrix()

    def init_matrix(self):
        self.mark_dict = {}

        self.mark_dict, offset = self.source_sp_center.fetch_unique_mark(self.mark_dict, 0)
        self.mark_dict, _ = self.source_rp_center.fetch_unique_mark(self.mark_dict, offset)

        count = len(self.mark_dict.keys())

        self.position_array = np.zeros((count, 3))
        self.line_matrix = np.zeros((count, count))

    def fill_matrix(self):
        print("deal the ship with mmsi: {}".format(self.source_sp_center.ais_point.mmsi))
        while self.source_sp_center.has_next_data() and self.source_rp_center.has_next_data():
            compare = self.source_rp_center.index - self.source_sp_center.index
            if compare < 0:
                print("!!!!!!!!!!!!!!!it would not happened in right situation")
                self.source_rp_center.fetch_data()
            elif compare > 0:
                self.connect_sp_point()
                print("deal the ship with mmsi: {}".format(self.source_sp_center.ais_point.mmsi))
            else:
                self.connect_sp_rp_point()

        while self.source_sp_center.has_next_data():
            self.connect_sp_point()

        self.average_position_array()

    def connect_sp_point(self):
        port_list = self.source_sp_center.fetch_data()
        self.update_matrix_by_point_list(port_list)

    def connect_sp_rp_point(self):
        port_list = self.source_sp_center.fetch_data()
        route_list = self.source_rp_center.fetch_data()

        self.update_matrix_by_point_list([port_list[0]] + route_list + [port_list[-1]])

    def update_matrix_by_point_list(self, point_list):
        for i in range(len(point_list) - 1):
            self.update_line_matrix(point_list[i], point_list[i + 1])
            self.update_position_array(point_list[i])

        self.update_position_array(point_list[-1])

    def update_line_matrix(self, start_info, end_info):
        start_label, end_label = start_info[1], end_info[1]
        start_index, end_index = self.mark_dict[start_label], self.mark_dict[end_info]
        self.line_matrix[start_index, end_index] += 1

    def update_position_array(self, row_info):
        ship_point, label = row_info
        index = self.mark_dict[label]
        self.position_array[index, 0] += ship_point.ship_position.X
        self.position_array[index, 1] += ship_point.ship_position.Y
        self.position_array[index, 2] += 1

    def average_position_array(self):
        for i in range(len(self.position_array)):
            self.position_array[i][0] /= self.position_array[i][2]
            self.position_array[i][1] /= self.position_array[i][2]

    def export_matrix(self):
        self.export_directed_trajectory()

        self.export_undirected_matrix()

    def export_directed_trajectory(self):
        height, width = self.line_matrix.shape
        for i in range(height):
            for j in range(width):
                self.export_single_line(i, j, self.target_directed_trajectory)

    def finish_deal(self):
        self.source_rp_center.close()
        self.source_sp_center.close()

    def export_undirected_matrix(self):
        height, width = self.line_matrix.shape
        for i in range(height):
            for j in range(i, width):
                self.line_matrix[i, j] += self.line_matrix[j, i]
                self.line_matrix[j, i] = 0
                self.export_single_line(i, j, self.target_undirected_trajectory)

    def export_single_line(self, i, j, target_trajectory):
        if self.line_matrix[i, j] > line_count_threshold:
            target_trajectory.line_file.write("{} 0\n".format(target_trajectory.index))
            longitude, latitude, _ = self.position_array[i]
            target_trajectory.line_file.write("0 {} {} 1.#QNAN 1.#QNAN\n".format(i, longitude, latitude))
            longitude, latitude, _ = self.position_array[j]
            target_trajectory.line_file.write("1 {} {} 1.#QNAN 1.#QNAN\n".format(i, longitude, latitude))

            target_trajectory.info_saver.writerow([target_trajectory.index, self.line_matrix[i, j]])

            target_trajectory.index += 1
