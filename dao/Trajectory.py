# -*- encoding: utf -*-
"""
Create on 2021/1/18 17:10
@author: Xiao Yijia
"""
from util.Utils import Utils


class Trajectory(object):

    def __init__(self):
        self.output_file = None
        self.output_saver = None
        self.info_file = None

        self.temp_point_set = []
        self.point_set = []
        self.index = 0

    def init_output_saver(self, trajectory_file_name, trajectory_header, trajectory_txt_name):
        self.output_file, self.output_saver = Utils.init_output_saver(trajectory_file_name, trajectory_header)
        self.info_file = open(trajectory_txt_name, "w")
        self.info_file.write("Polyline\n")

    def init_value(self):
        self.temp_point_set = []
        self.point_set = []
        self.index = 0

    def finish(self):
        if self.output_file is not None:
            self.output_file.close()
            self.output_file = None

        if self.info_file is not None:
            self.info_file.write("END\n")
            self.info_file.close()
            self.info_file = None

    def export_trajectory_point(self, point_set):
        if not self.is_suitable_point_set(point_set):
            return

        self.info_file.write("{} 0\n".format(self.index))
        for i, point in enumerate(point_set):
            self.output_saver.writerow(point.export_to_csv())
            self.info_file.write("{} {} {} 1.#QNAN 1.#QNAN\n".format(i, point.X, point.Y))

        self.index += 1

    def export_temp_trajectory_point(self, end_point):
        self.export_trajectory_point(self.temp_point_set + [end_point])
        self.temp_point_set = self.point_set
        self.point_set = []

    def update_temp_trajectory_point(self, sp_set):
        self.temp_point_set += sp_set + self.point_set
        self.point_set = []

    def is_suitable_point_set(self, point_set):
        return len(point_set) != 0

    def append_value(self, ship_point):
        self.point_set.append(ship_point)
