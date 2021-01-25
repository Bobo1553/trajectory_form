# -*- encoding: utf -*-
"""
Create on 2021/1/18 17:10
@author: Xiao Yijia
"""
from util.Utils import Utils


class Trajectory(object):

    def __init__(self):
        self.info_file = None
        self.info_saver = None
        self.line_file = None
        self.point_file = None
        self.point_saver = None

        self.temp_point_set = []
        self.point_set = []
        self.index = 0
        self.is_ship_beginning = True

    def init_output_saver(self, trajectory_file_name, trajectory_header, trajectory_txt_name, trajectory_point_name,
                          trajectory_point_header):
        self.info_file, self.info_saver = Utils.init_output_saver(trajectory_file_name, trajectory_header)
        self.point_file, self.point_saver = Utils.init_output_saver(trajectory_point_name, trajectory_point_header)
        self.line_file = open(trajectory_txt_name, "w")
        self.line_file.write("Polyline\n")

    def init_value(self):
        self.temp_point_set = []
        self.point_set = []
        self.is_ship_beginning = True

    def export_temp_trajectory_point(self, end_point, sp_index):
        if self.is_ship_beginning:
            self.is_ship_beginning = False
        else:
            self.export_trajectory(self.temp_point_set + [end_point], sp_index)
        self.temp_point_set = self.point_set
        self.point_set = []

    def export_trajectory(self, point_set, sp_index, ):
        if not self.is_suitable_point_set(point_set):
            return

        self.export_trajectory_info(point_set[0], sp_index)
        self.export_trajectory_line(point_set)
        self.export_trajectory_point(point_set, sp_index)

        self.index += 1

    def export_trajectory_info(self, point, sp_index):
        self.info_saver.writerow([self.index, sp_index, point.mark, point.mmsi, point.imo, point.vessel_name,
                                  point.vessel_type, point.length, point.width])

    def export_trajectory_line(self, point_set):
        self.line_file.write("{} 0\n".format(self.index))
        for i, point in enumerate(point_set):
            self.line_file.write("{} {} {} 1.#QNAN 1.#QNAN\n".format(i, point.ship_position.X, point.ship_position.Y))

    def export_trajectory_point(self, point_set, sp_index):
        for point in point_set[1:-1]:
            self.point_saver.writerow([self.index, sp_index] + point.export_to_csv())

    def update_temp_trajectory_point(self, sp_area):
        self.temp_point_set += sp_area.temp_still_point_set + self.point_set
        self.point_set = []

        sp_area.temp_still_point_set = sp_area.still_point_set
        sp_area.still_point_set = []

    def is_suitable_point_set(self, point_set):
        return point_set

    def append_value(self, ship_point):
        self.point_set.append(ship_point)

    def finish(self):
        if self.info_file is not None:
            self.info_file.close()
            self.info_file = None
            self.info_saver = None

        if self.line_file is not None:
            self.line_file.write("END\n")
            self.line_file.close()
            self.line_file = None

        if self.point_file is not None:
            self.point_file.close()
            self.point_saver = None
            self.point_file = None

    def remove_last_value(self):
        if self.point_set:
            self.point_set = self.point_set[:-1]
