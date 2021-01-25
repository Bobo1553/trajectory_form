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
        self.is_ship_beginning = True

    def init_output_saver(self, trajectory_file_name, trajectory_header, trajectory_txt_name):
        self.output_file, self.output_saver = Utils.init_output_saver(trajectory_file_name, trajectory_header)
        self.info_file = open(trajectory_txt_name, "w")
        self.info_file.write("Polyline\n")

    def init_value(self):
        self.temp_point_set = []
        self.point_set = []
        self.index = 0
        self.is_ship_beginning = True

    def export_trajectory_point(self, point_set):
        if not self.is_suitable_point_set(point_set):
            return

        self.info_file.write("{} 0\n".format(self.index))
        for i, point in enumerate(point_set):
            # print(point)
            self.info_file.write("{} {} {} 1.#QNAN 1.#QNAN\n".format(i, point.ship_position.X, point.ship_position.Y))

        point = point_set[0]
        self.output_saver.writerow([self.index, point.mark, point.mmsi, point.imo, point.vessel_name, point.vessel_type,
                                    point.length, point.width])
        self.index += 1

    def export_temp_trajectory_point(self, end_point):
        if self.is_ship_beginning:
            self.is_ship_beginning = False
        else:
            self.export_trajectory_point(self.temp_point_set + [end_point])
        self.temp_point_set = self.point_set
        self.point_set = []

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
        if self.output_file is not None:
            self.output_file.close()
            self.output_file = None

        if self.info_file is not None:
            self.info_file.write("END\n")
            self.info_file.close()
            self.info_file = None
