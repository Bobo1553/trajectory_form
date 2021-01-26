# -*- encoding: utf -*-
"""
Create on 2021/1/16 16:27
@author: Xiao Yijia
"""
import math

from util.Utils import Utils


class StillPointArea(object):

    def __init__(self, ):
        self.output_file = None
        self.output_saver = None

        self.index = 0
        self.still_point_set = []
        self.temp_still_point_set = []

    def init_value(self):
        self.still_point_set = []
        self.temp_still_point_set = []

    def init_output_saver(self, sp_file_name, sp_header):
        self.output_file, self.output_saver = Utils.init_output_saver(sp_file_name, sp_header)

    def append_value(self, after_ship):
        self.still_point_set.append(after_ship)

    def merge_still_point_set(self, trajectory, time_threshold, distance_threshold, ):
        if self.is_suit_for_combine_set(self.temp_still_point_set, self.still_point_set, time_threshold,
                                        distance_threshold, trajectory):
            self.temp_still_point_set += trajectory.point_set[1:] + self.still_point_set
            self.still_point_set = []
            trajectory.point_set = self.temp_still_point_set[-1:]
            return True
        return False

    def is_suit_for_combine_set(self, before_point_set, after_point_set, time_threshold, distance_threshold, trajectory):
        if len(trajectory.point_set) < 2:
            return True

        if not (before_point_set and after_point_set):
            return False

        time_gaps = after_point_set[0].utc - before_point_set[-1].utc
        after_point_center = Utils.get_center_point(after_point_set)
        before_point_center = Utils.get_center_point(before_point_set)
        distance = Utils.calculate_distance_between_items(before_point_center[1], after_point_center[1],
                                                          before_point_center[0],
                                                          after_point_center[0])
        return time_gaps < time_threshold and distance < distance_threshold

    def is_suitable_point_set(self, still_point_set, point_threshold, time_threshold):
        if not still_point_set:
            return False

        return (len(still_point_set) > point_threshold and
                math.fabs(still_point_set[-1].utc - still_point_set[0].utc) > time_threshold)

    def export_temp_still_point_set(self, ):
        self.export_point_set(self.temp_still_point_set)
        self.temp_still_point_set = self.still_point_set
        self.still_point_set = []

    def export_all_point_set(self, point_threshold, time_threshold):
        if self.is_suitable_point_set(self.temp_still_point_set, point_threshold, time_threshold):
            self.export_point_set(self.temp_still_point_set, )
        if self.is_suitable_point_set(self.still_point_set, point_threshold, time_threshold):
            self.export_point_set(self.still_point_set, )
        self.init_value()

    def export_point_set(self, point_set, ):
        for point in point_set:
            self.output_saver.writerow([self.index] + point.export_to_csv())

        self.index += 1

    def finish(self):
        if self.output_file is not None:
            self.output_file.close()
            self.output_file = None
