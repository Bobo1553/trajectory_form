# -*- encoding: utf -*-
"""
Create on 2021/1/16 16:27
@author: Xiao Yijia
"""
import math

from util.Utils import Utils


class StillPointArea(object):

    def __init__(self):
        self.index = 0
        self.still_point_set = []
        self.temp_still_point_set = []
        self.before_before_still_point_set = []

    def init_value(self):
        self.__init__()

    def append_value(self, after_ship):
        self.still_point_set.append(after_ship)

    def merge_still_point_set(self):
        if self.is_suit_for_combine_set(self.temp_still_point_set, self.still_point_set):
            self.temp_still_point_set += self.still_point_set
            self.still_point_set = []
            return True
        return False

    def is_suit_for_combine_set(self, before_point_set, after_point_set):
        time_gaps = after_point_set[0].utc - before_point_set[-1].utc
        after_point_center = self.get_center_point(after_point_set)
        before_point_center = self.get_center_point(before_point_set)
        distance = Utils.calculate_distance_between_items(before_point_center[1], after_point_center[1],
                                                          before_point_center[0],
                                                          after_point_center[0])
        return time_gaps < sp_combine_time_threshold and distance < sp_combine_distance_threshold

    def clean_all_dirty_set(self):
        self.clean_temp_dirty_set()
        self.clean_still_dirty_set()

    def clean_temp_dirty_set(self, ):
        if not self.is_suitable_point_set(self.temp_still_point_set):
            self.temp_still_point_set = []

    def clean_still_dirty_set(self):
        if not self.is_suitable_point_set(self.still_point_set):
            self.still_point_set = []

    def is_suitable_point_set(self, still_point_set):
        if not still_point_set:
            return False

        return (len(still_point_set) > sp_point_threshold and
                math.fabs(still_point_set[-1].utc - still_point_set[0].utc) > sp_still_time_threshold)

    def export_still_point_set(self, output_saver, ):
        self.export_point_set(output_saver, self.still_point_set)
        self.still_point_set = []

    def export_temp_still_point_set(self, output_saver):
        self.export_point_set(output_saver, self.temp_still_point_set)
        self.temp_still_point_set = self.still_point_set
        self.still_point_set = []

    def export_all_point_set(self, output_saver):
        self.export_point_set(output_saver, self.temp_still_point_set)
        self.export_point_set(output_saver, self.still_point_set)
        self.init_value()

    def export_point_set(self, output_saver, point_set):
        if not self.is_suitable_point_set(point_set):
            return

        for point in point_set:
            output_saver.writerow([self.index] + point.export_to_csv())

        self.index += 1

    # region Utils
    def get_center_point(self, point_set):
        center = [0, 0]
        for point in point_set:
            center[0] += point.ship_position.X
            center[1] += point.ship_position.Y
        center[0] = center[0] * 1.0 / len(point_set)
        center[1] = center[1] * 1.0 / len(point_set)
        return center
    # endregion
