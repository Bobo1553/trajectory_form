# -*- encoding: utf -*-
"""
Create on 2021/1/16 16:27
@author: Xiao Yijia
"""
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

    def clean_all_dirty_set(self):
        pass

    def clean_temp_dirty_set(self, ):
        pass

    def merge_still_point_set(self):
        if self.is_suit_for_combine_set(self.temp_still_point_set, self.still_point_set):
            self.temp_still_point_set += self.still_point_set
            return True

    def export_still_point_set(self, output_saver, ):
        self.export_point_set(output_saver, self.still_point_set)
        self.still_point_set = []

    def export_point_set(self, output_saver, point_set):
        if len(point_set) == 0:
            return

        for point in point_set:
            output_saver.writerow([self.index] + point.export_to_csv())

        self.index += 1

    def is_suit_for_combine_set(self, before_point_set, after_point_set):
        time_gaps = after_point_set[0].utc - before_point_set[-1].utc
        after_point_center = self.get_center_point(after_point_set)
        before_point_center = self.get_center_point(before_point_set)
        distance = Utils.calculate_distance_between_items(before_point_center[1], after_point_center[1],
                                                    before_point_center[0],
                                                    after_point_center[0])
        return time_gaps < sp_combine_time_threshold and distance < sp_combine_distance_threshold

    def get_center_point(self, point_set):
        center = [0, 0]
        for point in point_set:
            center[0] += point.ship_position.X
            center[1] += point.ship_position.Y
        center[0] = center[0] * 1.0 / len(point_set)
        center[1] = center[1] * 1.0 / len(point_set)
        return center

    def update_temp_still_point_set(self):
        self.temp_still_point_set += self.still_point_set

    def export_temp_still_point_set(self, output_saver):
        if not self.clean_dirty_set(self.temp_still_point_set):
            self.export_point_set(output_saver, self.temp_still_point_set)
        self.temp_still_point_set = self.still_point_set
        self.still_point_set = []
