# -*- encoding: utf -*-
"""
Create on 2021/1/30 20:28
@author: Xiao Yijia
"""
import arcpy

from util.utils import Utils


class RoutePoint(object):

    def __init__(self, ):
        self.sp_file, self.sp_saver = None, None
        self.route_file, self.route_saver = None, None

        self.temp_still_point_info = []
        self.temp_trajectory_set = []
        self.route_point_set = []
        self.sp_index = 0
        self.route_index = 0

    def init_output_saver(self, sp_file_name, sp_header, route_file_name, route_header, ):
        self.sp_file, self.sp_saver = Utils.init_output_saver(sp_file_name, sp_header)
        self.route_file, self.route_saver = Utils.init_output_saver(route_file_name, route_header)

    def export_route_center(self, rp_point_threshold, rp_acceleration_threshold, rp_avg_heading_threshold):
        if self.save_route_point_set(rp_point_threshold, rp_acceleration_threshold, rp_avg_heading_threshold):
            center_position = Utils.get_center_point([route_point[0] for route_point in self.route_point_set])
            center_point = self.route_point_set[0][0]
            center_point.update_position(center_position[0], center_position[1])
            self.route_saver.writerow([self.sp_index - 1, self.route_index] + center_point.export_to_csv())
            self.route_index += 1

    def export_sp_center(self, sp_area, port_service, distance_threshold):
        center_point = Utils.get_center_point(sp_area)
        port = port_service.get_nearest_port(arcpy.PointGeometry(arcpy.Point(center_point[0], center_point[1])),
                                             distance_threshold)

        if not port:
            self.temp_still_point_info = []
            return False

        temp_still_point_info = self.temp_still_point_info
        port_point = sp_area[0]
        avg_draft = Utils.get_most_value(sp_area, 'draft')
        port_point.update_position(port.get_x(), port.get_y())
        port_point.draft = avg_draft
        self.temp_still_point_info = port_point.export_to_csv() + [port.name]

        if temp_still_point_info:
            self.sp_saver.writerow([self.sp_index] + temp_still_point_info)
            self.sp_saver.writerow([self.sp_index] + self.temp_still_point_info)
            self.sp_index += 1

        return True

    def save_route_point_set(self, rp_point_threshold, rp_acceleration_threshold, rp_avg_heading_threshold):
        route_count = len(self.route_point_set)
        if route_count <= rp_point_threshold:
            return False

        avg_acceleration = 0
        avg_heading_change = 0
        for route_point in self.route_point_set:
            avg_heading_change += route_point[2]
            avg_acceleration += route_point[1]
        avg_acceleration /= route_count
        avg_heading_change /= route_count
        return avg_acceleration < rp_acceleration_threshold and avg_heading_change >= rp_avg_heading_threshold

    def fetch_temp_trajectory_set(self):
        return self.temp_trajectory_set

    def close(self):
        if not self.sp_file:
            self.sp_file.close()
            self.sp_file = None
            self.sp_saver = None

        if not self.route_file:
            self.route_file.close()
            self.route_file = None
            self.route_saver = None
