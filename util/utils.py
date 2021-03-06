# -*- encoding: utf -*-
"""
Create on 2021/1/16 21:29
@author: Xiao Yijia
"""
import arcgisscripting
import csv
import math
import os


class Utils(object):

    def __init__(self):
        pass

    @classmethod
    def calculate_distance_between_items(cls, lat1, lat2, lon1, lon2):
        R = 6371
        avgLat = Utils.radians(lat1 + lat2) / 2
        disLat = R * math.cos(avgLat) * Utils.radians(lon1 - lon2)
        disLon = R * Utils.radians(lat1 - lat2)
        return math.sqrt(disLat * disLat + disLon * disLon)

    @classmethod
    def radians(cls, x):
        return x * math.pi / 180

    @classmethod
    def get_center_point(cls, point_set):
        center = [0, 0]
        for point in point_set:
            center[0] += point.ship_position.X
            center[1] += point.ship_position.Y
        center[0] = center[0] * 1.0 / len(point_set)
        center[1] = center[1] * 1.0 / len(point_set)
        return center

    @classmethod
    def init_output_saver(cls, file_name, output_header):
        if not file_name:
            return None, None

        Utils.check_file_path_and_create(file_name)

        output_file = open(file_name, "wb")
        output_saver = csv.writer(output_file)
        if output_header:
            output_saver.writerow(output_header)
        return output_file, output_saver

    @classmethod
    def init_input_reader(cls, file_name):
        input_file = open(file_name)
        input_reader = csv.reader(input_file)
        return input_file, input_reader

    @classmethod
    def check_file_path_and_create(cls, check_file):
        path, filename = os.path.split(check_file)
        Utils.check_path(path)

    @classmethod
    def check_path(cls, file_path):
        if not os.path.exists(file_path):
            os.makedirs(file_path)

    @classmethod
    def create_feature_from_file(cls, input_txt_name, output_shp_name, ):
        gp = arcgisscripting.create()
        gp.CreateFeaturesFromTextFile(input_txt_name, '.', output_shp_name, "#")

    @classmethod
    def get_average_value(cls, point_set, field_name):
        total = 0
        for point in point_set:
            total += float(point.__dict__[field_name])
        return total * 1.0 / len(point_set)

    @classmethod
    def get_most_value(cls, point_set, field_name):
        value_dict = {}
        for point in point_set:
            key = point.__dict__[field_name]
            if key in dict:
                value_dict[key] += 1
            else:
                value_dict[key] = 1

        final_key, final_value = -1, -1
        for key, value in value_dict.items():
            if value > final_value:
                final_key, final_value = key, value
            elif value == final_value and key > final_key:
                final_key = key

        return final_key
