# -*- coding: utf-8 -*-
"""
Created on Wed Oct 31 16:15:50 2018

@author: Xiao Yijia
"""

import datetime
import math
import time

import arcpy


class Data(object):

    def __init__(self, mark, mmsi):
        self.mmsi = mmsi
        self.mark = mark

    def export_to_csv(self):
        data = [self.mark, self.mmsi]
        return data

    def judge_repeat(self, data):
        if self.mmsi == data.mmsi and self.mark == data.mark:
            return True
        else:
            return False

    def base_data_judge_repeat(self, mark, mmsi):
        if self.mmsi == mmsi and self.mark == mark:
            return True
        else:
            return False

    def set_value_by_item(self, data):
        self.mark = data.mark
        self.mmsi = data.mmsi

    def judge_same_ship(self, data):
        if self.mmsi == data.mmsi and self.mark == data.mark:
            return True
        else:
            return False


class StaticData(Data):

    def __init__(self, mark, mmsi, imo, vessel_name, vessel_type, length, width, dead_weight, gross_weight):
        Data.__init__(self, mark, mmsi)
        self.imo = imo
        self.vessel_name = vessel_name
        self.vessel_type = vessel_type
        self.length = length
        self.width = width
        self.dead_weight = dead_weight
        self.gross_weight = gross_weight

    def export_to_csv(self):
        data = Data.export_to_csv(self)
        data.append(self.imo)
        data.append(self.vessel_name)
        data.append(self.vessel_type)
        data.append(self.length)
        data.append(self.width)
        data.append(self.dead_weight)
        data.append(self.gross_weight)
        return data

    def judge_repeat(self, data):
        if (Data.judge_repeat(self, data) and self.imo == data.imo and self.vessel_name == data.vessel_name and
                self.vessel_type == data.vessel_type and self.length == data.length and self.width == data.width and
                self.dead_weight == data.dead_weight and self.gross_weight == data.gross_weight):
            return True
        else:
            return False

    def static_data_judge_repeat(self, mark, mmsi, imo, vessel_name, vessel_type, length, width, dead_weight,
                                 gross_weight):
        if (Data.base_data_judge_repeat(self, mark, mmsi) and self.imo == imo and self.vessel_name == vessel_name and
                self.vessel_type == vessel_type and self.length == length and self.width == width and
                self.dead_weight == dead_weight and self.gross_weight == gross_weight):
            return True
        else:
            return False

    def set_value_by_item(self, data):
        Data.set_value_by_item(self, data)
        self.imo = data.imo
        self.vessel_name = data.vessel_name
        self.vessel_type = data.vessel_type
        self.length = data.length
        self.width = data.width
        self.dead_weight = data.dead_weight
        self.gross_weight = data.gross_weight


class MovingData(Data):

    def __init__(self, mark, mmsi, longitude, latitude, draught, speed, utc):
        Data.__init__(self, mark, mmsi)
        self.ship_position = arcpy.Point(longitude, latitude)
        self.draught = draught
        self.speed = speed
        if len(str(utc)) == 14:
            self.utc = time_convert(str(utc))
        else:
            self.utc = int(utc)

    def export_to_csv(self):
        data = Data.export_to_csv(self)
        data.append(self.ship_position.X)
        data.append(self.ship_position.Y)
        data.append(self.draught)
        data.append(self.speed)
        data.append(self.utc)
        return data

    def judge_repeat(self, data):
        if (Data.judge_repeat(self, data) and self.ship_position.X == data.ship_position.X and self.ship_position.Y ==
                data.ship_position.Y and self.draught == data.draught and self.speed == data.speed and
                self.utc == data.utc):
            return True
        else:
            return False

    def moving_data_judge_repeat(self, mark, mmsi, longitude, latitude, draught, speed, utc):
        if (Data.base_data_judge_repeat(self, mark, mmsi) and self.ship_position.X == longitude and self.ship_position.Y
                == latitude and self.draught == draught and self.speed == speed and self.utc == utc):
            return True
        else:
            return False

    def judge_in_shp(self, shp_list):
        for shp_item in shp_list:
            if shp_item.contains(self.ship_position):
                return True
        return False

    def set_value_by_item(self, data):
        Data.set_value_by_item(self, data)
        self.ship_position = data.ship_position
        self.draught = data.draught
        self.speed = data.speed
        self.utc = data.utc

    def calculate_distance_to_item(self, point):
        """
        计算两点之间的距离，返回值单位为公里
        :param point:
        :return:
        """
        lat1 = self.ship_position.Y
        lat2 = point.ship_position.Y
        lon1 = self.ship_position.X
        lon2 = point.ship_position.X
        R = 6371
        avgLat = radians(lat1 + lat2) / 2
        disLat = R * math.cos(avgLat) * radians(lon1 - lon2)
        disLon = R * radians(lat1 - lat2)
        return math.sqrt(disLat * disLat + disLon * disLon)

    def calculate_avg_speed_to_item(self, point):
        """
        计算两点之间的平均速度，返回值为节
        :param point:
        :return:
        """
        distance = self.calculate_distance_to_item(point)
        return distance / math.fabs(self.utc - point.utc) * 3600 / 1.852

    def calculate_heading_to_item(self, point):
        """
        计算两个后点到前点的航向
        :param point: 前点
        :return:
        """
        lat1 = self.ship_position.Y
        lat2 = point.ship_position.Y
        lon1 = self.ship_position.X
        lon2 = point.ship_position.X
        numerator = math.sin(radians(lon2 - lon1)) * math.cos(radians(lat2))
        denominator = math.cos(radians(lat1)) * math.sin(radians(lat2)) - math.sin(radians(lat1)) * math.cos(
            radians(lat2)) * math.cos(radians(lon2 - lon1))
        x = math.atan2(math.fabs(numerator), math.fabs(denominator))
        if lon2 > lon1:
            if lat2 > lat1:
                result = x
            elif lat2 < lat1:
                result = math.pi - x
            else:
                result = math.pi / 2
        elif lon2 < lon1:
            if lat2 > lat1:
                result = 2 * math.pi - x
            elif lat2 < lat1:
                result = math.pi + x
            else:
                result = math.pi * 3 / 2
        else:
            if lat2 > lat1:
                result = 0
            elif lat2 < lat1:
                result = math.pi
            else:
                result = - math.pi
                # print("Shouldn't be same location!")
        return result * 180 / math.pi


class DetailData(MovingData):

    def __init__(self, mark, mmsi, imo, vessel_name, vessel_type, length, width, longitude,
                 latitude, draught, speed, utc):
        MovingData.__init__(self, mark, mmsi, longitude, latitude, draught, speed, utc)
        self.imo = imo
        self.vessel_name = vessel_name
        self.vessel_type = vessel_type
        self.length = length
        self.width = width

    def export_to_csv(self):
        data = Data.export_to_csv(self)
        data.append(self.imo)
        data.append(self.vessel_name)
        data.append(self.vessel_type)
        data.append(self.length)
        data.append(self.width)
        data.append(self.ship_position.X)
        data.append(self.ship_position.Y)
        data.append(self.draught)
        data.append(self.speed)
        data.append(self.utc)
        return data

    def judge_repeat(self, data):
        return (MovingData.judge_repeat(self, data) and self.vessel_name == data.vessel_name and self.vessel_type ==
                data.vessel_type and self.imo == data.imo and self.length == data.length and self.width == data.width)

    def detail_data_judge_repeat(self, mark, mmsi, imo, vessel_name, vessel_type, length, width, longitude, latitude,
                                 draught, speed, utc):
        return (MovingData.moving_data_judge_repeat(self, mark, mmsi, longitude, latitude, draught, speed, utc) and
                self.imo == imo and self.vessel_name == vessel_name and self.vessel_type == vessel_type and
                self.length == length and self.width == width)

    def set_value_by_item(self, data):
        MovingData.set_value_by_item(self, data)
        self.imo = data.imo
        self.vessel_name = data.vessel_name
        self.vessel_type = data.vessel_type
        self.length = data.length
        self.width = data.width


class DetailDataWithDeadWeight(DetailData):
    def __init__(self, mark, mmsi, imo, vessel_name, vessel_type, length, width, longitude,
                 latitude, draught, speed, utc, dead_weight, gross_weight, ):
        DetailData.__init__(self, mark, mmsi, imo, vessel_name, vessel_type, length, width, longitude, latitude,
                            draught, speed, utc)
        self.dead_weight = dead_weight
        self.gross_weight = gross_weight

    def export_to_csv(self):
        data = DetailData.export_to_csv(self)
        data.append(self.dead_weight)
        data.append(self.gross_weight)
        return data

    def judge_repeat(self, data):
        return DetailData.judge_repeat(self, data) and self.dead_weight == data.dead_weight and \
               self.gross_weight == data.gross_weight


class DraughtData(DetailData):

    def __init__(self, mark, mmsi, imo, vessel_name, vessel_type, length, width, dead_weight, gross_weight, longitude,
                 latitude, draught, speed, utc, load_state):
        DetailData.__init__(self, mark, mmsi, imo, vessel_name, vessel_type, length, width, dead_weight, gross_weight,
                            longitude, latitude, draught, speed, utc)
        self.load_state = load_state

    def export_to_csv(self):
        data = DetailData.export_to_csv(self)
        data.append(self.load_state)
        return data

    def judge_repeat(self, data):
        if DetailData.judge_repeat(self, data) and self.load_state == data.load_state:
            return True
        else:
            return False

    def draught_data_judge_repeat(self, mark, mmsi, imo, vessel_name, vessel_type, length, width, dead_weight,
                                  gross_weight, longitude, latitude, draught, speed, utc, load_state):
        if (DetailData.detail_data_judge_repeat(self, mark, mmsi, imo, vessel_name, vessel_type, length, width,
                                                dead_weight, gross_weight, longitude, latitude, draught, speed, utc)
                and self.load_state == load_state):
            return True
        else:
            return False

    def set_value_by_item(self, data):
        DetailData.set_value_by_item(self, data)
        self.load_state = data.load_state


class DetailDataWithBuiltTime(DetailDataWithDeadWeight):
    def __init__(self, mark, mmsi, imo, vessel_name, vessel_type, length, width, dead_weight, gross_weight, longitude,
                 latitude, draught, speed, utc, built_time):
        DetailDataWithDeadWeight.__init__(self, mark, mmsi, imo, vessel_name, vessel_type, length, width, dead_weight,
                                          gross_weight, longitude, latitude, draught, speed, utc)
        self.built_time = built_time

    def export_to_csv(self):
        data = DetailDataWithDeadWeight.export_to_csv(self)
        data.append(self.built_time)
        return data

    def set_value_by_item(self, data):
        DetailData.set_value_by_item(self, data)
        self.built_time = data.built_time

    def get_raster_data(self, raster_name):
        position = str(self.ship_position.X) + " " + str(self.ship_position.Y)
        data = arcpy.GetCellValue_management(raster_name, position).getOutput(0)
        return data


def time_convert(str_datetime):
    """
    将字符串的日期转化为utc时间
    :param str_datetime: 为字符串的日期，中间没有横杠间隔，如'19960403112010'
    :return: 返回为数值型的utc时间
    """
    date = datetime.datetime(int(str_datetime[0:4]), int(str_datetime[4:6]), int(str_datetime[6:8]),
                             int(str_datetime[8:10]), int(str_datetime[10:12]), int(str_datetime[12:14]))
    utc_time = int(time.mktime(date.timetuple()))
    return utc_time


def radians(x):
    return x * math.pi / 180


if __name__ == '__main__':
    ship_data = DetailDataWithBuiltTime(1, 1, 1, 1, 1, 1, 1, 1, 1, 200, -43.186, 0, 0, 0, 0)
    print(ship_data.get_raster_data(r'D:\GeoData\waterDepth\waterDepth.tif'))
