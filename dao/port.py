# -*- encoding: utf -*-
"""
Create on 2020/10/26 16:39
@author: Xiao Yijia
"""
import arcpy


class Port:

    def __init__(self, unique_id, name, country, point):
        self.unique_id = unique_id
        self.name = name
        self.country = country
        self.point = arcpy.PointGeometry(arcpy.Point(point[0], point[1]))

    def get_x(self):
        return self.point.firstPoint.X

    def get_y(self):
        return self.point.firstPoint.Y


if __name__ == '__main__':
    port = Port(1, "yangshan port", "CN", [0, 0])
    print port.__dict__['name']
