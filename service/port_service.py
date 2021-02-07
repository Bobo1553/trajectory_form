# -*- encoding: utf -*-
"""
Create on 2020/10/26 16:47
@author: Xiao Yijia
"""
import arcpy

from dao.port import Port


class PortService(object):

    def __init__(self, port_shp_name):
        self.ports = {}
        cursor = arcpy.da.SearchCursor(port_shp_name, ["FID", "portname", "country", "SHAPE"])
        for row in cursor:
            self.ports[row[0]] = Port(row[0], row[1], row[2], row[3])

    def get_nearest_port(self, point, distance_threshold):
        min_distance = distance_threshold
        nearest_port = None
        for port in self.ports.values():
            distance = point.distanceTo(port.point)
            if distance < min_distance:
                min_distance = distance
                nearest_port = port

        return nearest_port

    def get_port_by_name(self, name):
        return self.ports[name]


if __name__ == '__main__':
    port_shp_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\shp\wld_trs_ports_wfp.shp"
    port_service = PortService(port_shp_name)

    point = arcpy.PointGeometry(arcpy.Point(0, 0))
    for port in port_service.ports.values():
        print(port.point.spatialReference.alias)
        print(port.point.firstPoint.X)
        print(port.point.firstPoint.Y)
        print(port.unique_id)
        print(point.distanceTo(port.point))
        break
