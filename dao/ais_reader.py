# -*- encoding: utf -*-
"""
Create on 2021/1/26 10:20
@author: Xiao Yijia
"""
import copy

from const.const import Const
from dao.data import DetailData
from util.utils import Utils


class AISReader(object):

    def __init__(self, file_name, index_index, offset, mark_index=None, label_index=None):
        self.ais_point = None
        self.index = None
        self.mark = None
        self.label = None

        self.index_index = index_index
        self.mark_index = mark_index
        self.label_index = label_index
        self.offset = offset

        self.input_file, self.input_reader = Utils.init_input_reader(file_name)

    def start_fetch_data(self, ):
        next(self.input_reader)
        row = self.input_reader.next()
        self.update_value(row)

    def has_next_data(self):
        return self.ais_point

    def fetch_data(self):
        is_suitable_area = False
        if self.label:
            data_list = [[self.ais_point, self.label]]
        else:
            data_list = [self.ais_point]

        for row in self.input_reader:
            if not self.same_sp_area(row):
                self.update_ais_point(row)
                self.update_mark(row)
                return data_list, is_suitable_area
            else:
                self.update_value(row)
                if self.label:
                    data_list.append([self.ais_point, self.label])
                else:
                    data_list.append(self.ais_point)

            if not is_suitable_area and self.is_still_point(row):
                is_suitable_area = True

        self.init_value()
        return data_list, is_suitable_area

    def fetch_unique_mark(self, mark_dict, offset):
        index = offset
        for row in self.input_reader:
            label = row[self.label_index]

            if label not in mark_dict:
                mark_dict[label] = index
                index += 1

        self.input_file.seek(0)
        self.start_fetch_data()

        return mark_dict, index

    def same_sp_area(self, row):
        return int(row[self.index_index]) == self.index

    def update_value(self, row):
        self.update_ais_point(row)
        self.update_mark(row)

    def update_ais_point(self, row):
        self.ais_point = DetailData(row[0 + self.offset], row[1 + self.offset], row[2 + self.offset],
                                    row[3 + self.offset], row[4 + self.offset], row[5 + self.offset],
                                    row[6 + self.offset], row[7 + self.offset], row[8 + self.offset],
                                    row[9 + self.offset], row[10 + self.offset], row[11 + self.offset], )
        self.label = row[self.label_index]

    def update_mark(self, row):
        self.index = int(row[self.index_index])
        if self.mark_index:
            self.mark = int(row[self.mark_index])

    def is_still_point(self, row):
        return self.mark_index and int(row[self.mark_index]) == Const.STILL_POINT_MARK

    def init_value(self):
        self.ais_point = None
        self.index = None
        self.mark = None
        self.label = None

    def close(self):
        self.input_file.close()
