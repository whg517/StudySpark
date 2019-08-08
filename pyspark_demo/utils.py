# encoding: utf-8

"""

@Author: wanghuagang
@Contact: huagang517@126.com
@Project: StudySpark
@File:  utils
@Date: 2019/6/28 上午10:55
@Description:

"""
import json


def generator_simple_data():
    data = [
        {'id': 1, 'name': '小红', 'age': 18, 'addr': '上海', 'class': 1, 'birthday': '0511'},
        {'id': 2, 'name': '小明', 'age': 16, 'addr': '北京', 'class': 3, 'birthday': '0622'},
        {'id': 3, 'name': '小亮', 'age': 17, 'addr': '广州', 'class': 4, 'birthday': '0201'},
        {'id': 4, 'name': '小华', 'age': 19, 'addr': '四川', 'class': 2, 'birthday': '0910'},
        {'id': 5, 'name': '韩梅梅', 'age': 22, 'addr': '山东', 'class': 5, 'birthday': '1211'},
        {'id': 6, 'name': '小军', 'age': 17, 'addr': '安徽', 'class': 2, 'birthday': '0625'},
        {'id': 7, 'name': '小高', 'age': 18, 'addr': '西藏', 'class': 3, 'birthday': '0721'},
        {'id': 8, 'name': '小张', 'age': 15, 'addr': '云南', 'class': 1, 'birthday': '1010'},
    ]
    with open('../data/user.json', 'w') as w:
        w.write(json.dumps(data))


if __name__ == '__main__':
    generator_simple_data()
