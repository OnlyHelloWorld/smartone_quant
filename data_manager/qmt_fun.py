
from typing import List

from xtquant import xtdata

sectorList: List = xtdata.get_sector_list()
print(f'获取板块列表数据成功：{sectorList[10]}')


sector_name: str = '沪市基金'
stock_list_in_sector: List = xtdata.get_stock_list_in_sector(sector_name)
print(f'获取板块成分股列表成功：{stock_list_in_sector}')

xtdata.download_sector_data()
sector_list: List = xtdata.get_sector_list()
print(f'获取板块列表数据成功：{sector_list}')
