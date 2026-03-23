# -*- coding: utf-8 -*-
"""
数据源适配器兼容层

当前项目已统一切换到 Futu 主数据源实现。
保留本文件仅为兼容历史导入路径 `data.data_source_v2.DataSource`。
"""
from .data_source import DataSource as BaseDataSource


class DataSource(BaseDataSource):
    """兼容旧版命名的数据源适配器"""

    pass
