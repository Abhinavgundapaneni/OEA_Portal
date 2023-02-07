from typing import Callable
from abc import ABCMeta, abstractmethod
from OEA_Portal.settings import OEA_ASSETS

class BaseOEAAsset(metaclass=ABCMeta):
    def __init__(self, name, latest_version, min_oea_version):
        self.name = name
        self.latest_version = latest_version
        self.min_oea_version = min_oea_version

    @abstractmethod
    def install(self):
        pass

    @abstractmethod
    def update(self):
        pass

    @abstractmethod
    def uninstall(self):
        pass

class OEAAssetFactory:
    registry = {}
    for asset in OEA_ASSETS: registry[asset] = {}

    def _validate(cls, name: str, asset_type: str, **kwargs):
        if asset_type not in OEA_ASSETS:
            raise Exception(f'OEA doest not support {asset_type} currently.')

    @classmethod
    def register(cls, asset_name: str, asset_type: str) -> Callable:
        def inner_wrapper(wrapped_class: BaseOEAAsset) -> Callable:
            cls._validate(cls, asset_name, asset_type)
            cls.registry[asset_type][asset_name] = wrapped_class
            return wrapped_class

        return inner_wrapper

    @classmethod
    def get_asset(cls, asset_name: str, asset_type: str, **kwargs) -> 'BaseOEAAsset':
        cls._validate(cls, asset_name, asset_type)
        if (asset_name not in cls.registry[asset_type]):
            raise Exception(f"Could not find '{asset_name}' asset of '{asset_type}' type.")
        exec_class = cls.registry[asset_type][asset_name]
        executor = exec_class(**kwargs)
        return executor
