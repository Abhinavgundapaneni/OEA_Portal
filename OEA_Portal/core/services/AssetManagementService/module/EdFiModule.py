from ..asset import BaseOEAAsset, OEAAssetFactory

@OEAAssetFactory.register('edfi', 'module')
class EdFiModule(BaseOEAAsset):
    def install(self):
        print('installing edfi module')
        pass
    def update(self):
        pass
    def uninstall(self):
        pass
