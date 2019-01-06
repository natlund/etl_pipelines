from import_pipelines import ImportPipeline


class CamiraBOMImporter(ImportPipeline):

    @staticmethod
    def repository():
        return 'bom_repository'

    @classmethod
    def extract_from_db(cls, db_connection):
        print('Run a custom extraction from Camira database.')

    @classmethod
    def transform(cls, table, context):
        print('Run custom transform for Camira data, which has special requirements.')
        return []

    @staticmethod
    def run(context):
        print('Run custom import for camira, which has special requirements.')
