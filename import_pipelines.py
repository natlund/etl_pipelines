import abc

from lib import Product


class ImportPipeline(abc.ABC):

    @staticmethod
    @abc.abstractmethod
    def repository():
        pass

    @classmethod
    @abc.abstractmethod
    def extract_from_db(cls, db_connection):
        pass

    @classmethod
    @abc.abstractmethod
    def transform(cls, table, context):
        pass


class SimpleImportPipeline(ImportPipeline):

    @staticmethod
    @abc.abstractmethod
    def query():
        pass

    @classmethod
    def extract_from_db(cls, db_connection):
        table = db_connection.execute(cls.query())
        return table


class ProductImportPipeline(SimpleImportPipeline):

    @staticmethod
    def repository():
        return "product_repository"

    @staticmethod
    def query():
        return "SELECT * FROM client_product"

    @classmethod
    def transform(cls, table, context):
        entities = []

        for row in table:
            entity = cls.row_mapper(row)
            entities.append(entity)

        return entities

    @staticmethod
    def row_mapper(row):
        pid = row['pid']
        name = row['name']
        unit = row['unit']

        return Product(pid, name, unit)

    @staticmethod
    def validator(entities):
        return entities


class BOMImportPipeline(ImportPipeline):

    @staticmethod
    def repository():
        return 'bom_repository'

    @classmethod
    def extract_from_db(cls, db_connection):
        print('Run a not-so-simple import.  Eg. do multiple database calls.')
        print('This should assemble a single table that can be passed to the transform method.')

    @classmethod
    def transform(cls, table, context):
        print('Run a not so simple transform.')
        return []
