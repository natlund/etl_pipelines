class DatabaseNames(object):
    camira = 'camira'
    finnebrogue = 'finnebrogue'


class EntityNames(object):
    product = 'product'
    bom = 'bom'


class Product(object):

    def __init__(self, pid, name, unit):
        self.pid = pid
        self.name = name
        self.unit = unit


class DbConnection(object):
    def __init__(self, plant_name):
        self.plant_name = plant_name

    def execute(self, query):
        print('running query on db {}'.format(self.plant_name))
        print(query)

        return [{'pid': 1, 'name': 'bangers', 'unit': 'kgs'},
                {'pid': 2, 'name': 'stuffing', 'unit': 'kgs'}]


class Context(object):
    def __init__(self, plant_name):
        self.plant_name = plant_name

    def bulk_update(self, repo, entities):
        print('Writing entities to repository: {}.{}'.format(self.plant_name, repo))
        for ent in entities:
            print('writing entity {}'.format(ent))


def get_db_connection_for_plant(plant_config):
    return DbConnection(plant_config['db_name'])


def get_context_for_plant(plant):
    return Context(plant)
