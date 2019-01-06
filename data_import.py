import json

import lib
from import_pipelines import SimpleImportPipeline
from import_pipelines import (ProductImportPipeline,
                              BOMImportPipeline)
import custom_pipelines


etl_lookup = {lib.EntityNames.product: ProductImportPipeline,
              lib.EntityNames.bom: BOMImportPipeline}


def import_from_database(plant, entities_to_import):
    with open('plant_configs.json') as config_file:
        all_plant_configs = json.load(config_file)

    if not valid_input(all_plant_configs, plant, entities_to_import):
        return

    plant_config = all_plant_configs[plant]

    etl_specs = get_plant_pipelines(plant_config, entities_to_import)

    context = lib.get_context_for_plant(plant)
    db_connection = lib.get_db_connection_for_plant(plant_config)

    for entity_name, spec in etl_specs:
        print("\nImporting {}".format(entity_name))

        # Extract
        table = spec.extract_from_db(db_connection)

        # Transform
        transformed_entities = spec.transform(table, context)

        # Load
        repo = spec.repository()
        context.bulk_update(repo, transformed_entities)

    # run_imports_for_plant(db_connection, context, etl_specs)


def valid_input(all_plant_configs, plant, entities_to_import):

    if plant not in all_plant_configs:
        print('Plant not recognised: {}'.format(plant))
        return False

    for ent in entities_to_import:
        if ent not in etl_lookup:
            print("Entity '{}' not recognised as one of: {}".format(ent, list(etl_lookup.keys())))
            return False

    return True


def get_plant_pipelines(plant_config, entities_to_import):

    # Get actual ImportSpec classes from entity strings
    custom_etl_names = plant_config.get('custom_etl', {})
    etl_specs = []
    for entity in entities_to_import:
        if entity in custom_etl_names:
            etl_spec_name = custom_etl_names[entity]
            etl_spec = getattr(custom_pipelines, etl_spec_name)
        else:
            etl_spec = etl_lookup[entity]

        etl_specs.append((entity, etl_spec))

    return etl_specs

#
# def run_imports_for_plant(db_connection, context, entities_to_import):
#
#     for spec_name, sp in entities_to_import:
#         print("\nImporting {}".format(spec_name))
#         spec = sp()
#         if isinstance(spec, SimpleImportSpecs):
#             simple_import(db_connection, context, spec)
#         else:
#             spec.run(context)
#
#
# def simple_import(db_connection, context, etl_spec):
#
#     # Extract
#     query = etl_spec.query()
#     table = db_connection.execute(query)
#
#     # Transform
#     valid_entities = etl_spec.transform(table, context)
#
#     # Load
#     repo = etl_spec.repository()
#     context.bulk_update(repo, valid_entities)


if __name__ == '__main__':

    import_from_database('camira', ['product', 'bom'])
