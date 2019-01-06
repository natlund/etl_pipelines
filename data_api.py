import json
import lib

from import_pipelines import ProductImportPipeline, BOMImportPipeline
import custom_pipelines


def get_any_custom_etl(plant, entity_name):
    with open('plant_configs.json') as f:
        all_plant_configs = json.load(f)

    plant_configs = all_plant_configs[plant]
    custom_etl_pipes = plant_configs.get('custom_etl', {})
    custom_pipe = custom_etl_pipes.get(entity_name, None)
    if custom_pipe is not None:
        return getattr(custom_pipelines, custom_pipe)
    return None


def transform_and_load_product(plant, table):
    custom_etl_pipe = get_any_custom_etl(plant, lib.EntityNames.product)
    etl_spec = custom_etl_pipe or ProductImportPipeline

    context = lib.get_context_for_plant(plant=plant)
    entities = etl_spec.transform(table, context)
    context.bulk_update('product_repo', entities)


def transform_and_load_bom(plant, table):
    context = lib.get_context_for_plant(plant=plant)
    entities = BOMImportPipeline.transform(table)
    context.bulk_update('bom_repository', entities)
