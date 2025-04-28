import os
import importlib
import inspect
import pkgutil
import sys
from pathlib import Path
import yaml
import polars as pl
import dagster as dg  # Use dg alias consistently

# Assuming your project structure allows importing from kdags and config
# Adjust these paths if your execution context is different
# These paths are relative to the 'kverse' directory usually
KDAGS_ROOT_PATH = Path(__file__).parent.parent.parent  # Should point to kverse/kdags/
SCHEMAS_PACKAGE_PATH = KDAGS_ROOT_PATH / "schemas"
CONFIG_ROOT_PATH = KDAGS_ROOT_PATH.parent / "config"
TRANSLATION_FILE_PATH = CONFIG_ROOT_PATH / "translations.yaml"  # Simplified YAML

from kdags.resources.tidyr import MSGraph


def find_and_load_modules(package_path: Path):
    """
    Dynamically finds and imports all modules within a given package path.
    Errors during import will propagate.
    """
    modules = []
    package_parent_dir = str(package_path.parent.resolve())
    if package_parent_dir not in sys.path:
        sys.path.insert(0, package_parent_dir)

    base_import_path = ".".join(package_path.parts[len(KDAGS_ROOT_PATH.parent.parts) :])

    for _, modname, ispkg in pkgutil.walk_packages(
        path=[str(package_path)],
        prefix=base_import_path + ".",
        onerror=lambda x: print(f"Warning: Error importing module {x}"),  # Basic warning
    ):
        if not ispkg:
            # Allow import errors to propagate
            module = importlib.import_module(modname)
            modules.append(module)
    return modules


def find_schemas_in_module(module):
    """Finds TableSchema instances within a given module."""
    schemas = []
    for name, obj in inspect.getmembers(module):
        if isinstance(obj, dg.TableSchema):
            schemas.append((name, obj))
    return schemas


def load_translations(yaml_path: Path):
    """
    Loads the simplified translation YAML file.
    Returns empty dict if file not found, propagates YAML parsing errors.
    """
    if not yaml_path.is_file():
        print(f"Warning: Translation file not found at {yaml_path}. Returning empty map.")
        return {}
    # Let YAML parsing errors propagate
    with open(yaml_path, "r", encoding="utf-8") as f:
        translations = yaml.safe_load(f)
        return translations if isinstance(translations, dict) else {}


@dg.asset
def mutate_schema_catalog(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """
    Dagster asset that discovers TableSchemas, loads translations,
    and generates a consolidated Polars DataFrame catalog including the group name.
    """
    all_columns_data = []
    translation_map = load_translations(TRANSLATION_FILE_PATH)
    schema_modules = find_and_load_modules(SCHEMAS_PACKAGE_PATH)

    if not schema_modules:
        print("Warning: No schema modules found or loaded. Returning empty catalog.")
        return pl.DataFrame()

    total_schemas_found = 0
    for module in schema_modules:
        schemas = find_schemas_in_module(module)
        total_schemas_found += len(schemas)

        module_parts = module.__name__.split(".")
        group_name = module_parts[2] if len(module_parts) > 2 else "unknown"
        if group_name == "schemas":
            group_name = "general"

        for schema_variable_name, schema_obj in schemas:
            if isinstance(schema_obj, dg.TableSchema):
                schema_identifier = schema_variable_name
                for column in schema_obj.columns:
                    if not isinstance(column, dg.TableColumn):
                        print(f"Warning: Item {column} in schema {schema_identifier} is not a TableColumn. Skipping.")
                        continue

                    english_name = column.name
                    spanish_name = translation_map.get(english_name, english_name)

                    all_columns_data.append(
                        {
                            "group_name": group_name,
                            "schema_name": schema_identifier,
                            "column_name": english_name,
                            "type": column.type,
                            "description": column.description,
                            "spanish_name": spanish_name,
                        }
                    )

    if not all_columns_data:
        print("Warning: No columns found in any discovered schemas. Returning empty catalog.")
        return pl.DataFrame()

    column_order = ["group_name", "schema_name", "column_name", "type", "description", "spanish_name"]
    df_catalog = pl.DataFrame(all_columns_data).select(column_order)

    # Removed context.add_output_metadata

    return df_catalog  # Directly return the DataFrame


# --- Asset to publish the catalog to SharePoint ---


@dg.asset(
    description="Publishes the generated schema catalog DataFrame to SharePoint as an Excel file.",
    # compute_kind="sharepoint",
)
def publish_sp_schema_catalog(
    context: dg.AssetExecutionContext,
    mutate_schema_catalog: pl.DataFrame,
):
    msgraph = MSGraph()
    upload_results = []
    sp_paths = [
        # "sp://KCHCLGR00058/___/DOCS/catalogo_datos.xlsx",
        "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/DOCS/catalogo_datos.xlsx",
    ]
    for sp_path in sp_paths:
        context.log.info(f"Publishing to {sp_path}")
        upload_results.append(msgraph.upload_tibble(tibble=mutate_schema_catalog, sp_path=sp_path))
    return upload_results
