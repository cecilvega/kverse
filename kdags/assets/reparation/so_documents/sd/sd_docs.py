import dagster as dg
from kdags.config import DATA_CATALOG

# --- Relative module imports
from kdags.resources.tidyr import DataLake
import polars as pl
from ..extract_utils import *
from ..extra_utils import get_documents


@dg.asset()
def preliminary_report_sd_docs(
    context: dg.AssetExecutionContext, component_reparations: pl.DataFrame, so_documents: pl.DataFrame
):
    reports = get_documents(
        component_reparations=component_reparations,
        so_documents=so_documents,
        subcomponent_tag="5A30",
        file_type="preliminary_report",
    )
    # reports = [
    #     {
    #         "service_order": 14154894,
    #         "component_serial": "109",
    #         "az_path": "az://bhp-raw-data/RESO/DOCUMENTS/y=2025/m=04/d=29/14154894/PRELIMINARY_REPORT/14154894_317069_INI_APRO.pdf",
    #     },
    #     *reports[0:5],
    # ]
    df = extract_tibble_from_report(
        context,
        reports,
        table_title="conclusiones o comentarios",
        table_columns=["conclusiones", "serie del vástago"],
    ).drop(["pages", "section_number"])
    df = (
        pl.concat(
            [
                df.filter((pl.col("index").is_null())),
                df.drop_nulls("index").filter(pl.col("index").str.contains("(?i)conclusiones", strict=False)),
            ],
            how="diagonal",
        )
        .drop("index", strict=False)
        .with_columns(part_name=pl.lit("vastago"), document_type=pl.lit("preliminary_report"))
        .rename({"Serie del vástago": "part_serial", "Conclusiones": "conclusions"})
    )

    return df


@dg.asset()
def final_report_sd_docs(
    context: dg.AssetExecutionContext, component_reparations: pl.DataFrame, so_documents: pl.DataFrame
):
    reports = get_documents(
        component_reparations=component_reparations,
        so_documents=so_documents,
        subcomponent_tag="5A30",
        file_type="final_report",
    )
    # reports = [
    #     {
    #         "service_order": 14155819,
    #         "component_serial": "007",
    #         "az_path": "az://bhp-raw-data/RESO/DOCUMENTS/y=2025/m=06/d=03/14155819/FINAL_REPORT/14155819_319344_FIN_APRO.pdf",
    #     },
    #     *reports[0:5],
    # ]
    df = extract_tibble_from_report(
        context,
        reports,
        table_title="registro de vastago",
        table_columns=["numero de serie de vastago", "tipo de vastago"],
    ).drop(["pages", "section_number"])

    df = (
        # pl.concat(
        #     [
        #         df.filter((pl.col("index").is_null())),
        #         df.drop_nulls("index").filter(pl.col("index").str.contains("(?i)conclusiones", strict=False)),
        #     ],
        #     how="diagonal",
        # )
        # .drop("index", strict=False)
        df.with_columns(part_name=pl.lit("vastago"), document_type=pl.lit("final_report")).rename(
            {"Número de serie de vástago": "part_serial", "Tipo de vástago": "part_status"}
        )
    )
    return df


@dg.asset(compute_kind="mutate")
def mutate_sd_docs(
    context: dg.AssetExecutionContext, preliminary_report_sd_docs: pl.DataFrame, final_report_sd_docs: pl.DataFrame
):
    dl = DataLake(context)

    df = pl.concat([preliminary_report_sd_docs, final_report_sd_docs], how="diagonal")

    dl.upload_tibble(tibble=df, az_path=DATA_CATALOG["sd_docs"]["analytics_path"])
    return df
