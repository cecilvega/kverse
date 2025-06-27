import dagster as dg
from kdags.config import DATA_CATALOG

# --- Relative module imports
from kdags.resources.tidyr import DataLake
import polars as pl


@dg.asset(group_name="reparation")
def raw_so_documents(context: dg.AssetExecutionContext):
    dl = DataLake(context)
    # try:
    df = dl.read_tibble(DATA_CATALOG["so_documents"]["raw_path"])
    # except Exception as e:
    #     df = pl.DataFrame(schema=DOCUMENTS_LIST_SCHEMA)

    return df


@dg.asset(group_name="reparation", compute_kind="mutate")
def mutate_so_documents(context: dg.AssetExecutionContext, raw_so_documents: pl.DataFrame, so_report: pl.DataFrame):
    dl = DataLake(context)
    downloaded_documents = dl.list_paths("az://bhp-raw-data/RESO/DOCUMENTS").select(
        ["az_path", "last_modified", "file_size"]
    )
    df = raw_so_documents.clone()
    df = df.join(so_report.select(["service_order", "reception_date"]), how="left", on="service_order")
    df = (
        df.with_columns(_file_title=pl.col("file_title").str.to_lowercase())
        .with_columns(
            file_type=pl.when(pl.col("_file_title").str.contains("carta presupuesto"))
            .then(pl.lit("quotation"))
            .when(pl.col("_file_title").str.contains("informe tecnico preliminar"))
            .then(pl.lit("preliminary_report"))
            .when(pl.col("_file_title").str.contains("informe tecnico final"))
            .then(pl.lit("final_report"))
            .otherwise(pl.lit("other"))
        )
        .drop("_file_title")
        .with_columns(
            # Extract year, month, day from reception_date
            year=pl.col("reception_date").dt.year(),
            month=pl.col("reception_date").dt.month(),
            day=pl.col("reception_date").dt.day(),
        )
        .with_columns(
            pl.when(pl.col("file_type") == "quotation")
            .then(
                pl.concat_str(
                    [
                        pl.lit("az://bhp-raw-data/RESO/DOCUMENTS/"),
                        pl.lit("y="),
                        pl.col("year").cast(pl.Utf8),
                        pl.lit("/m="),
                        pl.col("month").cast(pl.Utf8).str.zfill(2),
                        pl.lit("/d="),
                        pl.col("day").cast(pl.Utf8).str.zfill(2),
                        pl.lit("/"),
                        pl.col("service_order"),
                        pl.lit("/QUOTATIONS/"),
                        pl.col("file_name"),
                    ]
                )
            )
            .when(pl.col("file_type") == "preliminary_report")
            .then(
                pl.concat_str(
                    [
                        pl.lit("az://bhp-raw-data/RESO/DOCUMENTS/"),
                        pl.lit("y="),
                        pl.col("year").cast(pl.Utf8),
                        pl.lit("/m="),
                        pl.col("month").cast(pl.Utf8).str.zfill(2),
                        pl.lit("/d="),
                        pl.col("day").cast(pl.Utf8).str.zfill(2),
                        pl.lit("/"),
                        pl.col("service_order"),
                        pl.lit("/PRELIMINARY_REPORT/"),
                        pl.col("file_name"),
                    ]
                )
            )
            .when(pl.col("file_type") == "final_report")
            .then(
                pl.concat_str(
                    [
                        pl.lit("az://bhp-raw-data/RESO/DOCUMENTS/"),
                        pl.lit("y="),
                        pl.col("year").cast(pl.Utf8),
                        pl.lit("/m="),
                        pl.col("month").cast(pl.Utf8).str.zfill(2),
                        pl.lit("/d="),
                        pl.col("day").cast(pl.Utf8).str.zfill(2),
                        pl.lit("/"),
                        pl.col("service_order"),
                        pl.lit("/FINAL_REPORT/"),
                        pl.col("file_name"),
                    ]
                )
            )
            .otherwise(
                pl.concat_str(
                    [
                        pl.lit("az://bhp-raw-data/RESO/DOCUMENTS/"),
                        pl.lit("y="),
                        pl.col("year").cast(pl.Utf8),
                        pl.lit("/m="),
                        pl.col("month").cast(pl.Utf8).str.zfill(2),
                        pl.lit("/d="),
                        pl.col("day").cast(pl.Utf8).str.zfill(2),
                        pl.lit("/"),
                        pl.col("service_order"),
                        pl.lit("/OTHER/"),
                        pl.col("file_name"),
                    ]
                )
            )
            .alias("az_path")
        )
        .drop(["year", "month", "day"])  # Clean up temporary columns
    )

    df = df.join(downloaded_documents, on="az_path", how="left")

    dl.upload_tibble(tibble=df, az_path=DATA_CATALOG["so_documents"]["analytics_path"])
    return df
