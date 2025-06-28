import polars as pl


def get_documents(
    component_reparations: pl.DataFrame, so_documents: pl.DataFrame, subcomponent_tag: str, file_type: str
):
    cr_df = component_reparations.filter(pl.col("subcomponent_tag") == subcomponent_tag)
    so_docs_df = so_documents.drop_nulls("file_size")
    records_list = (
        cr_df.join(
            so_docs_df.filter(pl.col("file_type") == file_type)
            # .select(["service_order", "component_serial", "az_path"])
            .unique(subset=["service_order", "component_serial"]),
            how="inner",
            on=["service_order", "component_serial"],
        )
        .sort("reception_date")
        .select(["service_order", "component_serial", "az_path"])
        .tail(5)
        .to_dicts()
    )
    return records_list
