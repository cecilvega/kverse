import os
from datetime import date
from datetime import datetime
from pathlib import Path

import dagster as dg
import polars as pl

from kdags.assets.reliability.icc.utils import parse_filename, get_shift_dates, extract_technical_report_data
from kdags.config import DATA_CATALOG
from kdags.resources.tidyr import DataLake, MSGraph
from kdags.resources.tidyr import MasterData
from kdags.resources.tidyreports.core import TidyReport


@dg.asset()
def spawn_icc_reports(
    context: dg.AssetExecutionContext,
    mutate_icc: pl.DataFrame,
):
    dl = DataLake(context)
    msgraph = MSGraph(context)
    icc_columns = [
        "equipment_name",
        "component_name",
        "position_name",
        "changeout_date",
    ]
    # mutate_icc = mutate_icc.filter(pl.col("component_name") == "cilindro_levante")
    mutate_icc = mutate_icc.filter(pl.col("equipment_name") == "TK285")
    for group, cc_data in mutate_icc.group_by(icc_columns):

        # Initialize the report
        report = TidyReport(
            filename="reporte_tecnico_faena_escondida.pdf",
            report_title="Informe Cambio Componente",
        )
        component_data = cc_data.filter("subcomponent_main").to_dicts()[0]
        context.log.info(f"Spawning ICC: {component_data['icc_code']}")
        subcomponent_data = cc_data.to_dicts()

        data_path = component_data["data_path"]
        if data_path:
            data_path = component_data["data_path"][0]
            data = dl.read_tibble(data_path, has_header=False)
            data = dict(
                zip(
                    data.select(pl.col("column_1")).to_series().to_list(),
                    data.select(pl.col("column_2")).to_series().to_list(),
                )
            )
            data = {k: v for k, v in data.items() if k is not None and v is not None}
            component_data = {**component_data, **data}

        # Add main title section
        report.add_section("1. Datos Generales")

        report.add_table(
            [
                {
                    "Faena": component_data["site_name"],
                    "Realizado por": component_data.get("author", "Andres Moraleda"),
                    "Correo": component_data.get("author", "andres.moraleda@global.komatsu"),
                },
                {
                    "Fecha CC": component_data["changeout_date"],
                    "Fecha Falla": component_data.get("failure_date", component_data["changeout_date"]),
                },
            ]
        )

        report.add_section("2. Datos de equipo")
        report.add_table(
            [
                {
                    "Equipo": component_data["equipment_name"],
                    "Modelo": component_data["equipment_model"],
                    "Serie": component_data["equipment_serial"],
                },
                {
                    "Horas Equipo": component_data["equipment_hours"],
                },
            ]
        )

        report.add_section("2. Datos de componente")
        # print({"header": "Componente saliente", **cc_data["component_serial"]})
        # print(component_data["icc_code"])
        for subcomponent in subcomponent_data:
            # print(subcomponent)
            report.add_heading(subcomponent["subcomponent_label"])
            report.add_table(
                [
                    {
                        "header": "Componente Saliente",
                        "Serie": subcomponent["component_serial"],
                        "Horas": component_data["component_hours"],
                    },
                    {
                        "header": "Componente Instalado",
                        "Serie": subcomponent["installed_component_serial"],
                        "Horas": 0,
                    },
                ]
            )

        report.add_section("2. Antecedentes")
        report.add_failure_analysis_table(
            {
                "antecedentes": {
                    "changeout_date": component_data["changeout_date"],
                    "component_hours": component_data["component_hours"],
                    "tbo": component_data["tbo"],
                    "utilizacion": component_data["component_usage"],
                    "ot": component_data["customer_work_order"],
                },
                "efecto_falla": component_data.get("efecto_falla", ""),
                "pieza": "Pendiente KRCC",
                "mecanismo_falla": component_data.get("mecanismo_falla", ""),
                "modo_falla": component_data.get("modo_falla", ""),
                "causa_basica": component_data.get("causa_basica", ""),
                "causa_raiz": component_data.get("causa_raiz", ""),
            }
        )

        report.add_section("4. Datos adicionales")

        photos_path = component_data["photos_path"]
        if photos_path:
            images = [
                {
                    "title": az_path.split("/")[-1].split(".", 1)[0],
                    "content": dl.read_bytes(az_path),
                }
                for az_path in photos_path
            ]

            report.add_photos(images, scale_factor=2)
        # Build the PDF
        content = report.build()
        dl.upload_bytes(content, component_data["az_path"])

        context.log.info(f"✅ Reporte técnico generado exitosamente: {component_data["az_path"]}")
    msgraph.upload_tibble(
        mutate_icc,
        "sp://KCHCLGR00058/___/CONFIABILIDAD/Informes Cambio Componente.xlsx",
    )
