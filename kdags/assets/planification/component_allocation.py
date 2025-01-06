# Standard library imports
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

# Third-party imports
import pandas as pd
import numpy as np
from dataclasses import dataclass

# Constants

CHANGEOUT_START_DATE = pd.Timestamp(year=2024, month=7, day=29)


@dataclass(frozen=True)
class ComponentCode:
    code: str
    name: str
    planned_ovh_days: int
    unplanned_ovh_days: int

    def __str__(self) -> str:
        return self.code


class ComponentType(Enum):
    BP = ComponentCode("blower_parrilla", "Blower", 51, 101)
    CD = ComponentCode("cilindro_direccion", "Cilindro Dirección", 46, 96)
    ST = ComponentCode("suspension_trasera", "Suspensión Trasera", 65, 125)
    CMS = ComponentCode("suspension_delantera", "CMSD", 64, 124)
    MT = ComponentCode("motor_traccion", "Motor Tracción", 74, 134)
    CL = ComponentCode("cilindro_levante", "Cilindro Levante", 75, 135)
    MP = ComponentCode("modulo_potencia", "Módulo Potencia", 110, 170)

    @classmethod
    def get_all_codes(cls) -> List[str]:
        return [component.value.code for component in cls]


def find_available_pool_slot(component_df: pd.DataFrame, changeout: pd.Series) -> pd.DataFrame:

    df = component_df.sort_values(["pool_slot", "changeout_date"]).drop_duplicates(subset=["pool_slot"], keep="last")
    df = (
        df[
            (df["changeout_date"] < changeout["changeout_date"])
            & (df["arrival_date"].notnull())
            & (df["arrival_date"] <= changeout["changeout_date"] - pd.Timedelta(days=0))
        ]
        .sort_values("changeout_date")
        .drop_duplicates(subset=["pool_slot", "component"], keep="last")
    )

    return df


def get_ovh_days(row: pd.Series) -> int:
    component = next(c for c in ComponentType if c.value.code == row["component"])
    if component == ComponentType.MP:
        if row["subcomponent"] in ["alternador_principal", "radiador"]:
            return 64 if row["pool_changeout_type"] == "P" else 114
    return component.value.planned_ovh_days if row["pool_changeout_type"] == "P" else component.value.unplanned_ovh_days


def add_arrival_date_proj(df: pd.DataFrame) -> pd.DataFrame:

    required_columns = {"component", "changeout_date", "pool_changeout_type", "subcomponent"}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"Missing required columns: {required_columns}")

    df = df.copy()
    df["ovh_days"] = df.apply(get_ovh_days, axis=1)
    df["arrival_date_proj"] = df["changeout_date"] + pd.to_timedelta(df["ovh_days"], unit="D")
    df["arrival_week_proj"] = df["arrival_date_proj"].dt.strftime("%G-W%V")

    return df.drop(columns=["ovh_days"])


def priority_sort(df: pd.DataFrame) -> pd.DataFrame:

    required_columns = {"component", "subcomponent", "equipo", "changeout_date", "position"}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"Missing required columns: {required_columns}")

    priority_map = {
        ComponentType.MT.value.code: {"MOTOR TRACCIÓN": 1},
        ComponentType.CMS.value.code: {"Suspension Delantera": 1},
        ComponentType.MP.value.code: {"MOTOR": 1, "Alternador Principal": 2, "Radiador": 3},
    }
    default_priority = 999

    def get_priority(row: pd.Series) -> int:
        component_priorities = priority_map.get(row["component"], {})
        return next(
            (
                priority
                for subcomponent, priority in component_priorities.items()
                if subcomponent in row["subcomponent"]
            ),
            default_priority,
        )

    df = df.copy()
    df["subcomponent_priority"] = df.apply(get_priority, axis=1)
    return (
        df.sort_values(["equipo", "changeout_date", "component", "position", "subcomponent_priority"])
        .reset_index(drop=True)
        .drop(columns=["subcomponent_priority"])
    )


class ComponentAllocation:
    def __init__(
        self, cc_df: pd.DataFrame, pool_proj_df: pd.DataFrame, arrivals_df: pd.DataFrame, blocked_lanes: pd.DataFrame
    ):
        self.blocked_lanes = blocked_lanes
        self.cc_df = self._preprocess_cc_df(cc_df)
        self.pool_proj_df = self._preprocess_pool_proj_df(pool_proj_df)
        self.arrivals_df = self._preprocess_arrivals_df(arrivals_df)
        self.missing_cc_df = self.get_missing_changeouts()
        self.pool_slots_df = self.get_base_pool_slots()
        self.allocated_df = pd.DataFrame
        self.allocations_log = {}

    def _preprocess_cc_df(self, cc_df: pd.DataFrame) -> pd.DataFrame:
        cc_df = priority_sort(cc_df)
        return cc_df.drop_duplicates(subset=["equipo", "component", "position", "changeout_date"])[
            cc_df["component"].isin(ComponentType.get_all_codes())
        ].reset_index(drop=True)

    def _preprocess_pool_proj_df(self, pool_proj_df: pd.DataFrame) -> pd.DataFrame:
        return pool_proj_df[pool_proj_df["component"].isin(ComponentType.get_all_codes())].reset_index(drop=True)

    def _preprocess_arrivals_df(self, arrivals_df: pd.DataFrame) -> pd.DataFrame:
        df = arrivals_df.copy()

        # merge_columns = ["component", "arrival_week"]
        # df = pd.merge(
        #     df.drop(columns=["arrival_date"]),
        #     self.pool_proj_df[merge_columns],
        #     on=merge_columns,
        #     how="left",
        #     indicator=True,
        # )
        # df = df.loc[df["_merge"] == "left_only"].drop(columns="_merge")

        df = df[["component", "arrival_week", "arrival_date", "arrival_type"]]
        # Para la proyección por plan, se usa modifica sólo acorde las llegadas reales.
        df = df.loc[df["arrival_type"] == "REAL"].drop(columns=["arrival_type"]).reset_index(drop=True)

        # df["arrival_date"] = df["arrival_week"].apply(lambda x: datetime.strptime(f"{x}-1", DATE_FORMAT))
        df["pool_slot"] = None
        return df

    def find_most_time_unchanged_slot(self, df: pd.DataFrame, changeout_date: pd.Timestamp) -> pd.Series:

        if df.empty:
            raise ValueError("Input DataFrame is empty")

        df = df.assign(days_unchanged=(changeout_date - df["arrival_date"]).dt.days)
        return df.loc[df["days_unchanged"].idxmax()]

    def add_arrival_date(self, component_df: pd.DataFrame, component_arrival_df: pd.DataFrame) -> pd.DataFrame:
        # Unir las fechas de llegada a los cambios del pool sin llegadas confirmadas.
        # unconfirmed_df = (
        #     component_df.sort_values(["pool_slot", "changeout_date"])
        #     .drop_duplicates(subset=["pool_slot"], keep="last")
        #     .sort_values("arrival_date_proj")
        # )
        # Buscar todas las asignaciones de pool con llegadas no confirmadas
        unconfirmed_df = (
            component_df.loc[component_df["arrival_status"] == "unconfirmed"]
            .sort_values("arrival_date_proj")
            .reset_index(drop=True)[["component", "arrival_date_proj", "changeout_date", "pool_slot"]]
        )
        # self.unconfirmed_df = unconfirmed_df

        # La unión se efectua encontrando la fecha más cercana
        component_arrival_df = pd.merge_asof(
            component_arrival_df[["component", "arrival_date"]],
            unconfirmed_df,
            by="component",
            left_on="arrival_date",
            right_on="arrival_date_proj",
            direction="nearest",
        )

        # self.component_arrival_df = component_arrival_df

        for _, row in component_arrival_df.iterrows():
            mask = (component_df["changeout_date"] == row["changeout_date"]) & (
                component_df["component"] == row["component"]
            )
            component_df.loc[mask, "arrival_date"] = row["arrival_date"]
            component_df.loc[mask, "arrival_status"] = "confirmed"
            self.arrivals_df.loc[
                (self.arrivals_df["arrival_date"] == row["arrival_date"])
                & (self.arrivals_df["component"] == row["component"]),
                "pool_slot",
            ] = row["pool_slot"]

        return component_df

    def allocate_components(self) -> pd.DataFrame:

        frames = []
        for component in self.missing_cc_df["component"].unique():
            # for component in ["motor_traccion"]:
            component_df = self.pool_slots_df.loc[self.pool_slots_df["component"] == component]

            self.allocations_log[component] = ""
            should_break = False
            # Encontrar semanas por donde va a ir iterando el algoritmo dado un componente
            weeks = (
                pd.concat(
                    [
                        self.missing_cc_df.loc[self.missing_cc_df["component"] == component]["changeout_week"],
                        self.arrivals_df.loc[self.arrivals_df["component"] == component]["arrival_week"],
                    ]
                )
                .drop_duplicates()
                .reset_index(drop=True)
                .to_list()
            )
            weeks = sorted(weeks)
            for week in weeks:
                self.allocations_log[component] += f"\nSemana {week}:"
                week_arrivals_df = self.arrivals_df.loc[
                    (self.arrivals_df["arrival_week"] == week) & (self.arrivals_df["component"] == component)
                ].reset_index(drop=True)
                week_cc_df = self.missing_cc_df.loc[
                    (self.missing_cc_df["changeout_week"] == week) & (self.missing_cc_df["component"] == component)
                ].reset_index(drop=True)
                if week_arrivals_df.shape[0] == 0:
                    self.allocations_log[component] += f"\nSin llegadas de componente"
                else:

                    self.allocations_log[
                        component
                    ] += f"\nSe agrega llegada componente {component} con fecha: {week_arrivals_df['arrival_date'].dt.strftime('%Y-%m-%d').to_list()}"

                    component_df = component_df.pipe(self.add_arrival_date, week_arrivals_df)

                if week_cc_df.shape[0] == 0:
                    self.allocations_log[
                        component
                    ] += f"\nNo existen cambios de componente para la semana {week}, componente {component}"
                else:

                    # Proceder con agregar el componente
                    for _, changeout in week_cc_df.iterrows():
                        self.allocations_log[component] += (
                            f"\nSe agrega cambio de componente con fecha: {changeout['changeout_date'].strftime('%Y-%m-%d')}, "
                            f"equipo: {changeout['equipo']}, serie: {changeout['component_serial']}"
                        )
                        new_row = changeout.copy()
                        # 0. Omite verificar si tiene disponibilidad en el pool en caso de que sea linea blockeada esperando
                        if not changeout["pool_changeout_type"] == "E":
                            # 1. Verifica si el componente sujeto a cambio tiene disponibilidad en el pool
                            available_slots_df = find_available_pool_slot(component_df, changeout)
                            if not available_slots_df.empty:

                                new_row["arrival_status"] = "unconfirmed"
                                available_slot = self.find_most_time_unchanged_slot(
                                    available_slots_df, changeout["changeout_date"]
                                )
                                new_row["pool_slot"] = available_slot["pool_slot"]

                            else:
                                should_break = True
                                self.allocations_log[component] += f"\nNo se pudo agregar componente."
                                break
                        new_row = pd.DataFrame([new_row])
                        new_row = add_arrival_date_proj(new_row)
                        component_df = pd.concat([component_df, new_row], ignore_index=True)
                        component_df = component_df.sort_values("changeout_date")

                        # Actualizar en missing components para debugging
                        self.missing_cc_df.loc[
                            (self.missing_cc_df["component"] == changeout["component"])
                            & (self.missing_cc_df["component_serial"] == changeout["component_serial"])
                            & (self.missing_cc_df["changeout_date"] == changeout["changeout_date"]),
                            "allocated",
                        ] = True
                    if should_break:
                        break
                self.allocations_log[component] += "\n"
            frames.append(component_df)

        df = pd.concat(frames)
        return df

    def get_base_pool_slots(self) -> pd.DataFrame:
        # proyección en base a un archivo base para darle forma al gráfico de timeline
        merge_columns = ["equipo", "component", "component_serial", "changeout_week"]

        df = pd.merge(self.pool_proj_df, self.cc_df, on=merge_columns, how="left", suffixes=("_proj", ""))

        df["pool_changeout_type"] = df["pool_changeout_type"].fillna(df["pool_changeout_type_proj"])
        df = df.drop(columns="pool_changeout_type_proj")

        df["changeout_date"] = df["changeout_date"].fillna(df["changeout_date_proj"])

        df = df.drop(columns="changeout_date_proj").reset_index(drop=True)

        df = add_arrival_date_proj(df)

        # Los que no fecha llegada asignada en la planilla base son proyecciones, no llegadas reales
        df = df.assign(arrival_status=np.where(df["arrival_date"].isnull(), "unconfirmed", "historical"))

        return df

    def get_missing_changeouts(self) -> pd.DataFrame:

        merge_columns = ["equipo", "component", "component_serial", "changeout_week"]
        df = self.cc_df[self.cc_df["changeout_date"] >= CHANGEOUT_START_DATE][
            [
                "equipo",
                "component",
                "subcomponent",
                "changeout_date",
                "changeout_week",
                "pool_changeout_type",
                "component_hours",
                "component_serial",
            ]
        ]
        df = df[df["pool_changeout_type"] != "N"]

        df = pd.merge(df, self.pool_proj_df[merge_columns], on=merge_columns, how="left", indicator=True)
        df = df[df["_merge"] == "left_only"].drop(columns="_merge")
        df["pool_changeout_type"] = df["pool_changeout_type"].fillna("P")

        df = pd.merge(
            df,
            self.blocked_lanes,
            how="outer",
            on=["component", "equipo", "changeout_date"],
            validate="m:1",
        )
        df = df.assign(
            pool_changeout_type=np.where(df["pool_slot"].notnull(), "E", df["pool_changeout_type"]),
            changeout_week=df["changeout_date"].dt.strftime("%G-W%V"),
            # arrival_week=df["arrival_date"].dt.strftime("%G-W%V"),
        )
        assert df.loc[df["pool_changeout_type"] == "E"].shape[0] == self.blocked_lanes.shape[0]
        df = df.sort_values(["component", "changeout_date"]).reset_index(drop=True)
        return df

    def generate_pool_projection(self) -> pd.DataFrame:

        df = self.allocate_components()

        df[["changeout_date", "arrival_date"]] = df[["changeout_date", "arrival_date"]].apply(pd.to_datetime)

        df["componente"] = df["component"].map({c.value.code: c.value.name for c in ComponentType})
        df = df.reset_index(drop=True)
        df = df.assign(arrival_date=np.where(df["arrival_date"].isnull(), df["arrival_date_proj"], df["arrival_date"]))
        df["ovh_days"] = df.apply(get_ovh_days, axis=1)
        self.allocated_df = df
