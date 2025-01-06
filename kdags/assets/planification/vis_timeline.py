import re


color_map = {
    "REPROGRAMADO": "#942d00",  # Red
    "REAL": "#007569",  # Green
    # "E": "#a5abaf",  # Gray
    # "R": "#ffc82f",  # Orange
    "PROYECTADO": "#140a9a",  # Blue
}


def validate_input(df):
    """Validate the input DataFrame."""
    required_columns = [
        "pool_slot",
        "changeout_date",
        "arrival_date",
        "pool_changeout_type",
        "equipo",
        "component_serial",
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")


def prepare_data(df):
    """Prepare the data for plotting."""
    df = df.sort_values(["pool_slot", "changeout_date"])

    # df = df.drop(columns=["subcomponent_priority"])
    return df


def clean_text_column(df, column_name):
    # Function to clean individual text entries
    def clean_text(text):
        # Remove 'REAL' and 'reprogramado REAL'
        text = re.sub(r"REAL|REPROGRAMADO|X1|X2", "", text, flags=re.IGNORECASE)

        # Remove extra whitespace, including newlines
        text = " ".join(text.split())
        text = re.sub(r"(\d{2}-\d{2}-\d{4})", r"\1<br>", text)
        return text.strip()

    # Apply the cleaning function to the specified column
    df[column_name] = df[column_name].astype(str).apply(clean_text)

    return df


def plot_component_arrival_timeline(df):
    import streamlit as st
    from streamlit_timeline import st_timeline

    for label, color in color_map.items():
        st.markdown(
            f'<div style="display: flex; align-items: center; margin-bottom: 5px;">'
            f'<div style="width: 20px; height: 20px; background-color: {color}; '
            f'margin-right: 10px; border: 1px solid #000;"></div>'
            f"<span>{label}</span>"
            "</div>",
            unsafe_allow_html=True,
        )

    df = df.assign(
        component=df["component"].map(
            lambda x: {
                "blower_parrilla": "Blower Parrilla",
                "cilindro_direccion": "Cilindro de Dirección",
                "suspension_trasera": "Suspensión Trasera",
                "suspension_delantera": "Suspensión Delantera",
                "motor_traccion": "Motor de Tracción",
                "cilindro_levante": "Cilindro de Levante",
                "modulo_potencia": "Módulo de Potencia",
            }[x]
        )
    )
    df = clean_text_column(df, "value")
    # df["label"] = df["arrival_date"].dt.strftime("%Y-%m-%d") + " (" + df["label"] + ")"

    # TODO: RETOMAR
    # proj_df = df.loc[
    #     (df["arrival_type"] == "PROYECTADO") & (df["arrival_date"] >= (datetime.now() - timedelta(days=7)))
    # ].sort_values("arrival_date")
    # columns = st.columns(4)
    # for i, (_, row) in enumerate(proj_df.iterrows()):
    #     with columns[i % 4]:
    #         days_until_arrival = (row["arrival_date"].date() - datetime.now().date()).days
    #         # if row["pool_changeout_type"] == "E":
    #         #     days_until_arrival = "?"
    #         #     row["arrival_week"] = "?"
    #         # repair_days = row["ohv_normal"] if row["pool_type"] == "P" else row["ohv_unplanned"]
    #         # repair_color = "normal" if row["pool_type"] == "P" else "inverse"
    #
    #         st.metric(
    #             label=f"{row['component']}",
    #             value=f"{days_until_arrival} días restantes",
    #             # delta=f"{repair_days} days repair",
    #             # delta_color=repair_color,
    #         )
    #         st.write(f"Semana estimada de llegada: {row['arrival_week']}")
    #         # st.write(f"Fecha cambio componente: {row['changeout_date'].date()}")
    #         # map_dict = {"I": "Imprevisto", "P": "Planificado", "E": "Esperando"}
    #         # st.write(f"Tipo de cambio: {map_dict[row['pool_changeout_type']]}")
    #         st.write("---")

    # Create items for the timeline
    items = []
    for _, row in df.iterrows():
        item = {
            "id": str(len(items) + 1),
            "content": f"{row['value']}",
            "start": row["arrival_date"].strftime("%Y-%m-%d"),
            "group": row["component"],
            "style": f"background-color: {color_map.get(row['arrival_type'], '#000000')};color: white;",
            "subgroup": row["N°"],
        }
        items.append(item)

    # Create groups for the timeline
    groups = [{"id": str(code), "content": code} for code in sorted(df["component"].unique())]

    # Set up options for the timeline
    options = {
        "selectable": True,
        "multiselect": False,
        "zoomable": True,
        "stack": False,
        "subgroupStack": False,
        "height": "650px",  # Increased height to accommodate more component codes
        "margin": {"item": 10},
        # "groupHeightMode": "fixed",
        "orientation": {"axis": "top", "item": "top"},
        # "stack": False,
        "format": {
            "minorLabels": {"week": "w"},
            "majorLabels": {"week": "MMMM YYYY"},
        },
    }

    # Create and return the timeline
    timeline = st_timeline(items=items, groups=groups, options=options)
    return timeline
