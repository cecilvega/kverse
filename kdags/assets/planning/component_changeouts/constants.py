COMPATIBILITY_MAPPING = {
    # (COMPONENTE, SUB COMPONENTE): (component_name, subcomponent_name)
    ("alternador_principal", "alternador_principal"): (
        "modulo_potencia",
        "alternador_principal",
    ),
    ("blower_parrilla", "blower_parrilla"): ("blower_parrilla", "blower_parrilla"),
    ("blower", "blower"): ("blower_parrilla", "blower_parrilla"),
    ("cilindro_direccion", "cilindro_direccion"): ("cilindro_direccion", "cilindro_direccion"),
    ("cilindro_levante", "cilindro_levante"): ("cilindro_levante", "cilindro_levante"),
    ("suspension_trasera", "suspension_trasera"): ("suspension_trasera", "suspension_trasera"),
    ("cms", "suspension"): ("conjunto_maza_suspension", "suspension_delantera"),
    ("cms", "suspension_delantera"): (
        "conjunto_maza_suspension",
        "suspension_delantera",
    ),
    ("cms", "masa"): ("conjunto_maza_suspension", "maza"),
    ("cms", "freno_servicio"): ("conjunto_maza_suspension", "freno_servicio_delantero"),
    ("cms", "freno_servicio_delanteros"): (
        "conjunto_maza_suspension",
        "freno_servicio_delantero",
    ),
    ("mdp", "radiador"): ("modulo_potencia", "radiador"),
    ("mdp", "subframe"): ("modulo_potencia", "subframe"),
    ("mdp", "motor_"): ("modulo_potencia", "motor"),
    ("modulo_potencia", "motor"): ("modulo_potencia", "motor"),
    ("mdp", "alternador_principal"): ("modulo_potencia", "alternador_principal"),
    ("motor_traccion", "motor_traccion"): ("motor_traccion", "transmision"),
    ("motor_traccion", "freno_estacionamiento"): (
        "motor_traccion",
        "freno_estacionamiento",
    ),
    ("motor_traccion", "freno_servicio"): ("motor_traccion", "freno_servicio_trasero"),
    ("motor_traccion", "motor_electrico"): ("motor_traccion", "motor_electrico"),
}

COLUMN_MAPPING = {
    "EQUIPO": "equipment_name",
    "MODELO": "equipment_model",
    "COMPONENTE": "component_name",
    "SUB COMPONENTE": "subcomponent_name",
    "POSICION": "position_name",
    "FECHA DE CAMBIO": "changeout_date",
    "OS  181": "customer_work_order",
    "HORA EQ": "equipment_hours",
    "HORA CC": "component_hours",
    "TBO": "tbo",
    "USO": "component_usage",
    "N/S RETIRADO": "component_serial",  # removed_component_serial
    "Equipo SAP": "sap_equipment_name",
    "N/S INSTALADO": "installed_component_serial",
    "DESCRIPCIÓN DE FALLA": "failure_description",
}
