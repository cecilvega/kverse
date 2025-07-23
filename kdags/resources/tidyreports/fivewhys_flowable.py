from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    Paragraph,
    Flowable,
)


class FiveWhysFlowable(Flowable):
    def __init__(self, data, width):
        Flowable.__init__(self)
        self.data = data
        self.width = width
        # Calculate dynamic height based on content
        self._calculate_required_height()

        # MODIFICADO: Paleta de colores actualizada para coincidir con el gráfico de ejemplo y los colores corporativos.
        self.RAMA_COLORS = {
            "Problema": colors.HexColor("#d1d4d3"),  # Komatsu Light Gray
            "Causa Física": colors.HexColor("#d1d4d3"),  # Komatsu Light Gray
            "Mantenimiento": colors.HexColor("#c4d79b"),  # Verde claro del diagrama
            "Reparación": colors.HexColor("#a5abaf"),  # Komatsu Gray
            "Operación": colors.HexColor("#fcd5b4"),  # Naranja/Salmón del diagrama
            # Colores para las conclusiones
            "Causa Raíz": colors.HexColor("#fcd5b4"),  # Naranja/Salmón
            "Hipótesis Descartada_Op": colors.HexColor("#0070c0"),  # Azul del diagrama
            "Hipótesis Descartada_Rep": colors.HexColor("#002060"),  # Azul oscuro del diagrama
        }
        self.FONDO_NIVEL_COLOR = colors.HexColor("#f2f2f2")  # Un gris muy claro para el fondo de cada "Porqué"

    def _calculate_required_height(self):
        """Calculate the minimum height required based on content"""
        # Count nodes per level to determine vertical space needed
        nodes_by_level = {i: [] for i in range(1, 6)}
        for nodo in self.data["nodos"]:
            nodes_by_level[nodo["nivel"]].append(nodo)

        # Find the level with most nodes
        max_nodes_in_level = max(len(nodes) for nodes in nodes_by_level.values()) if nodes_by_level else 1

        # Calculate required height
        box_height = 1.3 * inch
        vertical_spacing = 1.5 * inch
        title_space = 0.5 * inch
        margin_space = 1 * inch

        # Height = title space + (max nodes * box height) + ((max nodes - 1) * spacing) + margins
        self.height = (
            title_space
            + (max_nodes_in_level * box_height)
            + ((max_nodes_in_level - 1) * vertical_spacing)
            + margin_space
        )

        # Ensure minimum height
        self.height = max(self.height, 4 * inch)

    def _calculate_layout(self):
        """Calcula las coordenadas y dimensiones de cada nodo."""
        layout = {}

        # Parámetros de diseño
        self.box_width = self.width / 6
        self.box_height = 1.3 * inch
        self.level_spacing = self.width / 5.2
        self.vertical_spacing = 1.5 * inch

        # Calcular posiciones X para cada nivel
        self.level_x_positions = {i: (i - 1) * self.level_spacing for i in range(1, 6)}

        # Calcular posiciones Y para cada nodo
        nodes_by_level = {i: [] for i in range(1, 6)}
        for nodo in self.data["nodos"]:
            nodes_by_level[nodo["nivel"]].append(nodo)

        for level, nodes in nodes_by_level.items():
            num_nodes = len(nodes)
            # Centrar verticalmente el grupo de nodos
            start_y = (self.height / 2) + ((num_nodes - 1) / 2.0) * self.vertical_spacing

            for i, nodo in enumerate(nodes):
                x = self.level_x_positions[level] + (self.width * 0.04)
                y = start_y - i * self.vertical_spacing
                layout[nodo["id"]] = {"x": x, "y": y, "w": self.box_width, "h": self.box_height}

        return layout

    def draw(self):
        """Dibuja el gráfico completo en el canvas."""
        layout = self._calculate_layout()

        # --- NUEVO: Dibuja los fondos grises y títulos para cada nivel ---
        self.canv.setFont("Helvetica-Bold", 14)
        for i in range(1, 6):
            # Posición y tamaño del fondo
            bg_x = self.level_x_positions[i]
            bg_y = 0.5 * inch
            bg_width = self.level_spacing * 0.9
            bg_height = self.height - 1 * inch

            # Dibuja el rectángulo de fondo
            self.canv.setFillColor(self.FONDO_NIVEL_COLOR)
            self.canv.rect(bg_x, bg_y, bg_width, bg_height, fill=1, stroke=0)

            # Dibuja el título del nivel (ej. "1° Porque")
            self.canv.setFillColor(colors.black)
            title = f"{i}° Porque"
            if i == 5:
                title = "5° Porque / Causa Raíz"
            self.canv.drawCentredString(bg_x + bg_width / 2, bg_y + bg_height + 10, title)

        # Estilo de texto para los cuadros
        style = ParagraphStyle(name="BoxText", fontSize=8, leading=10, alignment=TA_CENTER)

        # 1. Dibujar los cuadros de contenido y el texto
        for nodo in self.data["nodos"]:
            pos = layout[nodo["id"]]

            # Determinar color del cuadro usando la paleta actualizada
            rama_key = nodo["rama"]
            if nodo["conclusion"]:
                if nodo["conclusion"] == "Causa Raíz":
                    rama_key = "Causa Raíz"
                elif nodo["conclusion"] == "Hipótesis Descartada":
                    rama_key = f"Hipótesis Descartada_{nodo['rama'][:3]}"

            color = self.RAMA_COLORS.get(rama_key, colors.lightgrey)

            # Dibujar el rectángulo
            self.canv.setFillColor(color)
            self.canv.setStrokeColor(colors.black)
            self.canv.setLineWidth(1)
            self.canv.rect(pos["x"], pos["y"] - pos["h"] / 2, pos["w"], pos["h"], fill=1, stroke=1)

            # Dibujar el texto
            p = Paragraph(nodo["texto"], style)
            p_w, p_h = p.wrapOn(self.canv, pos["w"] - 10, pos["h"] - 10)
            p.drawOn(self.canv, pos["x"] + 5, pos["y"] + (pos["h"] - p_h) / 2)  # Ajuste para centrar

            # Dibujar marcas de conclusión (X y ✓)
            if nodo["conclusion"]:
                self.canv.setFont("Helvetica-Bold", 22)  # Letra más grande y notoria
                if nodo["conclusion"] == "Causa Raíz":
                    self.canv.setFillColor(colors.red)
                    self.canv.drawString(pos["x"] + pos["w"] - 20, pos["y"] + pos["h"] / 2 - 20, "X")
                elif nodo["conclusion"] == "Hipótesis Descartada":
                    self.canv.setFillColor(colors.green)
                    self.canv.drawString(pos["x"] + pos["w"] - 20, pos["y"] + pos["h"] / 2 - 20, "✓")

        # 2. Dibujar las conexiones (flechas)
        self.canv.setStrokeColor(colors.black)
        self.canv.setLineWidth(1.5)
        for conn in self.data["conexiones"]:
            origen = layout[conn["origen"]]
            destino = layout[conn["destino"]]
            self.canv.line(origen["x"] + origen["w"], origen["y"], destino["x"], destino["y"])

    def wrap(self, availWidth, availHeight):
        """Handle pagination by checking if flowable fits on current page"""
        # If there's not enough space on current page, request a page break
        if availHeight < self.height:
            # Return dimensions that will trigger a page break
            return (self.width, availHeight + 1)
        return (self.width, self.height)
