from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Image,
    PageBreak,
    Table,
    TableStyle,
    Flowable,
    # Flowable,
)
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from datetime import datetime
import os
from kdags.resources.tidyreports.fivewhys_flowable import FiveWhysFlowable
from kdags.resources.tidyreports.timeline_flowable import TimelineFlowable
from pathlib import Path
from io import BytesIO
from typing import Union, List, Dict, Optional

# Komatsu Official Brand Colors
KOMATSU_BLUE = colors.HexColor("#140a9a")  # Azul Gloria
KOMATSU_YELLOW = colors.HexColor("#ffc82f")  # Natural Yellow
KOMATSU_BLACK = colors.HexColor("#1b232a")  # Black Graphite
KOMATSU_GRAY = colors.HexColor("#a5abaf")  # Gray or #d1d4d3

# Constants
BASE_PATH = Path(__file__).parent
KOMATSU_LOGO_PATH = (BASE_PATH / "komatsu_logo.png").__str__()  # Update with your actual path
BHP_LOGO_PATH = (BASE_PATH / "bhp_logo.png").__str__()  # Update with your actual path
PAGE_SIZE = letter


# Pega esta clase completa en tu archivo .py


class TidyReport:
    def __init__(self, filename="output.pdf", report_title="REPORTE", output_to_memory=True):
        """Initialize PDF with Komatsu brand settings"""
        self.filename = filename
        self.generation_date = datetime.now()
        self.report_title = report_title

        self.output_to_memory = output_to_memory

        # Create buffer if outputting to memory
        if self.output_to_memory:
            self.buffer = BytesIO()
            self.doc = SimpleDocTemplate(
                self.buffer,
                pagesize=PAGE_SIZE,
                topMargin=0.8 * inch,
                bottomMargin=0.6 * inch,
                leftMargin=0.5 * inch,
                rightMargin=0.5 * inch,
            )
        else:
            self.doc = SimpleDocTemplate(
                filename,
                pagesize=PAGE_SIZE,
                topMargin=0.8 * inch,
                bottomMargin=0.6 * inch,
                leftMargin=0.5 * inch,
                rightMargin=0.5 * inch,
            )

        self.story = []
        self.styles = getSampleStyleSheet()
        self._setup_styles()

    # Agrégalo dentro de tu clase KomatsuPDF, junto a los otros métodos "add_..."

    def add_five_whys_chart(self, analysis_data):
        """
        Agrega un gráfico de 5 Porqués al reporte.

        Args:
            analysis_data: Un diccionario con la estructura de nodos y conexiones.
        """
        five_whys_chart = FiveWhysFlowable(analysis_data, self.doc.width)
        self.story.append(five_whys_chart)
        self.story.append(Spacer(1, 0.3 * inch))

    def _setup_styles(self):
        """Setup Komatsu brand styles"""
        # Komatsu Title Style (Blue background, white text) - like the blue box
        self.styles.add(
            ParagraphStyle(
                name="KomatsuTitle",
                parent=self.styles["Heading1"],
                fontName="Helvetica-Bold",
                fontSize=16,  # Reduced from 20
                textColor=colors.white,
                backColor=KOMATSU_BLUE,
                spaceAfter=2,  # Reduced from 6
                spaceBefore=2,  # Reduced from 6
                leftIndent=0,
                rightIndent=0,
            )
        )

        # Komatsu Heading Style (Blue text)
        self.styles.add(
            ParagraphStyle(
                name="KomatsuHeading",
                parent=self.styles["Heading2"],
                fontName="Helvetica-Bold",
                fontSize=14,
                textColor=KOMATSU_BLUE,
                spaceAfter=6,  # Reduced from 8
                spaceBefore=8,  # Reduced from 12 to bring closer to sections
            )
        )

        # Komatsu Body Style (Black text)
        self.styles.add(
            ParagraphStyle(
                name="KomatsuBody",
                parent=self.styles["BodyText"],
                fontSize=11,  # Reduced from 11
                fontName="Helvetica",
                textColor=KOMATSU_BLACK,
                leading=12,  # Reduced from 16
                spaceAfter=4,  # Reduced from 12
            )
        )

        # Komatsu Accent Style (Blue text)
        self.styles.add(
            ParagraphStyle(
                name="KomatsuAccent",
                parent=self.styles["BodyText"],
                fontSize=12,
                textColor=KOMATSU_BLUE,
                leading=14,
                spaceAfter=12,
            )
        )

        # Photo Title Style
        self.styles.add(
            ParagraphStyle(
                name="PhotoTitle",
                parent=self.styles["BodyText"],
                fontSize=10,  # Smaller font size
                textColor=KOMATSU_BLACK,  # Changed to black
                alignment=TA_CENTER,
                spaceAfter=4,  # Reduced spacing
                spaceBefore=4,  # Reduced spacing
            )
        )

    def _header_footer(self, canvas, doc):
        """Add header and footer to each page"""
        canvas.saveState()

        # HEADER with blue background and yellow accent - positioned at absolute top
        header_height = 40

        # Blue background bar - full page width at absolute top
        canvas.setFillColor(KOMATSU_BLUE)
        canvas.rect(0, letter[1] - header_height, letter[0], header_height, fill=1, stroke=0)

        # Yellow accent bar - full page width
        canvas.setFillColor(KOMATSU_YELLOW)
        canvas.rect(0, letter[1] - header_height - 5, letter[0], 5, fill=1, stroke=0)

        # White text on blue background
        canvas.setFillColor(colors.white)
        canvas.setFont("Helvetica-Bold", 18)
        canvas.drawString(30, letter[1] - 28, self.report_title)

        # Page number on the right
        canvas.setFont("Helvetica", 12)
        page_text = f"Página {doc.page}"
        canvas.drawRightString(letter[0] - 30, letter[1] - 28, page_text)

        # FOOTER
        footer_y = 0.5 * inch

        # Draw blue line across the page
        canvas.setStrokeColor(KOMATSU_BLUE)
        canvas.setLineWidth(1)
        canvas.line(doc.leftMargin, footer_y + 20, doc.width + doc.leftMargin, footer_y + 20)

        # Add Komatsu logo on the left
        if os.path.exists(KOMATSU_LOGO_PATH):
            logo_height = 0.4 * inch
            logo_width = 1.8 * inch
            canvas.drawImage(
                KOMATSU_LOGO_PATH,
                doc.leftMargin,
                footer_y - 10,
                width=logo_width,
                height=logo_height,
                preserveAspectRatio=True,
                mask="auto",
            )

        # Add centered generation date
        canvas.setFont("Helvetica", 10)
        canvas.setFillColor(KOMATSU_BLUE)
        center_x = doc.width / 2 + doc.leftMargin
        date_text = f"Generated: {self.generation_date.strftime('%B %d, %Y')}"
        canvas.drawCentredString(center_x, footer_y, date_text)

        # Add BHP logo on the right
        if os.path.exists(BHP_LOGO_PATH):
            partner_height = 0.5 * inch
            partner_width = 1.2 * inch
            partner_x = doc.width + doc.leftMargin - partner_width
            canvas.drawImage(
                BHP_LOGO_PATH,
                partner_x,
                footer_y - 15,
                width=partner_width,
                height=partner_height,
                preserveAspectRatio=True,
                mask="auto",
            )

        canvas.restoreState()

    def add_section(self, title):
        """Add a section with blue background that extends beyond margins"""
        # Create a table that extends beyond the margins
        # The blue box will be wider than the text area
        extended_width = self.doc.width + 0.3 * inch  # Extend 0.15" on each side

        # Create paragraph with proper left margin to align with other content
        title_para = Paragraph(title, self.styles["KomatsuTitle"])

        # Create table with extended width but centered
        section_table = Table([[title_para]], colWidths=[extended_width])
        section_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), KOMATSU_BLUE),
                    ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 0.4 * inch),  # Reduced padding
                    ("RIGHTPADDING", (0, 0), (-1, -1), 8),  # Reduced padding
                    ("TOPPADDING", (0, 0), (-1, -1), 4),  # Reduced padding
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),  # Reduced padding
                ]
            )
        )

        # Center the extended table
        wrapper_table = Table([[section_table]], colWidths=[self.doc.width])
        wrapper_table.setStyle(
            TableStyle(
                [
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ]
            )
        )

        self.story.append(wrapper_table)
        self.story.append(Spacer(1, 0.1 * inch))  # Reduced from 0.2

    def add_heading(self, text):
        """Add a heading in Komatsu blue"""
        self.story.append(Paragraph(text, self.styles["KomatsuHeading"]))
        self.story.append(Spacer(1, 0.1 * inch))

    def add_paragraph(self, text):
        """Add a paragraph in black"""
        self.story.append(Paragraph(text, self.styles["KomatsuBody"]))

    def add_accent_text(self, text):
        """Add accented text in Komatsu blue"""
        self.story.append(Paragraph(text, self.styles["KomatsuAccent"]))

    def add_photos(
        self, images: List[Dict[str, Union[bytes, str]]], images_per_row: int = 2, scale_factor: float = 1.0
    ):
        """
        Add multiple photos with automatic batching and layout.

        Args:
            images: List of dictionaries, each containing:
                - 'content': bytes or file path of the image
                - 'title': Optional title for the image (defaults to "Image N" if not provided)
            images_per_row: Number of images per row (default 2, max 2 for optimal layout)
            scale_factor: Controls image size. 1.0 = default, 2.0 = 4 images/page, 3.0 = 6 images/page
                         Higher values create smaller images.

        Example:
            # Add 6 images that fit on one page (3 rows of 2)
            pdf.add_photos(images_list, scale_factor=3.0)

            # Add larger images (default size)
            pdf.add_photos(images_list, scale_factor=1.0)
        """
        if not images:
            return

        # Ensure images_per_row doesn't exceed 2 for optimal layout
        images_per_row = min(images_per_row, 2)

        # Process images in batches
        for i in range(0, len(images), images_per_row):
            batch = images[i : i + images_per_row]
            self._add_photo_batch(batch, scale_factor)

    def _add_photo_batch(self, batch: List[Dict[str, Union[bytes, str]]], scale_factor: float = 1.0):
        """Internal method to add a batch of photos (max 2)"""

        # Prepare image data and titles
        prepared_images = []
        for idx, img_dict in enumerate(batch):
            content = img_dict.get("content")
            title = img_dict.get("title")

            # Generate default title if not provided
            if not title:
                if isinstance(content, str) and os.path.exists(content):
                    # Extract from filename
                    filename = os.path.basename(content)
                    title = os.path.splitext(filename)[0].replace("_", " ").title()
                else:
                    # Generic title for bytes
                    title = f"Image {idx + 1}"

            prepared_images.append((content, title))

        # Calculate dimensions based on number of images
        available_width = self.doc.width

        if len(prepared_images) == 1:
            # Single image - use 70% of width, scaled down by factor
            img_width = (available_width * 0.7) / scale_factor
            self._create_single_image_table(prepared_images[0], img_width, scale_factor)
        else:
            # Multiple images - split width
            spacing_between = 0.5 * inch
            img_width = ((available_width - spacing_between) / 2) / scale_factor
            self._create_multiple_images_table(prepared_images, img_width, scale_factor)

    def _create_single_image_table(self, image_data: tuple, width: float, scale_factor: float = 1.0):
        """Create table for single centered image"""
        content, title = image_data

        # Create image from bytes or path
        if isinstance(content, bytes):
            img_buffer = BytesIO(content)
            img_buffer.seek(0)  # IMPORTANT: Reset position to beginning
            img = Image(img_buffer)
        else:
            img = Image(content)

        # Calculate dimensions with maximum height constraint
        aspect_ratio = img.imageHeight / img.imageWidth
        img.drawWidth = width - 10  # Account for border
        img.drawHeight = (width - 10) * aspect_ratio

        # Apply maximum height based on scale factor
        # For scale_factor 3, we want 3 rows to fit on a page
        usable_page_height = 9 * inch  # Approximate usable height on letter page
        max_height = (usable_page_height / scale_factor) - 0.8 * inch  # Account for spacing and title

        if img.drawHeight > max_height:
            img.drawHeight = max_height
            img.drawWidth = max_height / aspect_ratio

        # Create table with image and title
        data = [[img], [Paragraph(title, self.styles["PhotoTitle"])]]
        img_table = Table(data, colWidths=[img.drawWidth + 10])
        img_table.setStyle(
            TableStyle(
                [
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("BOX", (0, 0), (0, 0), 2, KOMATSU_BLUE),
                    ("TOPPADDING", (0, 0), (0, 0), 5),
                    ("BOTTOMPADDING", (0, 0), (0, 0), 5),
                    ("LEFTPADDING", (0, 0), (0, 0), 5),
                    ("RIGHTPADDING", (0, 0), (0, 0), 5),
                ]
            )
        )

        # Center the table
        wrapper_table = Table([[img_table]], colWidths=[self.doc.width])
        wrapper_table.setStyle(TableStyle([("ALIGN", (0, 0), (-1, -1), "CENTER")]))
        self.story.append(wrapper_table)
        self.story.append(Spacer(1, 0.3 * inch))

    def _create_multiple_images_table(self, images_data: List[tuple], img_width: float, scale_factor: float = 1.0):
        """Create table for multiple images side by side"""
        cells = []

        # Calculate maximum height for images
        usable_page_height = 9 * inch  # Approximate usable height on letter page
        max_height = (usable_page_height / scale_factor) - 0.8 * inch  # Account for spacing and title

        actual_widths = []  # Track actual widths after height constraints

        for content, title in images_data:
            # Create image from bytes or path
            if isinstance(content, bytes):
                img_buffer = BytesIO(content)
                img_buffer.seek(0)  # IMPORTANT: Reset position to beginning
                img = Image(img_buffer)
            else:
                img = Image(content)

            # Calculate dimensions
            aspect_ratio = img.imageHeight / img.imageWidth
            img.drawWidth = img_width - 10
            img.drawHeight = (img_width - 10) * aspect_ratio

            # Apply maximum height constraint
            if img.drawHeight > max_height:
                img.drawHeight = max_height
                img.drawWidth = max_height / aspect_ratio

            actual_widths.append(img.drawWidth + 10)

            # Create cell with image and title
            data = [[img], [Paragraph(title, self.styles["PhotoTitle"])]]
            cell_table = Table(data, colWidths=[img.drawWidth + 10])
            cell_table.setStyle(
                TableStyle(
                    [
                        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                        ("BOX", (0, 0), (0, 0), 2, KOMATSU_BLUE),
                        ("TOPPADDING", (0, 0), (0, 0), 5),
                        ("BOTTOMPADDING", (0, 0), (0, 0), 5),
                        ("LEFTPADDING", (0, 0), (0, 0), 5),
                        ("RIGHTPADDING", (0, 0), (0, 0), 5),
                    ]
                )
            )
            cells.append(cell_table)

        # Create main table with actual widths
        main_table = Table([cells], colWidths=actual_widths)
        main_table.setStyle(TableStyle([("ALIGN", (0, 0), (-1, -1), "CENTER"), ("VALIGN", (0, 0), (-1, -1), "TOP")]))
        self.story.append(main_table)
        self.story.append(Spacer(1, 0.3 * inch))

    def add_timeline(self, events):
        """
        Add a timeline with events

        Args:
            events: List of dicts with:
                - date: Date string (e.g., "2025-11-26")
                - description: Text for the yellow box

        Note: Maximum 4 events supported for optimal layout
        Positions alternate automatically starting from top
        """
        # Limit events to maximum 4
        events = events[:4]

        # Automatically assign alternating positions starting from top
        for i, event in enumerate(events):
            event["position"] = "top" if i % 2 == 0 else "bottom"

        # Create and add the timeline
        timeline = TimelineFlowable(events, self.doc.width, self.styles)
        self.story.append(timeline)
        self.story.append(Spacer(1, 0.3 * inch))

    def add_separator(self, color=KOMATSU_YELLOW):
        """Add a colored separator line"""
        separator = Table([[""]], colWidths=[self.doc.width], rowHeights=[3])
        separator.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), color),
                    ("LINEABOVE", (0, 0), (-1, 0), 0, colors.white),
                    ("LINEBELOW", (0, 0), (-1, 0), 0, colors.white),
                ]
            )
        )
        self.story.append(Spacer(1, 0.1 * inch))
        self.story.append(separator)
        self.story.append(Spacer(1, 0.1 * inch))

    def add_space(self, height=0.5):
        """Add vertical space (in inches)"""
        self.story.append(Spacer(1, height * inch))

    def add_table(self, table_data):
        """
        Add table(s) with improved interface and cleaner styling
        """

        def create_single_table(data, is_side_by_side=False):
            """Create a single table from dictionary data"""
            data_copy = data.copy()
            table_data = []

            # Extract header if present
            header = data_copy.pop("header", None)

            # Add heading row if provided
            if header:
                heading_row = [Paragraph(f"<b>{header}</b>", self.styles["KomatsuBody"]), ""]
                table_data.append(heading_row)

            # Add data rows
            for key, value in data_copy.items():
                # Use DataLabel style for keys (grey text)
                key_style = ParagraphStyle(
                    "DataLabel",
                    parent=self.styles["KomatsuBody"],
                    fontSize=10,
                    textColor=KOMATSU_BLACK,
                    fontName="Helvetica-Bold",
                )
                value_style = ParagraphStyle(
                    "DataValue", parent=self.styles["KomatsuBody"], fontSize=10, textColor=KOMATSU_BLACK
                )

                key_para = Paragraph(f"{key}:", key_style)
                value_para = Paragraph(str(value), value_style)
                table_data.append([key_para, value_para])

            # Calculate column widths
            if is_side_by_side:
                col_width = (self.doc.width - 0.3 * inch) / 2
                key_col_width = col_width * 0.35
                value_col_width = col_width * 0.65
            else:
                key_col_width = self.doc.width * 0.25
                value_col_width = self.doc.width * 0.75

            table = Table(table_data, colWidths=[key_col_width, value_col_width])

            # Cleaner table styling
            style_commands = [
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 10),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                ("LINEBELOW", (0, 0), (-1, -1), 0.5, KOMATSU_GRAY),
            ]

            # Header styling if present
            if header:
                style_commands.extend(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), KOMATSU_GRAY),
                        ("SPAN", (0, 0), (-1, 0)),
                        ("ALIGN", (0, 0), (-1, 0), "LEFT"),
                        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ]
                )

            table.setStyle(TableStyle(style_commands))
            return table

        # Handle different input formats (rest of the function remains the same)
        if isinstance(table_data, dict):
            single_table = create_single_table(table_data)
            self.story.append(single_table)
        elif isinstance(table_data, list):
            if len(table_data) == 1:
                single_table = create_single_table(table_data[0])
                self.story.append(single_table)
            elif len(table_data) == 2:
                left_table = create_single_table(table_data[0], is_side_by_side=True)
                right_table = create_single_table(table_data[1], is_side_by_side=True)

                wrapper_data = [[left_table, right_table]]
                col_width = (self.doc.width - 0.3 * inch) / 2
                wrapper_table = Table(wrapper_data, colWidths=[col_width, col_width])
                wrapper_table.setStyle(
                    TableStyle(
                        [
                            ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                            ("VALIGN", (0, 0), (-1, -1), "TOP"),
                            ("LEFTPADDING", (0, 0), (-1, -1), 0),
                            ("RIGHTPADDING", (0, 0), (0, -1), 15),
                        ]
                    )
                )
                self.story.append(wrapper_table)

        self.story.append(Spacer(1, 0.15 * inch))

    def add_component_comparison_table(self, components_data):
        """
        Add a component comparison table showing outgoing vs installed components

        Args:
            components_data: List of dicts, each containing:
                - outgoing: Dict with numero_parte, numero_serie, horas_componente
                - installed: Dict with numero_parte, numero_serie, horas_componente
        """

        # Create styles for the table
        header_style = ParagraphStyle(
            "ComponentHeader",
            parent=self.styles["Normal"],
            fontSize=10,
            textColor=colors.white,
            alignment=TA_CENTER,
            fontName="Helvetica-Bold",
        )

        subheader_style = ParagraphStyle(
            "ComponentSubHeader",
            parent=self.styles["Normal"],
            fontSize=9,
            textColor=colors.black,
            alignment=TA_LEFT,
            fontName="Helvetica-Bold",
        )

        data_style = ParagraphStyle(
            "ComponentData",
            parent=self.styles["Normal"],
            fontSize=9,
            textColor=colors.black,
            alignment=TA_LEFT,
        )

        # Build table data
        table_data = []

        # Main headers
        header_row = [
            Paragraph("Número de pieza saliente o defectuoso", header_style),
            Paragraph("Número de pieza o componente instalado", header_style),
        ]
        table_data.append(header_row)

        # Add component rows
        for component in components_data:
            outgoing = component.get("outgoing", {})
            installed = component.get("installed", {})

            # Component subheaders
            subheader_row = [
                Paragraph("<b>Número de pieza saliente o defectuoso</b>", subheader_style),
                Paragraph("<b>Número de pieza o componente instalado</b>", subheader_style),
            ]
            table_data.append(subheader_row)

            # Part number row
            part_row = [Paragraph(f"Número de parte:", data_style), Paragraph(f"Número de parte:", data_style)]
            table_data.append(part_row)

            # Serial number row
            serial_row = [
                Paragraph(f"Número de serie: <b>{outgoing.get('numero_serie', '')}</b>", data_style),
                Paragraph(f"Número de serie: <b>{installed.get('numero_serie', '')}</b>", data_style),
            ]
            table_data.append(serial_row)

            # Hours row
            hours_row = [
                Paragraph(f"Horas del componente: {outgoing.get('horas_componente', '')}", data_style),
                Paragraph(f"Horas de componente: {installed.get('horas_componente', '')}", data_style),
            ]
            table_data.append(hours_row)

        # Create the table
        table = Table(table_data, colWidths=[self.doc.width * 0.5, self.doc.width * 0.5])

        # Apply styling
        style_commands = [
            # Main header styling
            ("BACKGROUND", (0, 0), (-1, 0), KOMATSU_BLUE),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("ALIGN", (0, 0), (-1, 0), "CENTER"),
            ("VALIGN", (0, 0), (-1, 0), "MIDDLE"),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 10),
            # All other rows
            ("BACKGROUND", (0, 1), (-1, -1), colors.white),
            ("TEXTCOLOR", (0, 1), (-1, -1), colors.black),
            ("ALIGN", (0, 1), (-1, -1), "LEFT"),
            ("VALIGN", (0, 1), (-1, -1), "TOP"),
            # Grid lines
            ("GRID", (0, 0), (-1, -1), 0.5, KOMATSU_GRAY),
            ("LINEBELOW", (0, 0), (-1, 0), 2, colors.black),
            # Padding
            ("TOPPADDING", (0, 0), (-1, 0), 8),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
            ("TOPPADDING", (0, 1), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 1), (-1, -1), 4),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ]

        # Add subheader styling for every 4th row starting from row 1
        row_count = len(table_data)
        for i in range(1, row_count, 4):  # Every 4th row starting from 1
            if i < row_count:
                style_commands.append(("BACKGROUND", (0, i), (-1, i), KOMATSU_GRAY))
                style_commands.append(("FONTNAME", (0, i), (-1, i), "Helvetica-Bold"))

        table.setStyle(TableStyle(style_commands))

        self.story.append(table)
        self.story.append(Spacer(1, 0.15 * inch))

    def add_failure_analysis_table(self, data):
        """
        Add a failure analysis table with specific format

        Args:
            data: Dict with fields:
                - antecedentes: Dict with changeout_date, component_hours, tbo, utilizacion, ot
                - efecto_falla: String
                - pieza: List of strings or single string
                - mecanismo_falla: String
                - modo_falla: String
                - causa_basica: String
                - causa_raiz: String
        """

        # Helper function to format field names
        def format_field_name(name):
            replacements = {
                "changeout_date": "Fecha de cambio",
                "component_hours": "Horas Falla",
                "tbo": "TBO",
                "utilizacion": "Utilización",
                "ot": "OT",
                "efecto_falla": "Efecto falla",
                "pieza": "Pieza",
                "mecanismo_falla": "Mecanismo falla",
                "modo_falla": "Modo falla",
                "causa_basica": "Causa básica",
                "causa_raiz": "Causa raíz",
            }
            return replacements.get(name, name.replace("_", " ").title())

        # Create header style for white text on blue background
        header_style = ParagraphStyle(
            "TableHeaderWhite",
            parent=self.styles["Normal"],
            fontSize=12,
            textColor=colors.white,
            alignment=TA_CENTER,
            fontName="Helvetica-Bold",
        )

        # Create body style
        body_style = ParagraphStyle(
            "TableBody",
            parent=self.styles["Normal"],
            fontSize=10,
            textColor=KOMATSU_BLACK,
            alignment=TA_LEFT,
            leading=12,
        )

        # Build the table data
        table_data = []

        # Header row
        header_row = [
            Paragraph("Antecedentes", header_style),
            Paragraph("Efecto falla", header_style),
            Paragraph("Pieza", header_style),
            Paragraph("Mecanismo<br/>falla", header_style),
            Paragraph("Modo falla", header_style),
            Paragraph("Causa básica", header_style),
            Paragraph("Causa raíz", header_style),
        ]
        table_data.append(header_row)

        # Format antecedentes data
        antecedentes_text = ""
        if "antecedentes" in data and isinstance(data["antecedentes"], dict):
            ant = data["antecedentes"]
            if "changeout_date" in ant:
                antecedentes_text += f"<b>Fecha de cambio:</b><br/>{ant['changeout_date']}<br/>"
            if "component_hours" in ant:
                antecedentes_text += f"<b>Horas Falla:</b><br/>{ant['component_hours']}<br/>"
            if "tbo" in ant:
                antecedentes_text += f"<b>TBO:</b> {ant['tbo']}<br/>"
            if "utilizacion" in ant:
                antecedentes_text += f"<b>Utilización:</b> {ant['utilizacion']}<br/>"
            if "ot" in ant:
                antecedentes_text += f"<b>OT:</b> {ant['ot']}"

        # Format pieza data (handle list or string)
        pieza_text = ""
        if "pieza" in data:
            if isinstance(data["pieza"], list):
                for item in data["pieza"]:
                    pieza_text += f"• {item}<br/>"
                pieza_text = pieza_text.rstrip("<br/>")
            else:
                pieza_text = str(data["pieza"])

        # Data row
        data_row = [
            Paragraph(antecedentes_text, body_style),
            Paragraph(data.get("efecto_falla", ""), body_style),
            Paragraph(pieza_text, body_style),
            Paragraph(data.get("mecanismo_falla", ""), body_style),
            Paragraph(data.get("modo_falla", ""), body_style),
            Paragraph(data.get("causa_basica", ""), body_style),
            Paragraph(data.get("causa_raiz", ""), body_style),
        ]
        table_data.append(data_row)

        # Calculate column widths
        total_width = self.doc.width
        col_widths = [
            total_width * 0.18,  # Antecedentes
            total_width * 0.15,  # Efecto falla
            total_width * 0.13,  # Pieza
            total_width * 0.13,  # Mecanismo falla
            total_width * 0.13,  # Modo falla
            total_width * 0.15,  # Causa básica
            total_width * 0.13,  # Causa raíz
        ]

        # Create the table
        table = Table(table_data, colWidths=col_widths)

        # Apply table style
        table.setStyle(
            TableStyle(
                [
                    # Header row styling
                    ("BACKGROUND", (0, 0), (-1, 0), KOMATSU_BLUE),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("ALIGN", (0, 0), (-1, 0), "CENTER"),
                    ("VALIGN", (0, 0), (-1, 0), "MIDDLE"),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), 12),
                    # Data row styling
                    ("BACKGROUND", (0, 1), (-1, -1), colors.white),
                    ("TEXTCOLOR", (0, 1), (-1, -1), KOMATSU_BLACK),
                    ("ALIGN", (0, 1), (-1, -1), "LEFT"),
                    ("VALIGN", (0, 1), (-1, -1), "TOP"),
                    # Grid
                    ("GRID", (0, 0), (-1, -1), 1, KOMATSU_BLACK),
                    ("LINEBELOW", (0, 0), (-1, 0), 2, KOMATSU_BLACK),
                    # Padding
                    ("TOPPADDING", (0, 0), (-1, 0), 10),
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 10),
                    ("TOPPADDING", (0, 1), (-1, -1), 8),
                    ("BOTTOMPADDING", (0, 1), (-1, -1), 8),
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                    # Row height for data
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white]),
                    ("MINROWHEIGHT", (0, 1), (-1, -1), 2.5 * inch),  # Minimum height for data rows
                ]
            )
        )

        self.story.append(table)
        self.story.append(Spacer(1, 0.3 * inch))

    def add_repair_history(self, repairs, timeline_events=None):
        """
        Add a repair history table with timeline

        Args:
            repairs: List of dicts with:
                - fecha_reparacion: Repair date
                - horas_componente: Component hours
                - horas_acumuladas: Accumulated hours
                Maximum 4 records
            timeline_events: List of dicts with:
                - position: Index of the repair (0-based) where the event should appear
                - text: Text to display in the blue box (can be multi-line list)
        """
        # Limit to 4 records
        repairs = repairs[:4]

        # Create table style
        header_style = ParagraphStyle(
            "RepairHeader",
            parent=self.styles["Normal"],
            fontSize=11,
            textColor=colors.black,
            alignment=TA_CENTER,
            fontName="Helvetica-Bold",
        )

        data_style = ParagraphStyle(
            "RepairData", parent=self.styles["Normal"], fontSize=11, textColor=colors.black, alignment=TA_CENTER
        )

        # Build table data
        table_data = []

        # Headers
        header_row = [Paragraph("Fecha<br/>Reparación", header_style)]
        hours_row = [Paragraph("Horas<br/>Componente", header_style)]
        accumulated_row = [Paragraph("Horas<br/>Acumuladas", header_style)]

        # Add data columns
        for repair in repairs:
            header_row.append(Paragraph(repair.get("fecha_reparacion", ""), data_style))
            hours_row.append(Paragraph(str(repair.get("horas_componente", "")), data_style))
            accumulated_row.append(Paragraph(str(repair.get("horas_acumuladas", "")), data_style))

        table_data = [header_row, hours_row, accumulated_row]

        # Calculate column widths
        num_cols = len(repairs) + 1
        label_width = 1.5 * inch
        remaining_width = self.doc.width - label_width
        data_col_width = remaining_width / len(repairs)

        col_widths = [label_width] + [data_col_width] * len(repairs)

        # Create table
        table = Table(table_data, colWidths=col_widths)

        # Apply table style
        table.setStyle(
            TableStyle(
                [
                    # All cells
                    ("GRID", (0, 0), (-1, -1), 1, colors.black),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("TOPPADDING", (0, 0), (-1, -1), 8),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                    # First column (labels)
                    ("BACKGROUND", (0, 0), (0, -1), KOMATSU_GRAY),
                    ("ALIGN", (0, 0), (0, -1), "CENTER"),
                    ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
                    # Data columns
                    ("ALIGN", (1, 0), (-1, -1), "CENTER"),
                    ("BACKGROUND", (1, 0), (-1, -1), colors.white),
                ]
            )
        )

        self.story.append(table)
        self.story.append(Spacer(1, 0.3 * inch))

        # Add timeline if there are timeline events
        if timeline_events and len(timeline_events) > 0:

            class RepairTimelineFlowable(Flowable):
                def __init__(self, width, repairs, events):
                    Flowable.__init__(self)
                    self.width = width
                    self.repairs = repairs
                    self.events = events
                    self.height = 2.5 * inch

                def draw(self):
                    # Timeline parameters
                    timeline_y = 2.0 * inch
                    arrow_height = 0.15 * inch
                    timeline_start_x = 1.5 * inch + 0.1 * inch  # Align with table
                    timeline_length = self.width - 1.5 * inch - 0.2 * inch
                    timeline_end_x = timeline_start_x + timeline_length

                    # Draw the blue arrow line
                    self.canv.setStrokeColor(KOMATSU_BLUE)
                    self.canv.setLineWidth(3)
                    self.canv.line(timeline_start_x, timeline_y, timeline_end_x, timeline_y)

                    # Draw arrow head
                    p = self.canv.beginPath()
                    p.moveTo(timeline_end_x, timeline_y)
                    p.lineTo(timeline_end_x - arrow_height, timeline_y - arrow_height / 2)
                    p.lineTo(timeline_end_x - arrow_height, timeline_y + arrow_height / 2)
                    p.close()
                    self.canv.setFillColor(KOMATSU_BLUE)
                    self.canv.drawPath(p, fill=1, stroke=0)

                    # Calculate positions for events (align with table columns)
                    num_repairs = len(self.repairs)
                    spacing = timeline_length / num_repairs
                    positions = [timeline_start_x + (i + 0.5) * spacing for i in range(num_repairs)]

                    # Calculate maximum available width for each box to prevent overlap
                    max_box_width = spacing * 0.9  # Use 90% of available space between points

                    # Draw events
                    for event in self.events:
                        pos_idx = event.get("position", 0)
                        if pos_idx < len(positions):
                            x_pos = positions[pos_idx]

                            # Draw vertical line
                            self.canv.setStrokeColor(KOMATSU_BLUE)
                            self.canv.setLineWidth(2)
                            self.canv.line(x_pos, timeline_y, x_pos, timeline_y - 0.3 * inch)

                            # Draw arrow head pointing down
                            arrow_p = self.canv.beginPath()
                            arrow_p.moveTo(x_pos, timeline_y - 0.3 * inch)
                            arrow_p.lineTo(x_pos - 0.08 * inch, timeline_y - 0.15 * inch)
                            arrow_p.lineTo(x_pos + 0.08 * inch, timeline_y - 0.15 * inch)
                            arrow_p.close()
                            self.canv.setFillColor(KOMATSU_BLUE)
                            self.canv.drawPath(arrow_p, fill=1, stroke=0)

                            # Process text (can be string or list)
                            text_content = event.get("text", "")
                            if isinstance(text_content, list):
                                lines = text_content
                            else:
                                lines = text_content.split("\n")

                            # Calculate box dimensions based on content
                            num_lines = len(lines)
                            line_height = 0.13 * inch  # Reduced line height
                            box_padding = 0.08 * inch  # Reduced padding
                            box_height = (num_lines * line_height) + (2 * box_padding)

                            # Determine box width based on longest line, but constrained
                            self.canv.setFont("Helvetica-Bold", 8)  # Smaller font
                            max_line_width = 0
                            for line in lines:
                                line_width = self.canv.stringWidth(line, "Helvetica-Bold", 8)
                                max_line_width = max(max_line_width, line_width)

                            # Calculate box width with constraints
                            desired_box_width = max_line_width + (2 * box_padding) + 0.1 * inch
                            box_width = min(desired_box_width, max_box_width)  # Don't exceed maximum
                            box_width = max(box_width, 1.2 * inch)  # Minimum width reduced

                            # Draw box
                            box_y = timeline_y - 0.5 * inch - box_height
                            box_x = x_pos - box_width / 2

                            self.canv.setFillColor(KOMATSU_BLUE)
                            self.canv.roundRect(box_x, box_y, box_width, box_height, 5, fill=1, stroke=0)

                            # Draw text lines with word wrapping if needed
                            self.canv.setFillColor(colors.white)
                            self.canv.setFont("Helvetica-Bold", 8)  # Smaller font

                            # Center text vertically in box
                            text_start_y = box_y + box_height - box_padding - 0.10 * inch

                            for i, line in enumerate(lines):
                                y_pos = text_start_y - (i * line_height)

                                # Check if text fits in box width
                                text_width = self.canv.stringWidth(line, "Helvetica-Bold", 8)
                                if text_width > box_width - (2 * box_padding):
                                    # Truncate text with ellipsis if too long
                                    while text_width > box_width - (2 * box_padding) - 0.1 * inch and len(line) > 3:
                                        line = line[:-1]
                                        text_width = self.canv.stringWidth(line + "...", "Helvetica-Bold", 8)
                                    line = line + "..."

                                self.canv.drawCentredString(x_pos, y_pos, line)

                def wrap(self, availWidth, availHeight):
                    return (self.width, self.height)

            # Add the timeline
            timeline = RepairTimelineFlowable(self.doc.width, repairs, timeline_events)
            self.story.append(timeline)
            self.story.append(Spacer(1, 0.2 * inch))

    def add_component_status(self, image_path, tables_data):
        """
        Add component status visualization with image and repair history tables

        Args:
            image_path: Path to the component image
            tables_data: List of dicts (max 4), each containing:
                - title: Table title
                - repairs: List of repair column headers (e.g., ['Original', 'Repair 1', 'Repair 2'])
                - parts: List of dicts with:
                    - name: Part name
                    - health: List of health values (0-1) for each repair column
                             1 = green (healthy), 0.5 = yellow (damaged), 0 = red (critical)
        """

        class ComponentStatusFlowable(Flowable):
            def __init__(self, img_path, tables, doc_width, styles):
                Flowable.__init__(self)
                self.img_path = img_path
                self.tables = tables[:4]  # Limit to 4 tables
                self.doc_width = doc_width
                self.styles = styles
                self.height = 4 * inch  # Fixed height for better fit

            def get_health_color(self, health):
                """Get color based on health value (0-1)"""
                if health >= 0.8:
                    return colors.HexColor("#00C851")  # Green
                elif health >= 0.3:
                    return colors.HexColor("#FFB300")  # Yellow/Orange
                else:
                    return colors.HexColor("#ff4444")  # Red

            def draw(self):
                # Draw smaller image on the left (quarter page)
                img_height = 1.5 * inch  # Default
                img_width = 1.5 * inch

                if os.path.exists(self.img_path):
                    img = Image(self.img_path)
                    max_img_width = 1.5 * inch
                    max_img_height = 1.5 * inch

                    # Scale image to fit
                    aspect = img.imageHeight / img.imageWidth
                    if aspect > 1:
                        img_width = max_img_height / aspect
                        img_height = max_img_height
                    else:
                        img_width = max_img_width
                        img_height = max_img_width * aspect

                    img_x = 0.1 * inch
                    img_y = (self.height - img_height) / 2
                    self.canv.drawImage(
                        self.img_path,
                        img_x,
                        img_y,
                        width=img_width,
                        height=img_height,
                        preserveAspectRatio=True,
                        mask="auto",
                    )

                # Calculate vertical center of image for arrow alignment
                image_center_y = self.height / 2

                # Draw arrow at image center height
                arrow_x = 1.8 * inch
                arrow_y = image_center_y
                arrow_length = 0.5 * inch

                # Arrow shaft
                self.canv.setStrokeColor(KOMATSU_YELLOW)
                self.canv.setLineWidth(3)
                self.canv.line(arrow_x, arrow_y, arrow_x + arrow_length - 0.1 * inch, arrow_y)

                # Arrow head
                p = self.canv.beginPath()
                p.moveTo(arrow_x + arrow_length, arrow_y)
                p.lineTo(arrow_x + arrow_length - 0.1 * inch, arrow_y - 0.08 * inch)
                p.lineTo(arrow_x + arrow_length - 0.1 * inch, arrow_y + 0.08 * inch)
                p.close()
                self.canv.setFillColor(KOMATSU_YELLOW)
                self.canv.drawPath(p, fill=1, stroke=0)

                # Draw tables - vertically centered with image
                table_start_x = arrow_x + arrow_length + 0.2 * inch
                available_width = self.doc_width - table_start_x + 0.3 * inch

                num_tables = len(self.tables)
                if num_tables == 0:
                    return

                # Calculate table dimensions
                table_spacing = 0.1 * inch
                table_width = (available_width - (num_tables - 1) * table_spacing) / num_tables

                # Calculate max table height to center tables vertically
                # Estimate based on number of parts
                max_parts = max(len(table.get("parts", [])) for table in self.tables) if self.tables else 0
                estimated_table_height = 0.25 * inch + 0.15 * inch + (max_parts * 0.12 * inch) + 0.2 * inch

                # Center tables vertically with image
                table_top_y = image_center_y + (estimated_table_height / 2)

                # Draw each table
                for i, table_data in enumerate(self.tables):
                    table_x = table_start_x + i * (table_width + table_spacing)
                    table_y = table_top_y

                    # Table title with blue background
                    self.canv.setFillColor(KOMATSU_BLUE)
                    title_height = 0.25 * inch
                    self.canv.rect(table_x, table_y - title_height, table_width, title_height, fill=1, stroke=0)
                    self.canv.setFillColor(colors.white)
                    self.canv.setFont("Helvetica-Bold", 8)

                    # Wrap title if too long
                    title = table_data.get("title", "")
                    if self.canv.stringWidth(title, "Helvetica-Bold", 8) > table_width - 0.1 * inch:
                        # Split into two lines
                        words = title.split()
                        mid = len(words) // 2
                        line1 = " ".join(words[:mid])
                        line2 = " ".join(words[mid:])
                        self.canv.drawCentredString(table_x + table_width / 2, table_y - 0.08 * inch, line1)
                        self.canv.drawCentredString(table_x + table_width / 2, table_y - 0.18 * inch, line2)
                    else:
                        self.canv.drawCentredString(table_x + table_width / 2, table_y - 0.15 * inch, title)

                    # Column headers (repairs)
                    current_y = table_y - title_height - 0.15 * inch
                    repairs = table_data.get("repairs", ["Original"])
                    num_repairs = len(repairs)

                    # Calculate column widths
                    part_col_width = table_width * 0.4  # 40% for part names
                    repair_col_width = (table_width - part_col_width) / num_repairs

                    # Draw header row
                    self.canv.setFillColor(KOMATSU_GRAY)
                    self.canv.setFont("Helvetica-Bold", 6)
                    self.canv.drawString(table_x + 0.02 * inch, current_y, "Item")

                    # Repair headers
                    for j, repair in enumerate(repairs):
                        repair_x = table_x + part_col_width + j * repair_col_width
                        # Truncate repair name if needed
                        max_width = repair_col_width - 0.02 * inch
                        while self.canv.stringWidth(repair, "Helvetica-Bold", 6) > max_width:
                            repair = repair[:-1]
                        self.canv.drawCentredString(repair_x + repair_col_width / 2, current_y, repair)

                    # Draw parts and health status
                    current_y -= 0.15 * inch
                    self.canv.setFont("Helvetica", 6)

                    parts = table_data.get("parts", [])[:12]  # Limit rows

                    for part in parts:
                        # Draw part name
                        self.canv.setFillColor(colors.black)
                        part_name = part.get("name", "")
                        # Truncate if too long
                        max_part_width = part_col_width - 0.05 * inch
                        while self.canv.stringWidth(part_name, "Helvetica", 6) > max_part_width:
                            part_name = part_name[:-1]
                        self.canv.drawString(table_x + 0.02 * inch, current_y, part_name)

                        # Draw health circles for each repair
                        health_values = part.get("health", [])
                        for j, health in enumerate(health_values[:num_repairs]):
                            repair_x = table_x + part_col_width + j * repair_col_width
                            circle_x = repair_x + repair_col_width / 2
                            circle_y = current_y + 0.03 * inch
                            circle_radius = 0.03 * inch

                            # Get color based on health
                            health_color = self.get_health_color(health)
                            self.canv.setFillColor(health_color)
                            self.canv.circle(circle_x, circle_y, circle_radius, fill=1, stroke=0)

                        current_y -= 0.12 * inch

                    # Draw light border around table
                    self.canv.setStrokeColor(KOMATSU_GRAY)
                    self.canv.setLineWidth(0.5)
                    table_height = table_y - current_y + 0.05 * inch
                    self.canv.rect(table_x, current_y - 0.05 * inch, table_width, table_height, fill=0, stroke=1)

            def wrap(self, availWidth, availHeight):
                return (self.doc_width, self.height)

        # Create and add the component status flowable
        status = ComponentStatusFlowable(image_path, tables_data, self.doc.width, self.styles)
        self.story.append(status)
        self.story.append(Spacer(1, 0.3 * inch))

    # def build(self):
    #     """Generate the PDF"""
    #     self.doc.build(self.story, onFirstPage=self._header_footer, onLaterPages=self._header_footer)
    #     print(f"PDF created: {self.filename}")
    def build(self):
        """Generate the PDF"""
        self.doc.build(self.story, onFirstPage=self._header_footer, onLaterPages=self._header_footer)

        if self.output_to_memory:
            # Get the PDF bytes
            pdf_bytes = self.buffer.getvalue()
            self.buffer.close()
            return pdf_bytes
        else:
            print(f"PDF created: {self.filename}")
            return None


# Example usage:
if __name__ == "__main__":
    # Create Komatsu PDF instance
    pdf = TidyReport("komatsu_report.pdf", report_title="REPORTE TÉCNICO", output_to_memory=False)

    # Add content
    pdf.add_section("KOMATSU REPORT 2025")

    pdf.add_heading("Executive Summary")
    pdf.add_paragraph(
        "This report provides a comprehensive overview of our operations and performance metrics for the current quarter."
    )

    pdf.add_separator()

    # Single photo example
    # pdf.add_photo("equipment_photo.jpg")

    # Two photos example
    # pdf.add_photo(["photo1.jpg", "photo2.jpg"])

    # Photos with custom titles
    # pdf.add_photos(["komatsu_logo.png", "bhp_logo.png"], titles=["Equipment Overview", "Site Operations"])

    pdf.add_heading("Key Performance Indicators")
    pdf.add_accent_text("Q4 2024 Results: Exceeding Expectations")
    pdf.add_paragraph("Our team has achieved remarkable results this quarter.")

    # Test new improved table functionality
    pdf.add_heading("Información del Reporte")

    # First example - side by side tables without headers (using new list format)
    pdf.add_table(
        [
            {
                "header": "Componente saliente",
                "Faena": "Minera Escondida",
                "Realizado por": "Sebastian Cepeda Ferruzola",
                "Cargo": "Asesor Técnico",
            },
            {
                "header": "Componente saliente",
                "Fecha cambio": "14-06-2025",
                "Fecha reporte": "26-06-2025",
                "Correo": "sebastian.cepeda@global.komatsu",
            },
        ]
    )

    # Second example - side by side tables with headers (using new format)
    pdf.add_table(
        [
            {"header": "Componente saliente", "Número de serie": "WX14110045", "Horas": "18852 HRS."},
            {"header": "Componente instalado", "Número de serie": "WX14080335", "Horas": "0 HRS."},
        ]
    )

    # Example of single table with header
    pdf.add_table(
        {
            "header": "Información Adicional",
            "Técnico responsable": "Juan Pérez",
            "Supervisor": "María González",
            "Turno": "Día",
        }
    )

    # Add timeline
    pdf.add_heading("Historial de Mantenimiento")
    pdf.add_timeline(
        [
            {
                "date": "2025-11-26",
                "description": "Alarmas A190 (baja lubricación), relleno depósito, engrasar al punto, limpieza conector sensor riel trasero",
            },
            {
                "date": "2025-12-24",
                "description": "Alarmas A190 (baja lubricación), Se rellena depósito grasa",
            },
            # {"date": "2025-01-22", "description": "Engrasar al punto mitigar condición"},
            # {"date": "2025-02-19", "description": "Cambio de componente"},
            {"date": "2025-02-20", "description": "Lubricación de Punto"},
            {
                "date": "2025-03-18",
                "description": "Alarmas A190 (baja lubricación), rótulas sin lubricación, fuga grasa inyector trasero",
            },
        ]
    )

    pdf.add_section("2. Análisis Técnico")
    pdf.add_paragraph("El análisis detallado de los componentes revela...")

    pdf.add_failure_analysis_table(
        {
            "antecedentes": {
                "changeout_date": "2025-03-24",
                "component_hours": "28729",
                "tbo": "30000",
                "utilizacion": "96%",
                "ot": "18132204",
            },
            "efecto_falla": "Fuga en vástago.",
            "pieza": ["Carcasa", "Vástago", "Rótulas"],  # Can be a list or single string
            "mecanismo_falla": "Desgaste por lubricación deficiente.",
            "modo_falla": "Debilitamiento del material.",
            "causa_basica": "Desgaste acelerado y generación de holgura.",
            "causa_raiz": "Lubricación deficiente.",
        }
    )

    pdf.add_heading("Historial de Reparaciones")

    # Example with custom timeline events
    pdf.add_repair_history(
        repairs=[
            {"fecha_reparacion": "2012-09-26", "horas_componente": "0", "horas_acumuladas": "0"},
            {"fecha_reparacion": "2016-04-05", "horas_componente": "19670", "horas_acumuladas": "19670"},
            {"fecha_reparacion": "2020-06-02", "horas_componente": "24514", "horas_acumuladas": "44184"},
            {"fecha_reparacion": "2025-04-27", "horas_componente": "27745", "horas_acumuladas": "71929"},
        ],
        timeline_events=[
            {"position": 0, "text": ["Armado de", "Componente"]},
            {
                "position": 1,
                "text": ["Fuga por Sello Espejo;", "Componente", "cambiados de mayor", "costo, Rodamientos"],
            },
            {"position": 2, "text": ["Cumplimiento TBO ;", "Componente", "cambiados de mayor", "costo, Rodamientos"]},
            {
                "position": 3,
                "text": [
                    "Cumplimiento TBO;",
                    "Componente",
                    "cambiados de mayor",
                    "costo, 3 engra. baja",
                    "velocidad, 2 engra alta",
                    "velocidad. Tiene",
                    "campaña RETROFIT",
                ],
            },
        ],
    )

    # Example usage
    pdf.add_heading("Estado de Componentes")

    pdf.add_component_status(
        image_path="transmission_image.png",
        tables_data=[
            {
                "title": "Table 1",
                "repairs": ["Orig", "R1", "R2", "R3"],  # 4 columns
                "parts": [
                    {"name": "Part A", "health": [1.0, 0.8, 0.6, 0.4]},
                    {"name": "Part B", "health": [1.0, 0.9, 0.7, 0.5]},
                ],
            },
            {
                "title": "Table 2",
                "repairs": ["Orig", "R1", "R2"],  # 3 columns
                "parts": [
                    {"name": "Part C", "health": [1.0, 0.5, 0.3]},
                    {"name": "Part D", "health": [0.8, 0.6, 0.4]},
                ],
            },
            {
                "title": "Table 3",
                "repairs": ["Init", "Final"],  # 2 columns
                "parts": [
                    {"name": "Part E", "health": [1.0, 0.7]},
                    {"name": "Part F", "health": [0.9, 0.5]},
                ],
            },
            {
                "title": "Table 4",
                "repairs": ["Orig", "R1", "R2"],  # 3 columns
                "parts": [
                    {"name": "Part G", "health": [1.0, 0.8, 0.6]},
                    {"name": "Part H", "health": [0.7, 0.5, 0.3]},
                ],
            },
        ],
    )
    # Define tu diccionario de análisis (el que te di en la respuesta anterior)
    analisis_5_porques = {
        "titulo": "Análisis Causa Raíz: Falla Suspensión Trasera TK851",
        "nodos": [
            # Nivel 1: El Problema
            {"id": "1", "nivel": 1, "rama": "Problema", "texto": "Rótula superior fracturada", "conclusion": None},
            # Nivel 2: Causa Física Inmediata
            {
                "id": "2",
                "nivel": 2,
                "rama": "Causa Física",
                "texto": "Porque la rótula se debilitó por desgaste acelerado.",
                "conclusion": None,
            },
            # Nivel 3: Ramas de Investigación
            {
                "id": "3A",
                "nivel": 3,
                "rama": "Mantenimiento",
                "texto": "Porque el sistema de autolubricación falló en completar sus ciclos (Alarma A190 recurrente).",
                "conclusion": None,
            },
            {
                "id": "3B",
                "nivel": 3,
                "rama": "Reparación",
                "texto": "Hipótesis: El desgaste se debió a una fatiga prematura del componente por un defecto anterior.",
                "conclusion": None,
            },
            {
                "id": "3C",
                "nivel": 3,
                "rama": "Operación",
                "texto": "Hipótesis: La falla del sistema se debió a sobreesfuerzos operacionales.",
                "conclusion": None,
            },
            # Nivel 4: Desarrollo de la Investigación
            {
                "id": "4A",
                "nivel": 4,
                "rama": "Mantenimiento",
                "texto": "Porque las intervenciones se limitaron a acciones paliativas (rellenar grasa) sin corregir la causa de la alarma.",
                "conclusion": None,
            },
            {
                "id": "4B",
                "nivel": 4,
                "rama": "Reparación",
                "texto": "Análisis: Pruebas no destructivas (END) de la reparación anterior fueron satisfactorias.",
                "conclusion": None,
            },
            {
                "id": "4C",
                "nivel": 4,
                "rama": "Operación",
                "texto": "Análisis: Se revisaron datos de carga (GrossPayload) y velocidad, comparándolos con los límites de diseño.",
                "conclusion": None,
            },
            # Nivel 5: Conclusiones por Rama
            {
                "id": "5A",
                "nivel": 5,
                "rama": "Mantenimiento",
                "texto": "La criticidad de las alarmas se evalúa de forma individual. La alta recurrencia de la alarma A190 generó una condición latente que no fue visibilizada como un riesgo mayor.",
                "conclusion": "Causa Raíz",
            },
            {
                "id": "5B",
                "nivel": 5,
                "rama": "Reparación",
                "texto": "El estado inicial del componente no demuestra causalidad. La fatiga fue una consecuencia, no la causa.",
                "conclusion": "Hipótesis Descartada",
            },
            {
                "id": "5C",
                "nivel": 5,
                "rama": "Operación",
                "texto": "Los datos confirman que no existieron sobrecargas ni abusos operacionales.",
                "conclusion": "Hipótesis Descartada",
            },
        ],
        "conexiones": [
            {"origen": "1", "destino": "2"},
            {"origen": "2", "destino": "3A"},
            {"origen": "2", "destino": "3B"},
            {"origen": "2", "destino": "3C"},
            {"origen": "3A", "destino": "4A"},
            {"origen": "3B", "destino": "4B"},
            {"origen": "3C", "destino": "4C"},
            {"origen": "4A", "destino": "5A"},
            {"origen": "4B", "destino": "5B"},
            {"origen": "4C", "destino": "5C"},
        ],
    }

    # Llama a la nueva función para dibujar el gráfico
    pdf.add_five_whys_chart(analisis_5_porques)
    # Build the PDF
    pdf.build()
    # Example usage
