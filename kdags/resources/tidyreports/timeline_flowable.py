from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import (
    Flowable,
    # Flowable,
)

KOMATSU_BLUE = colors.HexColor("#140a9a")  # Azul Gloria
KOMATSU_YELLOW = colors.HexColor("#ffc82f")  # Natural Yellow


class TimelineFlowable(Flowable):
    def __init__(self, events, width, styles):
        Flowable.__init__(self)
        self.events = events
        self.width = width
        self.styles = styles
        self.height = 4.5 * inch  # Fixed height for timeline

    def draw(self):
        # Timeline parameters
        timeline_y = 2.2 * inch  # Center line vertically
        arrow_height = 0.3 * inch
        timeline_start_x = 0.1 * inch  # Reduced margin for more space
        timeline_end_x = self.width - 0.1 * inch  # Extended arrow to prevent date cropping
        timeline_length = timeline_end_x - timeline_start_x

        # Draw the blue arrow bar
        self.canv.setFillColor(KOMATSU_BLUE)

        # Main rectangle body
        self.canv.rect(
            timeline_start_x,
            timeline_y - arrow_height / 2,
            timeline_length - arrow_height,
            arrow_height,
            fill=1,
            stroke=0,
        )

        # Arrow head (triangle) - using path instead of drawPolygon
        p = self.canv.beginPath()
        p.moveTo(timeline_end_x - arrow_height, timeline_y - arrow_height / 2)
        p.lineTo(timeline_end_x - arrow_height, timeline_y + arrow_height / 2)
        p.lineTo(timeline_end_x, timeline_y)
        p.close()
        self.canv.setFillColor(KOMATSU_BLUE)
        self.canv.drawPath(p, fill=1, stroke=0)

        # Calculate positions for events
        num_events = len(self.events)
        if num_events > 1:
            spacing = (timeline_length - arrow_height - 1 * inch) / (num_events - 1)
        else:
            spacing = 0

        # Draw events
        for i, event in enumerate(self.events):
            # Calculate x position
            if num_events > 1:
                x_pos = timeline_start_x + 0.5 * inch + (i * spacing)
            else:
                x_pos = timeline_start_x + timeline_length / 2

            # Determine position (top/bottom)
            if "position" in event:
                position = event["position"]
            else:
                # Alternate if not specified
                position = "top" if i % 2 == 0 else "bottom"

            # Draw date on timeline
            self.canv.setFont("Helvetica-Bold", 9)
            self.canv.setFillColor(colors.white)
            self.canv.drawCentredString(x_pos, timeline_y - 5, event["date"])

            # Draw yellow box with content
            box_width = 2.2 * inch
            box_height = 0.8 * inch

            # Calculate box position
            box_x = x_pos - box_width / 2
            if position == "top":
                box_y = timeline_y + arrow_height / 2 + 0.3 * inch
                line_start_y = timeline_y + arrow_height / 2
                line_end_y = box_y
            else:
                box_y = timeline_y - arrow_height / 2 - 0.3 * inch - box_height
                line_start_y = timeline_y - arrow_height / 2
                line_end_y = box_y + box_height

            # Draw connecting line
            self.canv.setStrokeColor(colors.black)
            self.canv.setLineWidth(1)
            self.canv.setDash(2, 2)  # Dashed line
            self.canv.line(x_pos, line_start_y, x_pos, line_end_y)

            # Draw yellow box with dashed border
            self.canv.setFillColor(KOMATSU_YELLOW)
            self.canv.setStrokeColor(colors.black)
            self.canv.setDash(3, 3)  # Dashed border
            self.canv.rect(box_x, box_y, box_width, box_height, fill=1, stroke=1)
            self.canv.setDash()  # Reset to solid line

            # Add text to box
            text_y = box_y + box_height - 0.15 * inch
            self.canv.setFont("Helvetica", 9)
            self.canv.setFillColor(colors.black)

            # Split text into lines if too long
            words = event["description"].split()
            lines = []
            current_line = []

            for word in words:
                test_line = " ".join(current_line + [word])
                if self.canv.stringWidth(test_line, "Helvetica", 9) < box_width - 0.2 * inch:
                    current_line.append(word)
                else:
                    if current_line:
                        lines.append(" ".join(current_line))
                    current_line = [word]
            if current_line:
                lines.append(" ".join(current_line))

            # Draw text lines
            for j, line in enumerate(lines[:4]):  # Max 4 lines
                self.canv.drawCentredString(x_pos, text_y - (j * 0.15 * inch), line)

    def wrap(self, availWidth, availHeight):
        return (self.width, self.height)
