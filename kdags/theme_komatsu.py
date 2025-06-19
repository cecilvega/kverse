import polars as pl
import plotnine as p9
from datetime import datetime
import warnings

warnings.filterwarnings("ignore")

# ============================================================================
# KOMATSU THEME CONFIGURATION
# ============================================================================

# Komatsu color palette from theme_komatsu.py
KOMATSU_COLORS = {
    "primary_blue": "#003F87",
    "primary_yellow": "#FFCC00",
    "light_blue": "#0066CC",
    "gray": "#E5E5E5",
    "text_primary": "#333333",
    "text_secondary": "#666666",
    "grid_color": "#E0E0E0",
}
# Extended color palette for multiple series
COMPONENT_COLORS = [
    "#003F87",  # Deep blue
    "#FFCC00",  # Yellow
    "#333333",  # Dark gray
    "#0066CC",  # Light blue
    "#9370DB",  # Medium purple
    "#FFA500",  # Orange
    "#808080",  # Gray
    "#4169E1",  # Royal blue
]


# Enhanced Theme Komatsu function
def theme_komatsu(figure_size=(7, 5)):
    """Enhanced Komatsu corporate theme for plotnine plots"""
    return p9.theme_minimal() + p9.theme(
        figure_size=figure_size,
        # Text elements - BALANCED SIZES
        text=p9.element_text(family="Arial", color=KOMATSU_COLORS["text_primary"], size=9),
        axis_text_x=p9.element_text(rotation=0, ha="center", size=8, color="#333333"),
        axis_text_y=p9.element_text(size=8, color="#333333"),
        axis_title_x=p9.element_text(size=10, weight="bold", margin={"t": 8}),
        axis_title_y=p9.element_text(size=10, weight="bold", margin={"r": 8}),
        plot_title=p9.element_text(
            size=14, weight="bold", color=KOMATSU_COLORS["primary_blue"], ha="left", margin={"b": 3}  # Increased
        ),
        plot_subtitle=p9.element_text(
            size=10, color=KOMATSU_COLORS["text_secondary"], ha="left", margin={"b": 10}  # Increased
        ),
        # Legend - balanced size
        legend_position="top",
        legend_direction="horizontal",
        legend_title=p9.element_blank(),
        legend_text=p9.element_text(size=8),
        legend_background=p9.element_blank(),
        legend_key=p9.element_blank(),
        legend_key_size=10,
        legend_key_spacing=5,
        legend_box_margin=0,
        legend_margin=0,
        legend_box_spacing=0,
        # Grid
        panel_grid_major_x=p9.element_blank(),
        panel_grid_minor=p9.element_blank(),
        panel_grid_major_y=p9.element_line(color="#E5E5E5", size=0.4, linetype="solid"),
        # Panel
        panel_border=p9.element_blank(),
        panel_background=p9.element_rect(fill="white"),
        plot_background=p9.element_rect(fill="white", color="white"),
        # Axis
        axis_line_x=p9.element_line(color="#333333", size=0.5),
        axis_line_y=p9.element_blank(),
        axis_ticks=p9.element_line(color="#333333", size=0.5),
        axis_ticks_length=4,
        # Margins - balanced
        plot_margin_top=0.08,
        plot_margin_bottom=0.08,
        plot_margin_left=0.08,
        plot_margin_right=0.08,
    )


# ============================================================================
# SIMPLE BAR PLOT
# ============================================================================


def komatsu_bar_plot(
    df,
    x,
    y,
    fill=None,
    title="",
    subtitle="",
    x_label=None,
    y_label=None,
    legend_title=None,
    color_palette=None,
    figure_size=(14, 7),
    bar_width=0.8,
    show_values=False,
    value_format="{:.0f}",
    x_axis_rotation=0,
):
    """
    Create a simple bar plot with Komatsu theme

    Args:
        df: DataFrame (pandas or polars - will convert)
        x: Column name for x-axis
        y: Column name for y-axis values
        fill: Column name for bar colors (optional)
        title: Plot title
        subtitle: Plot subtitle
        x_label: X-axis label (defaults to column name)
        y_label: Y-axis label (defaults to column name)
        legend_title: Legend title (defaults to fill column name)
        color_palette: List of colors or dict mapping
        figure_size: Tuple of (width, height)
        bar_width: Width of bars (0-1)
        show_values: Show value labels on bars
        value_format: Format string for values
        x_axis_rotation: Rotation angle for x-axis labels

    Returns:
        plotnine plot object
    """
    # Convert polars to pandas if needed
    plot_df = df.to_pandas() if hasattr(df, "to_pandas") else df

    # Set defaults
    x_label = x_label or x
    y_label = y_label or y

    # Base plot
    if fill:
        plot = p9.ggplot(plot_df, p9.aes(x=x, y=y, fill=fill))
        legend_title = legend_title or fill
    else:
        plot = p9.ggplot(plot_df, p9.aes(x=x, y=y))

    # Add bars
    plot = plot + p9.geom_col(width=bar_width)

    # Add value labels if requested
    if show_values:
        if fill:
            plot = plot + p9.geom_text(p9.aes(label=y), format_string=value_format, va="bottom", size=8)
        else:
            plot = plot + p9.geom_text(p9.aes(label=y), format_string=value_format, va="bottom", size=8)

    # Colors
    if fill and color_palette:
        if isinstance(color_palette, dict):
            plot = plot + p9.scale_fill_manual(values=color_palette, name=legend_title)
        else:
            plot = plot + p9.scale_fill_manual(values=color_palette[: len(plot_df[fill].unique())], name=legend_title)
    elif fill:
        unique_vals = len(plot_df[fill].unique())
        plot = plot + p9.scale_fill_manual(values=COMPONENT_COLORS[:unique_vals], name=legend_title)
    else:
        plot = plot + p9.geom_col(fill=KOMATSU_COLORS["primary_blue"])

    # Labels and theme
    plot = (
        plot
        + p9.labs(
            title=title,
            subtitle=subtitle,
            x=x_label,
            y=y_label,
            caption=f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}',
        )
        + theme_komatsu(figure_size=figure_size)
    )

    # Adjust x-axis rotation if needed
    if x_axis_rotation != 0:
        plot = plot + p9.theme(axis_text_x=p9.element_text(rotation=x_axis_rotation, ha="right"))

    return plot


# ============================================================================
# STACKED BAR PLOT
# ============================================================================


def komatsu_stacked_bar_plot(
    df,
    x,
    y,
    fill,
    title="",
    subtitle="",
    x_label=None,
    y_label=None,
    legend_title=None,
    color_palette=None,
    figure_size=(14, 7),
    bar_width=0.8,
    x_axis_rotation=0,
    position="stack",  # "stack" or "fill" (for 100% stacked)
):
    """
    Create a stacked bar plot with Komatsu theme

    Args:
        df: DataFrame
        x: Column name for x-axis
        y: Column name for y-axis values
        fill: Column name for stack segments
        position: "stack" for regular stacked, "fill" for 100% stacked
        (other args same as simple bar plot)

    Returns:
        plotnine plot object
    """
    # Convert polars to pandas if needed
    plot_df = df.to_pandas() if hasattr(df, "to_pandas") else df

    # Set defaults
    x_label = x_label or x
    y_label = y_label or y
    legend_title = legend_title or fill

    # Base plot
    plot = p9.ggplot(plot_df, p9.aes(x=x, y=y, fill=fill)) + p9.geom_col(position=position, width=bar_width)

    # Colors
    if color_palette:
        if isinstance(color_palette, dict):
            plot = plot + p9.scale_fill_manual(values=color_palette, name=legend_title)
        else:
            unique_vals = len(plot_df[fill].unique())
            plot = plot + p9.scale_fill_manual(values=color_palette[:unique_vals], name=legend_title)
    else:
        unique_vals = len(plot_df[fill].unique())
        plot = plot + p9.scale_fill_manual(values=COMPONENT_COLORS[:unique_vals], name=legend_title)

    # For 100% stacked, adjust y-axis
    if position == "fill":
        plot = plot + p9.scale_y_continuous(labels=lambda l: [f"{v:.0%}" for v in l])
        y_label = y_label + " (%)" if "%" not in y_label else y_label

    # Labels and theme
    plot = (
        plot
        + p9.labs(
            title=title,
            subtitle=subtitle,
            x=x_label,
            y=y_label,
            caption=f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}',
        )
        + theme_komatsu(figure_size=figure_size)
    )

    # Adjust x-axis rotation if needed
    if x_axis_rotation != 0:
        plot = plot + p9.theme(axis_text_x=p9.element_text(rotation=x_axis_rotation, ha="right"))

    return plot


# ============================================================================
# LINE PLOT
# ============================================================================


def komatsu_line_plot(
    df,
    x,
    y,
    color=None,
    title="",
    subtitle="",
    x_label=None,
    y_label=None,
    legend_title=None,
    color_palette=None,
    figure_size=(14, 7),
    line_size=1.5,
    show_points=True,
    point_size=3,
    smooth=False,
    x_axis_rotation=0,
):
    """
    Create a line plot with Komatsu theme

    Args:
        df: DataFrame
        x: Column name for x-axis
        y: Column name for y-axis values
        color: Column name for line colors (optional)
        line_size: Thickness of lines
        show_points: Whether to show data points
        point_size: Size of data points
        smooth: Add smoothed trend line
        (other args same as bar plot)

    Returns:
        plotnine plot object
    """
    # Convert polars to pandas if needed
    plot_df = df.to_pandas() if hasattr(df, "to_pandas") else df

    # Set defaults
    x_label = x_label or x
    y_label = y_label or y

    # Base plot
    if color:
        plot = p9.ggplot(plot_df, p9.aes(x=x, y=y, color=color, group=color))
        legend_title = legend_title or color
    else:
        plot = p9.ggplot(plot_df, p9.aes(x=x, y=y))

    # Add line
    plot = plot + p9.geom_line(size=line_size)

    # Add points if requested
    if show_points:
        plot = plot + p9.geom_point(size=point_size)

    # Add smooth if requested
    if smooth:
        plot = plot + p9.geom_smooth(method="loess", se=False, size=1, alpha=0.5)

    # Colors
    if color and color_palette:
        if isinstance(color_palette, dict):
            plot = plot + p9.scale_color_manual(values=color_palette, name=legend_title)
        else:
            unique_vals = len(plot_df[color].unique())
            plot = plot + p9.scale_color_manual(values=color_palette[:unique_vals], name=legend_title)
    elif color:
        unique_vals = len(plot_df[color].unique())
        plot = plot + p9.scale_color_manual(values=COMPONENT_COLORS[:unique_vals], name=legend_title)
    else:
        plot = plot + p9.geom_line(color=KOMATSU_COLORS["primary_blue"])
        if show_points:
            plot = plot + p9.geom_point(color=KOMATSU_COLORS["primary_blue"])

    # Labels and theme
    plot = (
        plot
        + p9.labs(
            title=title,
            subtitle=subtitle,
            x=x_label,
            y=y_label,
            caption=f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}',
        )
        + theme_komatsu(figure_size=figure_size)
    )

    # Adjust x-axis rotation if needed
    if x_axis_rotation != 0:
        plot = plot + p9.theme(axis_text_x=p9.element_text(rotation=x_axis_rotation, ha="right"))

    return plot


# ============================================================================
# HORIZONTAL BAR PLOT
# ============================================================================


def komatsu_horizontal_bar_plot(
    df,
    x,
    y,
    fill=None,
    title="",
    subtitle="",
    x_label=None,
    y_label=None,
    legend_title=None,
    color_palette=None,
    figure_size=(14, 7),
    bar_width=0.8,
    show_values=False,
    value_format="{:.0f}",
):
    """
    Create a horizontal bar plot with Komatsu theme

    Args:
        Same as komatsu_bar_plot but creates horizontal bars

    Returns:
        plotnine plot object
    """
    # Use regular bar plot and flip coordinates
    plot = komatsu_bar_plot(
        df=df,
        x=x,
        y=y,
        fill=fill,
        title=title,
        subtitle=subtitle,
        x_label=x_label,
        y_label=y_label,
        legend_title=legend_title,
        color_palette=color_palette,
        figure_size=figure_size,
        bar_width=bar_width,
        show_values=show_values,
        value_format=value_format,
        x_axis_rotation=0,
    )

    # Flip coordinates
    plot = plot + p9.coord_flip()

    return plot


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Create sample data
    import numpy as np

    # Sample data 1: Simple bar chart
    df_simple = pl.DataFrame(
        {
            "component": ["Engine", "Transmission", "Final Drive", "Hydraulic Pump", "Alternator"],
            "failures": [45, 32, 28, 19, 12],
        }
    )

    # Sample data 2: Grouped/stacked data
    df_grouped = pl.DataFrame(
        {
            "month": ["Jan", "Feb", "Mar", "Apr", "May", "Jun"] * 3,
            "warranty_type": ["Standard"] * 6 + ["Extended"] * 6 + ["Goodwill"] * 6,
            "count": np.random.randint(10, 50, 18),
        }
    )

    # Sample data 3: Time series
    dates = pl.date_range(start=pl.date(2024, 1, 1), end=pl.date(2024, 12, 31), interval="1mo", eager=True)
    df_time = pl.DataFrame(
        {"date": dates, "availability": np.random.uniform(85, 95, len(dates)), "target": [90] * len(dates)}
    )

    # Example plots

    # 1. Simple bar chart
    plot1 = komatsu_bar_plot(
        df_simple,
        x="component",
        y="failures",
        title="Component Failures - Q4 2024",
        subtitle="Total failures by component type",
        y_label="Number of Failures",
        show_values=True,
    )

    # 2. Stacked bar chart
    plot2 = komatsu_stacked_bar_plot(
        df_grouped,
        x="month",
        y="count",
        fill="warranty_type",
        title="Monthly Warranty Distribution",
        subtitle="Warranty claims by type",
        y_label="Number of Claims",
    )

    # 3. Line plot
    plot3 = komatsu_line_plot(
        df_time,
        x="date",
        y="availability",
        title="Equipment Availability Trend",
        subtitle="Monthly availability percentage",
        y_label="Availability (%)",
        show_points=True,
    )

    # 4. Horizontal bar chart
    plot4 = komatsu_horizontal_bar_plot(
        df_simple,
        x="component",
        y="failures",
        title="Component Failures - Ranked",
        subtitle="Failures by component (horizontal view)",
        x_label="Number of Failures",
        show_values=True,
    )
