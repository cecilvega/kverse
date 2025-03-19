from .ggprism import GGPrism
import polars as pl
import matplotlib.pyplot as plt


def ggplot_time_series(
    df: pl.DataFrame,
    equipment_name: str,
    parameter_codes: list[str],
    component_code: str = None,
    position_code: int = None,
    date_range: tuple = None,
    figsize: (int, int) = (12, 7),
    xlabel: str = "Sample Date",
    ylabel: str = "Parameter Value",
    legend_title: str = "Parameters",
    title: str = None,
    ylim: (float, float) = None,
    annotation_interval: int = 3,  # Annotate every nth point
    date_column_name: str = "record_date",
):
    """
    Create a time series plot for oil analysis data using Polars DataFrame.

    Args:
        df: Polars DataFrame with oil analysis data
        equipment_name: Equipment name to filter for
        parameter_codes: List of parameter codes to plot (e.g., ["fe", "cu"])
        component_code: Optional component code to filter for
        position_code: Optional position code to filter for
        date_range: Optional tuple of (start_date, end_date) to filter data
        figsize: Figure size as (width, height) in inches
        xlabel: X-axis label
        ylabel: Y-axis label
        legend_title: Title for the legend
        title: Plot title (if None, will generate based on equipment_name)
        ylim: Optional tuple for y-axis limits (min, max)
        annotation_interval: Interval for adding value annotations (None to disable)
        date_column_name: Name of the date column to use for x-axis

    Returns:
        tuple: (fig, ax) Matplotlib figure and axes objects
    """
    # Apply filters
    filtered_df = df.filter(pl.col("equipment_name") == equipment_name)

    if component_code is not None:
        filtered_df = filtered_df.filter(pl.col("component_code") == component_code)

    if position_code is not None:
        filtered_df = filtered_df.filter(pl.col("position_code") == position_code)

    if date_range is not None:
        start_date, end_date = date_range
        filtered_df = filtered_df.filter(
            (pl.col(date_column_name) >= start_date) & (pl.col(date_column_name) <= end_date)
        )

    # Check if we have data after filtering
    if filtered_df.height == 0:
        raise ValueError(f"No data found after applying filters for equipment {equipment_name}")

    # Convert to pandas for plotting with GGPrism
    # Note: This assumes a melted DataFrame with parameter_code and parameter_value columns
    plot_data = filtered_df.filter(pl.col("parameter_code").is_in(parameter_codes)).sort(date_column_name).to_pandas()

    # Pivot to wide format for plotting
    plot_data_wide = plot_data.pivot(
        index=date_column_name, columns="parameter_code", values="parameter_value"
    ).reset_index()

    # Create a GGPrism plot
    theme = GGPrism()
    fig, ax = theme.create_figure(figsize=figsize)

    # Create the line plot with multiple series using GGPrism's line_plot method
    theme.line_plot(
        ax,
        plot_data_wide,
        x=date_column_name,
        y=parameter_codes,
        marker="o",
        markersize=6,
        linewidth=2,
    )

    # Position the legend on the right side outside the plot
    ax.legend(title=legend_title, loc="center left", bbox_to_anchor=(1.02, 0.5))

    # Add annotations at regular intervals
    if annotation_interval:
        for i, param in enumerate(parameter_codes):
            param_data = plot_data[plot_data["parameter_code"] == param]

            # Select points to annotate based on interval
            points_to_annotate = param_data.iloc[::annotation_interval]

            for _, row in points_to_annotate.iterrows():
                ax.annotate(
                    f"{row['parameter_value']:.1f}",
                    xy=(row[date_column_name], row["parameter_value"]),
                    xytext=(0, 10),  # Small offset above the point
                    textcoords="offset points",
                    ha="center",
                    fontsize=8,
                    color=theme.COLORS[i % len(theme.COLORS)],
                )

    # Apply y-axis limits if provided
    if ylim is not None:
        ax.set_ylim(ylim)

    # Generate title if not provided
    if title is None:
        comp_str = f", {component_code}" if component_code else ""
        pos_str = f", Position {position_code}" if position_code else ""
        title = f"Parameter Trends for {equipment_name}{comp_str}{pos_str}"

    # Finalize the plot with proper styling
    theme.finalize_plot(
        fig,
        ax,
        title=title,
        xlabel=xlabel,
        ylabel=ylabel,
        # Legend is now positioned manually, so we don't need to set it here
    )

    # Adjust layout to make room for the legend
    plt.tight_layout(rect=[0, 0, 0.85, 1])  # Adjust the rect to leave space for the legend

    return fig, ax
