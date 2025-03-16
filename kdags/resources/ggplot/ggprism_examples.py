"""
ggprism_theme_examples.py - Examples of using the GGPrismTheme class

This script demonstrates different ways to use the GGPrismTheme class
to create consistently styled plots.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from kdags.resources.ggplot.ggprism import GGPrism


# -----------------------------------------
# Example 1: Simple Bar Plot
# -----------------------------------------
def example_bar_plot():
    # Create sample data
    data = pd.DataFrame({"category": ["A", "B", "C", "D", "E"], "value": [25, 18, 30, 15, 22]})

    # Create theme and figure
    theme = GGPrism()
    fig, ax = theme.create_figure(figsize=(8, 5))

    # Create bar plot
    theme.bar_plot(ax, data, "category", "value")

    # Finalize and show
    theme.finalize_plot(fig, ax, title="Simple Bar Plot", xlabel="Category", ylabel="Value")

    theme.save_plot(fig, "example_bar_plot.png")

    return fig, ax


# -----------------------------------------
# Example 2: Stacked Bar Plot (like the ICC report)
# -----------------------------------------
def example_stacked_bar_plot():
    # Create sample data similar to the ICC report
    weeks = [f"2024-W{i:02d}" for i in range(1, 11)]

    # Generate some random data
    np.random.seed(42)
    total = np.random.randint(3, 7, size=len(weeks))
    missing = np.random.randint(0, 3, size=len(weeks))
    missing = np.minimum(missing, total)  # Make sure missing <= total
    available = total - missing

    # Calculate missing percentage
    missing_percentage = (missing / total * 100).round().astype(int)

    data = pd.DataFrame(
        {
            "week": weeks,
            "total": total,
            "missing": missing,
            "available": available,
            "missing_percentage": missing_percentage,
        }
    )

    # Create theme and figure
    theme = GGPrism(palette="winter_bright")
    fig, ax = theme.create_figure(figsize=(12, 7))

    # Plot stacked bars
    x = np.arange(len(data))
    ax.bar(
        x,
        data["available"],
        width=0.85,
        color=theme.PALETTES["winter_bright"][0],  # First color from palette
        label="Available",
        edgecolor="white",
        linewidth=0.8,
    )
    ax.bar(
        x,
        data["missing"],
        width=0.85,
        bottom=data["available"],
        color=theme.PALETTES["winter_bright"][4],  # Fifth color from palette
        label="Missing",
        edgecolor="white",
        linewidth=0.8,
    )

    # Add percentage labels
    for i, row in enumerate(data.itertuples()):
        if row.missing > 0:
            ax.text(
                i,
                row.total + 0.2,
                f"{row.missing_percentage}%",
                ha="center",
                va="bottom",
                fontsize=14,
                fontweight="bold",
                color=theme.FIXED_COLORS["axis_color"],
            )
        else:
            ax.text(
                i,
                row.total + 0.2,
                "0%",
                ha="center",
                va="bottom",
                fontsize=14,
                fontweight="bold",
                color=theme.FIXED_COLORS["axis_color"],
            )

    # Set x-ticks
    ax.set_xticks(x)
    ax.set_xticklabels(data["week"], rotation=90)

    # Finalize plot
    theme.finalize_plot(
        fig, ax, title="Weekly Reports Status", xlabel="Week", ylabel="Number of Reports", legend_title="Report Status"
    )

    theme.save_plot(fig, "example_stacked_bar.png")

    return fig, ax


# -----------------------------------------
# Example 3: Line Plot with Multiple Series
# -----------------------------------------
def example_line_plot():
    # Create sample data
    x = np.linspace(0, 10, 20)
    df = pd.DataFrame({"x": x, "y1": np.sin(x), "y2": np.cos(x), "y3": np.sin(x) * np.cos(x)})

    # Create theme and figure
    theme = GGPrism(palette="winter_soft")
    fig, ax = theme.create_figure(figsize=(10, 6))

    # Create line plot
    theme.line_plot(ax, df, "x", ["y1", "y2", "y3"])

    # Finalize and show
    theme.finalize_plot(
        fig, ax, title="Multiple Series Line Plot", xlabel="X Value", ylabel="Y Value", legend_title="Series"
    )

    theme.save_plot(fig, "example_line_plot.png")

    return fig, ax


# -----------------------------------------
# Example 4: Reproducing the ICC Reports Plot
# -----------------------------------------
def reproduce_icc_report():
    # In a real scenario, you would read this from a file
    # Here we'll generate sample data similar to the ICC report

    # Sample data: This would normally come from your data pipeline
    df = pd.DataFrame(
        {
            "week": [f"2024-W{i:02d}" for i in range(1, 21)],
            "total": np.random.randint(3, 6, 20),
            "missing_percentage": np.random.choice([0, 0, 0, 16, 33, 50, 75, 100], 20),
        }
    )

    # Calculate missing and available
    df["missing"] = (df["total"] * df["missing_percentage"] / 100).round().astype(int)
    df["available"] = df["total"] - df["missing"]

    # Create theme
    theme = GGPrism()

    # Create figure with appropriate size
    fig, ax = theme.create_figure(figsize=(12, 7))

    # Plot stacked bars
    x = np.arange(len(df))
    ax.bar(
        x,
        df["available"],
        width=0.85,
        color=theme.get_palette("winter_bright")[0],  # Use theme colors
        label="Available",
        edgecolor="white",
        linewidth=0.8,
    )
    ax.bar(
        x,
        df["missing"],
        width=0.85,
        bottom=df["available"],
        color=theme.get_palette("winter_bright")[4],  # Use theme colors
        label="Missing",
        edgecolor="white",
        linewidth=0.8,
    )

    # Add percentage labels
    for i, row in enumerate(df.itertuples()):
        if row.missing > 0:
            ax.text(
                i,
                row.total + 0.2,
                f"{int(row.missing_percentage)}%",
                ha="center",
                va="bottom",
                fontsize=14,
                fontweight="bold",
                color=theme.FIXED_COLORS["axis_color"],
            )
        else:
            ax.text(
                i,
                row.total + 0.2,
                "0%",
                ha="center",
                va="bottom",
                fontsize=14,
                fontweight="bold",
                color=theme.FIXED_COLORS["axis_color"],
            )

    # Set x-ticks
    ax.set_xticks(x)
    ax.set_xticklabels(df["week"], rotation=90)

    # Apply the styling through the theme
    theme.style_legend(ax, title="Report Status", loc="upper right")

    # Finalize plot with labels and title
    theme.finalize_plot(fig, ax, title="Weekly ICC Reports Status", xlabel="Week", ylabel="Number of Reports")

    # Save the plot
    theme.save_plot(fig, "weekly_icc_reports_themed.png")

    return fig, ax


# Run all examples
if __name__ == "__main__":
    print("Running Example 1: Simple Bar Plot")
    example_bar_plot()

    print("Running Example 2: Stacked Bar Plot")
    example_stacked_bar_plot()

    print("Running Example 3: Line Plot")
    example_line_plot()

    print("Running Example 4: ICC Report Plot")
    reproduce_icc_report()

    # Show all plots
    plt.show()
