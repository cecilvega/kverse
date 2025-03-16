"""
GGPrism.py - A matplotlib styling module based on ggprism

This module provides tools to style matplotlib plots in a way that mimics
the ggprism package from R, specifically using the winter_bright palette.

Usage:
    from GGPrism import GGPrism

    # Create a new figure with ggprism theme
    theme = GGPrism()
    fig, ax = theme.create_figure(figsize=(10, 6))

    # Or apply to existing figure/axis
    fig, ax = plt.subplots()
    theme.apply_theme(ax)
"""

import matplotlib.pyplot as plt
import matplotlib as mpl
from matplotlib.ticker import MaxNLocator
import numpy as np
import pandas as pd
from typing import Union, List, Tuple, Dict, Optional, Any


class GGPrism:
    """
    A class that implements ggprism styling for matplotlib using the winter_bright palette
    """

    # Winter bright color palette
    COLORS = [
        "#077E97",  # blue
        "#800080",  # purple
        "#000080",  # navy
        "#8D8DFF",  # light blue
        "#C000C0",  # magenta
        "#056943",  # dark green
        "#077E97",  # blue (repeat)
        "#800080",  # purple (repeat)
        "#000080",  # navy (repeat)
    ]

    # Fixed colors from ggprism
    FIXED_COLORS = {
        "axis_color": "#000080",  # Navy blue - exact ggprism axisColor
        "axis_label_color": "#000080",  # Navy blue
        "axis_title_color": "#000080",  # Navy blue
        "plot_title_color": "#000080",  # Navy blue
        "page_background": "#FFFFFF",  # White
        "plotting_area": "#FFFFFF",  # White
    }

    def __init__(self, base_size: int = 14, base_family: str = "sans", base_fontface: str = "bold"):
        """
        Initialize the theme with base parameters

        Parameters:
            base_size (int): Base font size in points
            base_family (str): Base font family
            base_fontface (str): Default font weight/style
        """
        self.base_size = base_size
        self.base_family = base_family
        self.base_fontface = base_fontface

        # Set default line and rect sizes based on base_size
        self.base_line_size = base_size / 14
        self.base_rect_size = base_size / 14

        # Derived font sizes following ggprism conventions
        self.title_size = self.base_size * 1.2
        self.axis_title_size = self.base_size
        self.axis_text_size = self.base_size * 0.95
        self.legend_title_size = self.base_size
        self.legend_text_size = self.base_size * 0.8

        # Other style parameters
        self.tick_length = self.base_size / 2
        self.tick_width = self.base_line_size

    def create_figure(self, figsize: Tuple[float, float] = None) -> Tuple[plt.Figure, plt.Axes]:
        """
        Create a new figure and axis with the theme applied

        Parameters:
            figsize (tuple): Figure size (width, height) in inches

        Returns:
            tuple: (fig, ax) Matplotlib figure and axis objects
        """
        if figsize is None:
            # Default to golden ratio
            figsize = (self.base_size * 0.8, self.base_size * 0.5)

        # Set up rcParams
        plt.rcParams["font.family"] = self.base_family
        plt.rcParams["font.sans-serif"] = ["Arial", "DejaVu Sans"]
        plt.rcParams["font.weight"] = self.base_fontface
        plt.rcParams["font.size"] = self.base_size

        # Create figure and apply theme
        fig, ax = plt.subplots(figsize=figsize)
        self.apply_theme(ax)

        return fig, ax

    def apply_theme(self, ax: plt.Axes) -> plt.Axes:
        """
        Apply the theme to an existing matplotlib axis

        Parameters:
            ax (plt.Axes): Matplotlib axis object

        Returns:
            plt.Axes: The modified axis object
        """
        fig = ax.figure

        # Set colors
        axis_color = self.FIXED_COLORS["axis_color"]

        # Set background colors
        fig.patch.set_facecolor(self.FIXED_COLORS["page_background"])
        ax.set_facecolor(self.FIXED_COLORS["plotting_area"])

        # Remove grid lines
        ax.grid(False)

        # Style spines
        for spine in ax.spines.values():
            spine.set_color(axis_color)
            spine.set_linewidth(self.base_line_size)

        # Style ticks
        ax.tick_params(width=self.tick_width, length=self.tick_length, colors=axis_color, labelsize=self.axis_text_size)

        # Style axis labels
        ax.xaxis.label.set_color(axis_color)
        ax.xaxis.label.set_fontsize(self.axis_title_size)
        ax.xaxis.label.set_fontweight(self.base_fontface)

        ax.yaxis.label.set_color(axis_color)
        ax.yaxis.label.set_fontsize(self.axis_title_size)
        ax.yaxis.label.set_fontweight(self.base_fontface)

        # Style y-axis to show integer ticks if appropriate
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))

        return ax

    def style_title(self, ax: plt.Axes, title: str, pad: int = 28) -> None:
        """
        Add a properly styled title to the plot

        Parameters:
            ax (plt.Axes): Matplotlib axis object
            title (str): Title text
            pad (int): Padding between title and plot
        """
        ax.set_title(
            title,
            fontsize=self.title_size,
            fontweight=self.base_fontface,
            color=self.FIXED_COLORS["plot_title_color"],
            pad=pad,
        )

    def style_legend(self, ax: plt.Axes, title: str = None, loc: str = "upper right") -> None:
        """
        Style the legend according to ggprism standards

        Parameters:
            ax (plt.Axes): Matplotlib axis object
            title (str): Legend title
            loc (str): Legend location
        """
        if title is None:
            legend = ax.legend(frameon=True, loc=loc, fontsize=self.legend_text_size, framealpha=1.0)
        else:
            legend = ax.legend(
                title=title,
                frameon=True,
                loc=loc,
                title_fontsize=self.legend_title_size,
                fontsize=self.legend_text_size,
                framealpha=1.0,
            )
            legend.get_title().set_fontweight(self.base_fontface)
            legend.get_title().set_color(self.FIXED_COLORS["axis_title_color"])

        legend.get_frame().set_linewidth(self.base_line_size)
        legend.get_frame().set_edgecolor(self.FIXED_COLORS["axis_color"])

        # Make legend text regular weight (not bold)
        for text in legend.get_texts():
            text.set_color(self.FIXED_COLORS["axis_color"])
            text.set_fontweight("normal")

    def bar_plot(
        self,
        ax: plt.Axes,
        data: pd.DataFrame,
        x: str,
        y: Union[str, List[str]],
        color: str = None,
        stacked: bool = False,
        width: float = 0.85,
        **kwargs
    ) -> plt.Axes:
        """
        Create a bar plot with ggprism styling

        Parameters:
            ax (plt.Axes): Matplotlib axis object
            data (pd.DataFrame): Data to plot
            x (str): Column name for x-axis
            y (str or list): Column name(s) for y-axis
            color (str): Bar color, defaults to first palette color
            stacked (bool): Whether to create stacked bars
            width (float): Bar width
            **kwargs: Additional arguments for plt.bar

        Returns:
            plt.Axes: The modified axis object
        """
        # Apply theme first
        self.apply_theme(ax)

        # Handle single y column or multiple columns
        if isinstance(y, str):
            # Single column bar plot
            if color is None:
                color = self.COLORS[0]

            ax.bar(data[x], data[y], width=width, color=color, edgecolor="white", linewidth=0.8, **kwargs)
        elif isinstance(y, list) and stacked:
            # Stacked bar plot with multiple columns
            bottom = np.zeros(len(data))

            for i, col in enumerate(y):
                color = self.COLORS[i % len(self.COLORS)] if color is None else color

                ax.bar(
                    data[x],
                    data[col],
                    width=width,
                    bottom=bottom,
                    color=self.COLORS[i % len(self.COLORS)],
                    edgecolor="white",
                    linewidth=0.8,
                    label=col,
                    **kwargs
                )

                bottom += data[col].values

            self.style_legend(ax)
        else:
            # Grouped bar plot
            x_pos = np.arange(len(data))
            bar_width = width / len(y)

            for i, col in enumerate(y):
                offset = (i - len(y) / 2 + 0.5) * bar_width

                ax.bar(
                    x_pos + offset,
                    data[col],
                    width=bar_width,
                    color=self.COLORS[i % len(self.COLORS)],
                    edgecolor="white",
                    linewidth=0.8,
                    label=col,
                    **kwargs
                )

            ax.set_xticks(x_pos)
            ax.set_xticklabels(data[x])
            self.style_legend(ax)

        return ax

    def line_plot(
        self,
        ax: plt.Axes,
        data: pd.DataFrame,
        x: str,
        y: Union[str, List[str]],
        color: str = None,
        marker: str = "o",
        markersize: float = 5,
        linewidth: float = 1.5,
        **kwargs
    ) -> plt.Axes:
        """
        Create a line plot with ggprism styling

        Parameters:
            ax (plt.Axes): Matplotlib axis object
            data (pd.DataFrame): Data to plot
            x (str): Column name for x-axis
            y (str or list): Column name(s) for y-axis
            color (str): Line color, defaults to palette colors
            marker (str): Marker style
            markersize (float): Marker size
            linewidth (float): Line width
            **kwargs: Additional arguments for plt.plot

        Returns:
            plt.Axes: The modified axis object
        """
        # Apply theme first
        self.apply_theme(ax)

        # Handle single y column or multiple columns
        if isinstance(y, str):
            # Single column line plot
            if color is None:
                color = self.COLORS[0]

            ax.plot(data[x], data[y], color=color, marker=marker, markersize=markersize, linewidth=linewidth, **kwargs)
        else:
            # Multiple line plot
            for i, col in enumerate(y):
                ax.plot(
                    data[x],
                    data[col],
                    color=self.COLORS[i % len(self.COLORS)] if color is None else color,
                    marker=marker,
                    markersize=markersize,
                    linewidth=linewidth,
                    label=col,
                    **kwargs
                )

            self.style_legend(ax)

        return ax

    def finalize_plot(
        self,
        fig: plt.Figure,
        ax: plt.Axes,
        title: str = None,
        xlabel: str = None,
        ylabel: str = None,
        legend_title: str = None,
        xlabelpad: int = 15,
        ylabelpad: int = 15,
        tight_layout: bool = True,
    ) -> None:
        """
        Finalize a plot with proper styling and labels

        Parameters:
            fig (plt.Figure): Matplotlib figure
            ax (plt.Axes): Matplotlib axis
            title (str): Plot title
            xlabel (str): X-axis label
            ylabel (str): Y-axis label
            legend_title (str): Legend title
            xlabelpad (int): X-axis label padding
            ylabelpad (int): Y-axis label padding
            tight_layout (bool): Whether to apply tight layout
        """
        # Apply axis labels if provided
        if xlabel is not None:
            ax.set_xlabel(
                xlabel,
                fontsize=self.axis_title_size,
                fontweight=self.base_fontface,
                color=self.FIXED_COLORS["axis_title_color"],
                labelpad=xlabelpad,
            )

        if ylabel is not None:
            ax.set_ylabel(
                ylabel,
                fontsize=self.axis_title_size,
                fontweight=self.base_fontface,
                color=self.FIXED_COLORS["axis_title_color"],
                labelpad=ylabelpad,
            )

        # Apply title if provided
        if title is not None:
            self.style_title(ax, title)

        # Style legend if it exists
        if ax.get_legend() is not None:
            self.style_legend(ax, title=legend_title)

        # Apply tight layout
        if tight_layout:
            plt.tight_layout(rect=[0, 0, 1, 0.97])  # Match ggprism margins

    def save_plot(self, fig: plt.Figure, filename: str, dpi: int = 300, bbox_inches: str = "tight") -> None:
        """
        Save the plot with high quality settings

        Parameters:
            fig (plt.Figure): Matplotlib figure
            filename (str): Output filename
            dpi (int): Resolution in dots per inch
            bbox_inches (str): Bounding box setting
        """
        fig.savefig(filename, dpi=dpi, bbox_inches=bbox_inches, facecolor=fig.get_facecolor())
