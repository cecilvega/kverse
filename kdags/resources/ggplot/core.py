"""
core.py - Core GGPrism theme class

This module provides the GGPrism class which implements matplotlib styling
based on the ggprism approach from R.
"""

import matplotlib.pyplot as plt
import matplotlib as mpl
from matplotlib.ticker import MaxNLocator
from typing import Tuple, Optional, Dict, Any


class GGPrism:
    """
    A class that implements ggprism styling for matplotlib

    The GGPrism theme provides a consistent and visually appealing styling
    for matplotlib plots, inspired by the ggprism package from R.
    """

    # Color palettes
    PALETTES = {
        "winter_bright": [
            "#077E97",  # blue
            "#800080",  # purple
            "#000080",  # navy
            "#8D8DFF",  # light blue
            "#C000C0",  # magenta
            "#056943",  # dark green
            "#077E97",  # blue (repeat)
            "#800080",  # purple (repeat)
            "#000080",  # navy (repeat)
        ],
        "winter_soft": [
            "#4D8FAC",  # soft blue
            "#9370DB",  # medium purple
            "#1E90FF",  # dodger blue
            "#87CEEB",  # sky blue
            "#BA55D3",  # medium orchid
            "#3CB371",  # medium sea green
            "#4D8FAC",  # soft blue (repeat)
            "#9370DB",  # medium purple (repeat)
            "#1E90FF",  # dodger blue (repeat)
        ],
    }

    # Default color palette
    COLORS = PALETTES["winter_bright"]

    # Fixed colors from ggprism
    FIXED_COLORS = {
        "axis_color": "#000080",  # Navy blue - exact ggprism axisColor
        "axis_label_color": "#000080",  # Navy blue
        "axis_title_color": "#000080",  # Navy blue
        "plot_title_color": "#000080",  # Navy blue
        "page_background": "#FFFFFF",  # White
        "plotting_area": "#FFFFFF",  # White
    }

    def __init__(
        self,
        base_size: int = 14,
        base_family: str = "sans",
        base_fontface: str = "bold",
        palette: str = "winter_bright",
    ):
        """
        Initialize the theme with base parameters

        Parameters:
            base_size (int): Base font size in points
            base_family (str): Base font family
            base_fontface (str): Default font weight/style
            palette (str): Color palette to use ("winter_bright" or "winter_soft")
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

        # Set color palette
        if palette in self.PALETTES:
            self.COLORS = self.PALETTES[palette]
        else:
            self.COLORS = self.PALETTES["winter_bright"]

    def get_palette(self, palette_name: str = None) -> list:
        """
        Get a color palette by name or the current default palette

        Parameters:
            palette_name (str, optional): Name of the palette to get

        Returns:
            list: The requested color palette
        """
        if palette_name and palette_name in self.PALETTES:
            return self.PALETTES[palette_name]
        return self.COLORS

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

        if legend:
            legend.get_frame().set_linewidth(self.base_line_size)
            legend.get_frame().set_edgecolor(self.FIXED_COLORS["axis_color"])

            # Make legend text regular weight (not bold)
            for text in legend.get_texts():
                text.set_color(self.FIXED_COLORS["axis_color"])
                text.set_fontweight("normal")

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


# """
# core.py - Core GGPrism theme class
#
# This module provides the GGPrism class which implements matplotlib styling
# based on the ggprism approach from R.
# """
#
# import matplotlib.pyplot as plt
# import matplotlib as mpl
# from matplotlib.ticker import MaxNLocator
# from typing import Tuple, Optional, Dict, Any
#
# """
# ggprism.py - Minimal matplotlib styling with winter bright palette
# """
#
# import matplotlib.pyplot as plt
#
# # Winter bright palette colors
# COLORS = [
#     "#077E97",  # blue
#     "#800080",  # purple
#     "#000080",  # navy
#     "#8D8DFF",  # light blue
#     "#C000C0",  # magenta
#     "#056943",  # dark green
# ]
#
# # Fixed colors
# AXIS_COLOR = "#000080"  # Navy blue
#
#
# def apply_theme(ax):
#     """Apply the winter bright theme to a matplotlib axis"""
#     # Set axis colors
#     ax.spines["bottom"].set_color(AXIS_COLOR)
#     ax.spines["top"].set_color(AXIS_COLOR)
#     ax.spines["left"].set_color(AXIS_COLOR)
#     ax.spines["right"].set_color(AXIS_COLOR)
#
#     # Set tick colors
#     ax.tick_params(axis="x", colors=AXIS_COLOR)
#     ax.tick_params(axis="y", colors=AXIS_COLOR)
#
#     # Set label colors
#     ax.xaxis.label.set_color(AXIS_COLOR)
#     ax.yaxis.label.set_color(AXIS_COLOR)
#     ax.title.set_color(AXIS_COLOR)
#
#     # Remove grid
#     ax.grid(False)
#
#     return ax
#
#
# def create_figure(figsize=(10, 6)):
#     """Create a figure with the theme applied"""
#     fig, ax = plt.subplots(figsize=figsize)
#     apply_theme(ax)
#     return fig, ax
#
#
# def style_legend(ax, title=None, loc="best"):
#     """Style the legend with winter bright theme"""
#     legend = ax.legend(title=title, loc=loc, frameon=True)
#     if legend:
#         legend.get_frame().set_edgecolor(AXIS_COLOR)
#         if title:
#             legend.get_title().set_color(AXIS_COLOR)
