"""
basic_plots.py - Basic plot functions for GGPrism theme

This module provides functions for creating common plot types
using the GGPrism theme and Polars dataframes.
"""

import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from typing import Union, List, Tuple, Optional, Dict, Any

from .core import GGPrism


def bar_plot(
    theme: GGPrism,
    df: pl.DataFrame,
    x: str,
    y: Union[str, List[str]],
    ax: Optional[plt.Axes] = None,
    figsize: Tuple[float, float] = (10, 6),
    color: Optional[str] = None,
    stacked: bool = False,
    width: float = 0.85,
    **kwargs
) -> Tuple[plt.Figure, plt.Axes]:
    """
    Create a bar plot with GGPrism styling using Polars DataFrame

    Parameters:
        theme (GGPrism): GGPrism theme instance
        df (pl.DataFrame): Polars DataFrame with data to plot
        x (str): Column name for x-axis categories
        y (str or list): Column name(s) for y-axis values
        ax (plt.Axes, optional): Existing axes to plot on
        figsize (tuple): Figure size as (width, height) in inches
        color (str, optional): Bar color, defaults to first theme color
        stacked (bool): Whether to create stacked bars for multiple y columns
        width (float): Bar width
        **kwargs: Additional arguments for plt.bar

    Returns:
        tuple: (fig, ax) Matplotlib figure and axes objects
    """
    # Create figure and axes if not provided
    if ax is None:
        fig, ax = theme.create_figure(figsize)
    else:
        fig = ax.figure
        theme.apply_theme(ax)

    # Convert polars dataframe to numpy arrays for plotting
    x_data = df.select(x).to_numpy().flatten()

    # Handle single y column
    if isinstance(y, str):
        y_data = df.select(y).to_numpy().flatten()

        # Set default color if not provided
        if color is None:
            color = theme.COLORS[0]

        ax.bar(x_data, y_data, width=width, color=color, edgecolor="white", linewidth=0.8, **kwargs)

    # Handle multiple y columns with stacking
    elif isinstance(y, list) and stacked:
        bottom = np.zeros(len(df))

        for i, col in enumerate(y):
            y_data = df.select(col).to_numpy().flatten()
            bar_color = theme.COLORS[i % len(theme.COLORS)] if color is None else color

            ax.bar(
                x_data,
                y_data,
                width=width,
                bottom=bottom,
                color=bar_color,
                edgecolor="white",
                linewidth=0.8,
                label=col,
                **kwargs
            )

            bottom += y_data

    # Handle multiple y columns without stacking (grouped)
    elif isinstance(y, list):
        x_pos = np.arange(len(df))
        bar_width = width / len(y)

        # Get category labels for x-axis
        x_labels = df.select(x).to_numpy().flatten()

        for i, col in enumerate(y):
            y_data = df.select(col).to_numpy().flatten()
            offset = (i - len(y) / 2 + 0.5) * bar_width
            bar_color = theme.COLORS[i % len(theme.COLORS)] if color is None else color

            ax.bar(
                x_pos + offset,
                y_data,
                width=bar_width,
                color=bar_color,
                edgecolor="white",
                linewidth=0.8,
                label=col,
                **kwargs
            )

        ax.set_xticks(x_pos)
        ax.set_xticklabels(x_labels)

    return fig, ax


def line_plot(
    theme: GGPrism,
    df: pl.DataFrame,
    x: str,
    y: Union[str, List[str]],
    ax: Optional[plt.Axes] = None,
    figsize: Tuple[float, float] = (10, 6),
    color: Optional[str] = None,
    marker: str = "o",
    markersize: float = 5,
    linewidth: float = 1.5,
    **kwargs
) -> Tuple[plt.Figure, plt.Axes]:
    """
    Create a line plot with GGPrism styling using Polars DataFrame

    Parameters:
        theme (GGPrism): GGPrism theme instance
        df (pl.DataFrame): Polars DataFrame with data to plot
        x (str): Column name for x-axis
        y (str or list): Column name(s) for y-axis
        ax (plt.Axes, optional): Existing axes to plot on
        figsize (tuple): Figure size as (width, height) in inches
        color (str, optional): Line color, defaults to theme colors
        marker (str): Marker style
        markersize (float): Marker size
        linewidth (float): Line width
        **kwargs: Additional arguments for plt.plot

    Returns:
        tuple: (fig, ax) Matplotlib figure and axes objects
    """
    # Create figure and axes if not provided
    if ax is None:
        fig, ax = theme.create_figure(figsize)
    else:
        fig = ax.figure
        theme.apply_theme(ax)

    # Get x data
    x_data = df.select(x).to_numpy().flatten()

    # Handle single y column
    if isinstance(y, str):
        y_data = df.select(y).to_numpy().flatten()

        # Set default color if not provided
        if color is None:
            color = theme.COLORS[0]

        ax.plot(
            x_data, y_data, color=color, marker=marker, markersize=markersize, linewidth=linewidth, label=y, **kwargs
        )

    # Handle multiple y columns
    elif isinstance(y, list):
        for i, col in enumerate(y):
            y_data = df.select(col).to_numpy().flatten()
            line_color = theme.COLORS[i % len(theme.COLORS)] if color is None else color

            ax.plot(
                x_data,
                y_data,
                color=line_color,
                marker=marker,
                markersize=markersize,
                linewidth=linewidth,
                label=col,
                **kwargs
            )

    return fig, ax


def scatter_plot(
    theme: GGPrism,
    df: pl.DataFrame,
    x: str,
    y: str,
    category: Optional[str] = None,
    ax: Optional[plt.Axes] = None,
    figsize: Tuple[float, float] = (10, 6),
    color: Optional[str] = None,
    s: float = 50,
    alpha: float = 0.7,
    **kwargs
) -> Tuple[plt.Figure, plt.Axes]:
    """
    Create a scatter plot with GGPrism styling using Polars DataFrame

    Parameters:
        theme (GGPrism): GGPrism theme instance
        df (pl.DataFrame): Polars DataFrame with data to plot
        x (str): Column name for x-axis
        y (str): Column name for y-axis
        category (str, optional): Column name for point categories
        ax (plt.Axes, optional): Existing axes to plot on
        figsize (tuple): Figure size as (width, height) in inches
        color (str, optional): Point color, defaults to first theme color
        s (float): Point size
        alpha (float): Point transparency
        **kwargs: Additional arguments for plt.scatter

    Returns:
        tuple: (fig, ax) Matplotlib figure and axes objects
    """
    # Create figure and axes if not provided
    if ax is None:
        fig, ax = theme.create_figure(figsize)
    else:
        fig = ax.figure
        theme.apply_theme(ax)

    # Get data as numpy arrays
    x_data = df.select(x).to_numpy().flatten()
    y_data = df.select(y).to_numpy().flatten()

    # Set default color if not provided
    if color is None:
        color = theme.COLORS[0]

    # Create scatter plot without categories
    if category is None:
        ax.scatter(x_data, y_data, color=color, s=s, alpha=alpha, **kwargs)
    # Create scatter plot with categories
    else:
        categories = df.select(category).to_numpy().flatten()
        unique_categories = np.unique(categories)

        for i, cat in enumerate(unique_categories):
            mask = categories == cat
            cat_color = theme.COLORS[i % len(theme.COLORS)]

            ax.scatter(x_data[mask], y_data[mask], color=cat_color, s=s, alpha=alpha, label=str(cat), **kwargs)

    return fig, ax


def histogram(
    theme: GGPrism,
    df: pl.DataFrame,
    column: str,
    ax: Optional[plt.Axes] = None,
    figsize: Tuple[float, float] = (10, 6),
    bins: Union[int, List[float]] = 10,
    color: Optional[str] = None,
    alpha: float = 0.7,
    kde: bool = False,
    **kwargs
) -> Tuple[plt.Figure, plt.Axes]:
    """
    Create a histogram with GGPrism styling using Polars DataFrame

    Parameters:
        theme (GGPrism): GGPrism theme instance
        df (pl.DataFrame): Polars DataFrame with data to plot
        column (str): Column name for the histogram
        ax (plt.Axes, optional): Existing axes to plot on
        figsize (tuple): Figure size as (width, height) in inches
        bins (int or list): Number of bins or bin edges
        color (str, optional): Bar color, defaults to first theme color
        alpha (float): Bar transparency
        kde (bool): Whether to add a kernel density estimate curve
        **kwargs: Additional arguments for plt.hist

    Returns:
        tuple: (fig, ax) Matplotlib figure and axes objects
    """
    # Create figure and axes if not provided
    if ax is None:
        fig, ax = theme.create_figure(figsize)
    else:
        fig = ax.figure
        theme.apply_theme(ax)

    # Set default color if not provided
    if color is None:
        color = theme.COLORS[0]

    # Get data as numpy array
    data = df.select(column).to_numpy().flatten()

    # Create histogram
    n, bins, patches = ax.hist(data, bins=bins, color=color, alpha=alpha, edgecolor="white", linewidth=0.8, **kwargs)

    # Add kernel density estimate if requested
    if kde:
        # Need to compute bin centers for overlay
        bin_centers = 0.5 * (bins[1:] + bins[:-1])

        # Compute the KDE
        from scipy.stats import gaussian_kde
        from scipy import stats

        # Use scipy's gaussian_kde
        kde = stats.gaussian_kde(data)

        # Generate points to evaluate the KDE
        x_grid = np.linspace(min(data), max(data), 1000)
        y_grid = kde(x_grid)

        # Scale the KDE to match histogram height
        scaling_factor = max(n) / max(y_grid) if max(y_grid) > 0 else 1
        y_grid = y_grid * scaling_factor

        # Plot the KDE
        ax.plot(x_grid, y_grid, color="darkblue", linewidth=2)

    return fig, ax


def box_plot(
    theme: GGPrism,
    df: pl.DataFrame,
    x: Optional[str] = None,
    y: str = None,
    ax: Optional[plt.Axes] = None,
    figsize: Tuple[float, float] = (10, 6),
    color: Optional[str] = None,
    **kwargs
) -> Tuple[plt.Figure, plt.Axes]:
    """
    Create a box plot with GGPrism styling using Polars DataFrame

    Parameters:
        theme (GGPrism): GGPrism theme instance
        df (pl.DataFrame): Polars DataFrame with data to plot
        x (str, optional): Column name for categories
        y (str): Column name for values
        ax (plt.Axes, optional): Existing axes to plot on
        figsize (tuple): Figure size as (width, height) in inches
        color (str, optional): Box color, defaults to first theme color
        **kwargs: Additional arguments for plt.boxplot

    Returns:
        tuple: (fig, ax) Matplotlib figure and axes objects
    """
    # Create figure and axes if not provided
    if ax is None:
        fig, ax = theme.create_figure(figsize)
    else:
        fig = ax.figure
        theme.apply_theme(ax)

    # Set default color if not provided
    if color is None:
        color = theme.COLORS[0]

    # Simple single boxplot
    if x is None:
        data = df.select(y).to_numpy().flatten()
        boxplot = ax.boxplot(data, patch_artist=True, **kwargs)

        # Style the boxplot
        for element in ["boxes", "whiskers", "fliers", "means", "medians", "caps"]:
            plt.setp(boxplot[element], color=theme.FIXED_COLORS["axis_color"])

        for patch in boxplot["boxes"]:
            patch.set(facecolor=color, alpha=0.7)

    # Multiple boxplots by category
    else:
        # Get unique categories
        categories = df.select(pl.col(x).unique()).to_numpy().flatten()

        # Prepare data for each category
        boxplot_data = []
        for cat in categories:
            cat_data = df.filter(pl.col(x) == cat).select(y).to_numpy().flatten()
            boxplot_data.append(cat_data)

        # Create boxplot
        boxplot = ax.boxplot(boxplot_data, patch_artist=True, labels=categories, **kwargs)

        # Style the boxplot
        for element in ["whiskers", "fliers", "means", "medians", "caps"]:
            plt.setp(boxplot[element], color=theme.FIXED_COLORS["axis_color"])

        for i, patch in enumerate(boxplot["boxes"]):
            patch_color = theme.COLORS[i % len(theme.COLORS)]
            patch.set(facecolor=patch_color, alpha=0.7)

    return fig, ax
