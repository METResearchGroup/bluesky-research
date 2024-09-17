"""Calculate metrics for engagement and treatment scores."""

from statistics import mean, median, stdev, quantiles

import matplotlib.pyplot as plt


def calculate_summary_statistics(scores: list[float]) -> dict[str, float]:
    return {
        "min": min(scores),
        "max": max(scores),
        "mean": mean(scores),
        "median": median(scores),
        "std_dev": stdev(scores),
        "quartiles": quantiles(scores),
    }


def plot_single_scores(scores: list[float]) -> None:
    """Plots a single set of scores."""
    plt.figure(figsize=(10, 6))
    plt.hist(scores, bins=30, alpha=0.8, label="Scores", color="blue")
    plt.title("Distribution of Scores")
    plt.xlabel("Scores")
    plt.ylabel("Frequency")
    plt.legend()
    plt.grid(True)
    plt.show()


def plot_scores(
    score1: list[float], label1: str, score2: list[float], label2: str
) -> None:
    """Plots two sets of scores on top of each other, for comparison, the
    engagement and the treatment scores."""
    plt.figure(figsize=(10, 6))

    plt.hist(score1, bins=30, alpha=0.8, label=label1, color="blue")
    plt.hist(score2, bins=30, alpha=0.8, label=label2, color="orange")

    plt.title("Distribution of Scores")
    plt.xlabel("Scores")
    plt.ylabel("Frequency")
    plt.legend()
    plt.grid(True)
    plt.show()


def plot_line(x: list[float], y: list[float]) -> None:
    """Plots dots for the given X and Y arrays and draws a smoothed line connecting them."""
    plt.figure(figsize=(10, 6))
    plt.scatter(x, y, color="blue", label="Data Points")

    # Sort the data for a smooth line
    sorted_indices = sorted(range(len(x)), key=lambda i: x[i])
    sorted_x = [x[i] for i in sorted_indices]
    sorted_y = [y[i] for i in sorted_indices]

    # Plot a smoothed line
    plt.plot(sorted_x, sorted_y, color="orange", label="Smoothed Line", linewidth=2)

    plt.title("Plot of X vs Y with Smoothed Line")
    plt.xlabel("X")
    plt.ylabel("Y")
    plt.legend()
    plt.grid(True)
    plt.show()


def plot_lines(
    x1: list[float],
    y1: list[float],
    x2: list[float],
    y2: list[float],
    label1: str,
    label2: str,
) -> None:
    """Plots two sets of points with smoothed lines for comparison."""
    plt.figure(figsize=(10, 6))

    # Plot the first set of points and line
    plt.scatter(x1, y1, color="blue", label=label1)
    sorted_indices1 = sorted(range(len(x1)), key=lambda i: x1[i])
    sorted_x1 = [x1[i] for i in sorted_indices1]
    sorted_y1 = [y1[i] for i in sorted_indices1]
    plt.plot(sorted_x1, sorted_y1, color="blue", linewidth=2)

    # Plot the second set of points and line
    plt.scatter(x2, y2, color="orange", label=label2)
    sorted_indices2 = sorted(range(len(x2)), key=lambda i: x2[i])
    sorted_x2 = [x2[i] for i in sorted_indices2]
    sorted_y2 = [y2[i] for i in sorted_indices2]
    plt.plot(sorted_x2, sorted_y2, color="orange", linewidth=2)

    plt.title("Comparison of Two Sets of Data")
    plt.xlabel("X")
    plt.ylabel("Y")
    plt.legend()
    plt.grid(True)
    plt.show()
