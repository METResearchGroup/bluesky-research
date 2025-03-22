"""Creates visualizations from feed proportion per source data."""

import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from lib.helper import generate_current_datetime_str

current_filedir = os.path.dirname(os.path.abspath(__file__))


def plot_feed_proportions(csv_file_path: str) -> None:
    """
    Creates two plots from feed proportion per source data:
    1. Time series of proportions for 'most-liked' and 'firehose' feeds
    2. Stacked bar chart of total posts by source

    Args:
        csv_file_path: Path to the CSV file containing feed proportion data
    """
    # Create timestamp for output directory
    timestamp = generate_current_datetime_str()
    output_dir = os.path.join(
        current_filedir, f"output/feed_proportion_per_source_{timestamp}"
    )
    os.makedirs(output_dir, exist_ok=True)

    # Read the CSV file
    df = pd.read_csv(csv_file_path)

    # Convert date column to datetime
    df["date"] = pd.to_datetime(df["date"])

    # Sort by date ascending
    df = df.sort_values("date", ascending=True)

    # Plot 1: Time series of proportions
    plt.figure(figsize=(12, 6))
    plt.plot(
        df["date"],
        df["Average proportion of most-liked posts per feed"],
        marker="o",
        linestyle="-",
        label="Average proportion of most-liked posts per feed",
    )
    plt.plot(
        df["date"],
        df["Average proportion of firehose posts per feed"],
        marker="s",
        linestyle="-",
        label="Average proportion of firehose posts per feed",
    )

    # Format the plot
    plt.xlabel("Date")
    plt.ylabel("Proportion")
    plt.title("Proportion of Posts by Feed Source Over Time")
    plt.legend()
    plt.grid(True, alpha=0.3)

    # Format x-axis dates
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=7))
    plt.gcf().autofmt_xdate()

    # Save the plot
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "feed_proportions_time_series.jpeg"), dpi=300)
    plt.close()

    # Plot 2: Stacked bar chart of total posts
    plt.figure(figsize=(12, 6))

    # Create the stacked bar chart
    plt.bar(
        df["date"],
        df["Total firehose posts used across all feeds"],
        label="Total posts from 'firehose' feed",
        color="tab:orange",
    )
    plt.bar(
        df["date"],
        df["Total most-liked posts used across all feeds"],
        bottom=df["Total firehose posts used across all feeds"],
        label="Total posts from 'most-liked' feed",
        color="tab:blue",
    )

    # Format the plot
    plt.xlabel("Date")
    plt.ylabel("Total Posts")
    plt.title("Total Posts by Feed Source Over Time")
    plt.legend()
    plt.grid(True, alpha=0.3)

    # Format x-axis dates
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=7))
    plt.gcf().autofmt_xdate()

    # Save the plot
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "feed_total_posts_stacked_bar.jpeg"), dpi=300)
    plt.close()

    print(f"Plots saved to {output_dir}")


def find_latest_csv_file() -> str:
    """
    Finds the most recent feed proportion CSV file based on timestamp in the filename.

    Returns:
        Path to the most recent CSV file
    """
    feeds_dir = os.path.join(os.path.dirname(current_filedir), "generate_reports/feeds")

    csv_files = [
        f
        for f in os.listdir(feeds_dir)
        if f.startswith("feed_proportion_per_source_") and f.endswith(".csv")
    ]

    if not csv_files:
        raise FileNotFoundError("No feed proportion CSV files found")

    # Sort files by timestamp in filename (newest first)
    csv_files.sort(reverse=True)

    return os.path.join(feeds_dir, csv_files[0])


if __name__ == "__main__":
    try:
        latest_csv_file = find_latest_csv_file()
        print(f"Using CSV file: {latest_csv_file}")
        plot_feed_proportions(latest_csv_file)
    except Exception as e:
        print(f"Error: {e}")
