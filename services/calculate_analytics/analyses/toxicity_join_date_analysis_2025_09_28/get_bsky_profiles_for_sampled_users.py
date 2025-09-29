"""
Fetch Bluesky profiles for sampled users and combine with toxicity data.

This script loads the sampled user data, fetches their Bluesky profiles via API,
and combines the toxicity/outrage data with profile information including
join dates, follower counts, and other profile details.
"""

import os
from datetime import datetime
from typing import Optional
import time

from atproto_client.exceptions import BadRequestError
import pandas as pd

from transform.bluesky_helper import get_author_record


def load_sampled_users() -> pd.DataFrame:
    """
    Load the sampled users data.

    Returns:
        DataFrame with sampled user data
    """
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Find the most recent sampled users file
    sampled_users_dir = os.path.join(script_dir, "sampled_users")
    if not os.path.exists(sampled_users_dir):
        raise FileNotFoundError(
            f"Sampled users directory not found: {sampled_users_dir}"
        )

    # Get all timestamp directories and find the most recent one
    timestamp_dirs = [
        d
        for d in os.listdir(sampled_users_dir)
        if os.path.isdir(os.path.join(sampled_users_dir, d))
    ]
    if not timestamp_dirs:
        raise FileNotFoundError("No timestamp directories found in sampled_users")

    latest_timestamp = sorted(timestamp_dirs)[-1]
    data_file = os.path.join(
        sampled_users_dir, latest_timestamp, "sampled_users.parquet"
    )

    if not os.path.exists(data_file):
        raise FileNotFoundError(f"Sampled users file not found: {data_file}")

    print(f"📊 Loading sampled users from: {data_file}")
    df = pd.read_parquet(data_file)
    print(f"✅ Loaded {len(df):,} sampled users")

    return df


def load_existing_profiles() -> pd.DataFrame:
    """
    Load any existing profile data to avoid re-fetching.

    Returns:
        DataFrame with existing profile data, or empty DataFrame if none exists
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    profiles_dir = os.path.join(script_dir, "sampled_user_profiles")

    if not os.path.exists(profiles_dir):
        print("📁 No existing profiles directory found")
        return pd.DataFrame()

    # Find all parquet files in timestamp subdirectories
    all_files = []
    for timestamp_dir in os.listdir(profiles_dir):
        timestamp_path = os.path.join(profiles_dir, timestamp_dir)
        if os.path.isdir(timestamp_path):
            for file in os.listdir(timestamp_path):
                if file.endswith(".parquet"):
                    all_files.append(os.path.join(timestamp_path, file))

    if not all_files:
        print("📁 No existing profile files found")
        return pd.DataFrame()

    # Load and combine all existing profile files
    existing_dfs = []
    for file_path in all_files:
        df = pd.read_parquet(file_path)
        existing_dfs.append(df)

    combined_df = pd.concat(existing_dfs, ignore_index=True)
    print(f"📁 Found {len(combined_df):,} existing profiles")

    return combined_df


def get_user_profile(did: str) -> Optional[dict]:
    """
    Fetch a user's profile from Bluesky API.

    Args:
        did: User's DID

    Returns:
        Dictionary with profile data or None if error
    """
    try:
        profile = get_author_record(did=did)
        profile_dict = profile.model_dump()

        # Extract only the fields we need
        return {
            "did": profile_dict["did"],
            "created_at": profile_dict["created_at"],
            "description": profile_dict["description"],
            "display_name": profile_dict["display_name"],
            "followers_count": profile_dict["followers_count"],
            "follows_count": profile_dict["follows_count"],
            "handle": profile_dict["handle"],
            "posts_count": profile_dict["posts_count"],
        }

    except BadRequestError as e:
        print(f"❌ Bad request for DID {did}: {e}")
        return None
    except Exception as e:
        print(f"❌ Error fetching profile for DID {did}: {e}")
        return None


def process_user_chunk(
    user_chunk: pd.DataFrame, chunk_num: int, total_chunks: int
) -> pd.DataFrame:
    """
    Process a chunk of users to fetch their profiles.

    Args:
        user_chunk: DataFrame with user data for this chunk
        chunk_num: Current chunk number
        total_chunks: Total number of chunks

    Returns:
        DataFrame with combined toxicity and profile data
    """
    print(f"🔄 Processing chunk {chunk_num}/{total_chunks} ({len(user_chunk)} users)")

    profiles_data = []
    successful_fetches = 0
    failed_fetches = 0

    for idx, row in user_chunk.iterrows():
        did = row["author_did"]
        print(f"   📡 Fetching profile for {did}...")

        profile_data = get_user_profile(did)

        if profile_data:
            # Combine toxicity data with profile data
            combined_data = {
                "author_did": did,
                "average_toxicity": row["average_toxicity"],
                "average_outrage": row["average_outrage"],
                "total_labeled_posts": row["total_labeled_posts"],
                **profile_data,
            }
            profiles_data.append(combined_data)
            successful_fetches += 1
        else:
            failed_fetches += 1

        # Small delay to be respectful to the API
        time.sleep(0.1)

    print(
        f"   ✅ Chunk {chunk_num} complete: {successful_fetches} successful, {failed_fetches} failed"
    )

    return pd.DataFrame(profiles_data)


def save_profiles_chunk(profiles_df: pd.DataFrame, chunk_num: int) -> str:
    """
    Save a chunk of profile data to parquet file.

    Args:
        profiles_df: DataFrame with profile data
        chunk_num: Chunk number for filename

    Returns:
        Path to the saved file
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    output_dir = os.path.join(script_dir, "sampled_user_profiles", timestamp)
    os.makedirs(output_dir, exist_ok=True)

    filename = f"user_profiles_chunk_{chunk_num:03d}.parquet"
    output_file = os.path.join(output_dir, filename)

    profiles_df.to_parquet(output_file, index=False)

    print(f"💾 Saved chunk {chunk_num} to: {output_file}")
    print(f"   - File size: {os.path.getsize(output_file) / 1024 / 1024:.2f} MB")

    return output_file


def main(chunk_size: int = 100):
    """
    Main function to fetch profiles for sampled users.

    Args:
        chunk_size: Number of users to process in each chunk
    """
    print("🔍 Starting Bluesky Profile Fetching")
    print("=" * 50)

    try:
        # Load sampled users
        sampled_users = load_sampled_users()

        # Load existing profiles to avoid re-fetching
        existing_profiles = load_existing_profiles()

        # Filter out users we already have profiles for
        if not existing_profiles.empty:
            existing_dids = set(existing_profiles["author_did"])
            original_count = len(sampled_users)
            sampled_users = sampled_users[
                ~sampled_users["author_did"].isin(existing_dids)
            ]
            filtered_count = len(sampled_users)
            print(
                f"🔧 Filtered out {original_count - filtered_count:,} users with existing profiles"
            )
            print(f"   - Remaining users to process: {filtered_count:,}")

        if sampled_users.empty:
            print("✅ All users already have profiles fetched!")
            return

        # Process in chunks
        total_users = len(sampled_users)
        total_chunks = (total_users + chunk_size - 1) // chunk_size

        print(
            f"📦 Processing {total_users:,} users in {total_chunks:,} chunks of {chunk_size:,}"
        )

        saved_files = []

        for chunk_num in range(total_chunks):
            start_idx = chunk_num * chunk_size
            end_idx = min(start_idx + chunk_size, total_users)
            user_chunk = sampled_users.iloc[start_idx:end_idx]

            # Process this chunk
            profiles_chunk = process_user_chunk(user_chunk, chunk_num + 1, total_chunks)

            if not profiles_chunk.empty:
                # Save this chunk
                output_file = save_profiles_chunk(profiles_chunk, chunk_num + 1)
                saved_files.append(output_file)

            print()  # Add spacing between chunks

        print("🎉 Profile fetching completed!")
        print(f"📁 Saved {len(saved_files)} chunk files")

        # Summary statistics
        if saved_files:
            # Load all saved data for summary
            all_profiles = []
            for file_path in saved_files:
                df = pd.read_parquet(file_path)
                all_profiles.append(df)

            combined_profiles = pd.concat(all_profiles, ignore_index=True)

            print()
            print("📊 Summary:")
            print(f"   - Total profiles fetched: {len(combined_profiles):,}")
            print(
                f"   - Users with join dates: {combined_profiles['created_at'].notna().sum():,}"
            )
            print(
                f"   - Average followers: {combined_profiles['followers_count'].mean():.0f}"
            )
            print(
                f"   - Average follows: {combined_profiles['follows_count'].mean():.0f}"
            )
            print(f"   - Average posts: {combined_profiles['posts_count'].mean():.0f}")

            # Show date range of join dates
            if combined_profiles["created_at"].notna().any():
                join_dates = pd.to_datetime(combined_profiles["created_at"])
                print(
                    f"   - Join date range: {join_dates.min().strftime('%Y-%m-%d')} to {join_dates.max().strftime('%Y-%m-%d')}"
                )

    except Exception as e:
        print(f"❌ Error during profile fetching: {e}")
        raise


if __name__ == "__main__":
    main(chunk_size=100)
