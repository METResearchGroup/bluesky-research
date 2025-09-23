"""Simple test script for hashtag analysis module.

This script provides a minimal test to verify the core functionality
of the hashtag analysis module works correctly.
"""

import os
import sys
import pandas as pd

# Add the parent directory to the path to import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hashtag_analysis import (
    extract_hashtags_from_text,
    get_hashtag_counts_for_posts,
    filter_rare_hashtags,
    get_election_period,
    ELECTION_DATE,
    MIN_HASHTAG_FREQUENCY,
)


def test_basic_functionality():
    """Test basic hashtag analysis functionality."""
    print("🧪 Testing basic hashtag analysis functionality...")

    # Test 1: Hashtag extraction
    print("   Test 1: Hashtag extraction")
    test_text = "This is a #test post with #multiple #hashtags"
    hashtags = extract_hashtags_from_text(test_text)
    expected = ["test", "multiple", "hashtags"]
    assert sorted(hashtags) == sorted(expected), f"Expected {expected}, got {hashtags}"
    print("   ✅ Hashtag extraction working")

    # Test 2: Hashtag counting
    print("   Test 2: Hashtag counting")
    posts_df = pd.DataFrame(
        {
            "text": [
                "Post with #test hashtag",
                "Another post with #test and #different hashtags",
                "Third post with #different hashtag",
            ]
        }
    )
    counts = get_hashtag_counts_for_posts(posts_df)
    expected_counts = {"test": 2, "different": 2}
    assert counts == expected_counts, f"Expected {expected_counts}, got {counts}"
    print("   ✅ Hashtag counting working")

    # Test 3: Filtering rare hashtags
    print("   Test 3: Filtering rare hashtags")
    hashtag_counts = {"common": 10, "rare": 2, "very_rare": 1}
    filtered = filter_rare_hashtags(hashtag_counts, min_frequency=5)
    expected_filtered = {"common": 10}
    assert (
        filtered == expected_filtered
    ), f"Expected {expected_filtered}, got {filtered}"
    print("   ✅ Hashtag filtering working")

    # Test 4: Election period determination
    print("   Test 4: Election period determination")
    pre_period = get_election_period("2024-11-04")
    post_period = get_election_period("2024-11-06")
    assert pre_period == "pre_election", f"Expected pre_election, got {pre_period}"
    assert post_period == "post_election", f"Expected post_election, got {post_period}"
    print("   ✅ Election period determination working")

    print("🎉 All basic tests passed!")


def test_edge_cases():
    """Test edge cases and error handling."""
    print("\n🔍 Testing edge cases...")

    # Test empty text
    empty_hashtags = extract_hashtags_from_text("")
    assert empty_hashtags == [], f"Expected empty list, got {empty_hashtags}"
    print("   ✅ Empty text handling")

    # Test None text
    none_hashtags = extract_hashtags_from_text(None)
    assert none_hashtags == [], f"Expected empty list, got {none_hashtags}"
    print("   ✅ None text handling")

    # Test text without hashtags
    no_hashtags = extract_hashtags_from_text("This text has no hashtags")
    assert no_hashtags == [], f"Expected empty list, got {no_hashtags}"
    print("   ✅ No hashtags handling")

    # Test case normalization
    case_hashtags = extract_hashtags_from_text("This has #UPPERCASE and #MixedCase")
    expected_case = ["uppercase", "mixedcase"]
    assert sorted(case_hashtags) == sorted(
        expected_case
    ), f"Expected {expected_case}, got {case_hashtags}"
    print("   ✅ Case normalization working")

    print("🎉 All edge case tests passed!")


def main():
    """Run all tests."""
    print("🚀 Starting Hashtag Analysis Simple Test")
    print("=" * 50)

    try:
        test_basic_functionality()
        test_edge_cases()

        print("\n" + "=" * 50)
        print("🎉 All tests passed successfully!")
        print(f"📊 Election date: {ELECTION_DATE}")
        print(f"🔢 Min hashtag frequency: {MIN_HASHTAG_FREQUENCY}")

    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
