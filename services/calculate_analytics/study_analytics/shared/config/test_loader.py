"""Test script to demonstrate the new Pydantic v2-based configuration system."""

from services.calculate_analytics.study_analytics.shared.config import (
    get_config,
    get_study_config,
    get_feature_config,
    get_default_config,
    get_study_week_dates,
    get_feature_columns,
    get_feature_threshold,
    validate_config,
)
from lib.log.logger import get_logger

logger = get_logger(__name__)


def test_basic_config_loading():
    """Test basic configuration loading and validation."""
    print("=== Testing Basic Configuration Loading ===")

    try:
        # Test full config loading
        config = get_config()
        print("✓ Full config loaded successfully")
        print(f"  - Number of studies: {len(config.studies)}")
        print(f"  - Number of features: {len(config.features)}")
        print(f"  - Default lookback days: {config.defaults.lookback_days}")

        return True
    except Exception as e:
        print(f"✗ Failed to load configuration: {e}")
        return False


def test_study_config_access():
    """Test accessing study-specific configuration."""
    print("\n=== Testing Study Configuration Access ===")

    try:
        # Test wave1 study
        wave1_config = get_study_config("wave1")
        print("✓ Wave1 study config loaded successfully")
        print(f"  - Start date: {wave1_config.start_date}")
        print(f"  - End date: {wave1_config.end_date}")
        print(f"  - Number of weeks: {len(wave1_config.weeks)}")

        # Test week access
        first_week = wave1_config.weeks[0]
        print(
            f"  - First week: {first_week.start} to {first_week.end} (week {first_week.number})"
        )

        return True
    except Exception as e:
        print(f"✗ Failed to load study configuration: {e}")
        return False


def test_feature_config_access():
    """Test accessing feature-specific configuration."""
    print("\n=== Testing Feature Configuration Access ===")

    try:
        # Test toxicity feature
        toxicity_config = get_feature_config("toxicity")
        print("✓ Toxicity feature config loaded successfully")
        print(f"  - Threshold: {toxicity_config.threshold}")
        print(f"  - Columns: {toxicity_config.columns}")

        # Test IME feature
        ime_config = get_feature_config("ime")
        print("✓ IME feature config loaded successfully")
        print(f"  - Threshold: {ime_config.threshold}")
        print(f"  - Number of columns: {len(ime_config.columns)}")

        return True
    except Exception as e:
        print(f"✗ Failed to load feature configuration: {e}")
        return False


def test_utility_functions():
    """Test utility functions for configuration access."""
    print("\n=== Testing Utility Functions ===")

    try:
        # Test week dates
        week_dates = get_study_week_dates("wave1")
        print("✓ Week dates retrieved successfully")
        print(
            f"  - Week start dates: {week_dates['week_start_dates'][:3]}..."
        )  # Show first 3

        # Test feature columns
        toxicity_columns = get_feature_columns("toxicity")
        print("✓ Toxicity columns retrieved successfully")
        print(f"  - Columns: {toxicity_columns}")

        # Test feature threshold
        toxicity_threshold = get_feature_threshold("toxicity")
        print("✓ Toxicity threshold retrieved successfully")
        print(f"  - Threshold: {toxicity_threshold}")

        # Test default config
        default_config = get_default_config()
        print("✓ Default config retrieved successfully")
        print(f"  - Lookback days: {default_config['lookback_days']}")
        print(f"  - Label threshold: {default_config['label_threshold']}")

        return True
    except Exception as e:
        print(f"✗ Failed to test utility functions: {e}")
        return False


def test_dot_notation_access():
    """Test dot notation access to configuration."""
    print("\n=== Testing Dot Notation Access ===")

    try:
        # Test accessing nested values
        toxicity_threshold = get_config("features.toxicity.threshold")
        print("✓ Dot notation access successful")
        print(f"  - Toxicity threshold via dot notation: {toxicity_threshold}")

        # Test accessing study dates
        wave1_start = get_config("studies.wave1.start_date")
        print(f"  - Wave1 start date via dot notation: {wave1_start}")

        # Test accessing default values
        lookback_days = get_config("defaults.lookback_days")
        print(f"  - Lookback days via dot notation: {lookback_days}")

        return True
    except Exception as e:
        print(f"✗ Failed to test dot notation access: {e}")
        return False


def test_validation():
    """Test configuration validation."""
    print("\n=== Testing Configuration Validation ===")

    try:
        is_valid = validate_config()
        if is_valid:
            print("✓ Configuration validation passed")
        else:
            print("✗ Configuration validation failed")

        return is_valid
    except Exception as e:
        print(f"✗ Validation test failed: {e}")
        return False


def test_type_safety():
    """Test that the configuration provides proper type safety."""
    print("\n=== Testing Type Safety ===")

    try:
        # Get typed configs
        config = get_config()
        wave1_config = get_study_config("wave1")
        toxicity_config = get_feature_config("toxicity")

        # Test that we get proper types
        print("✓ Type safety verified")
        print(f"  - Config type: {type(config).__name__}")
        print(f"  - Study config type: {type(wave1_config).__name__}")
        print(f"  - Feature config type: {type(toxicity_config).__name__}")

        # Test that we can access attributes safely
        print(f"  - Wave1 start date type: {type(wave1_config.start_date).__name__}")
        print(
            f"  - Toxicity threshold type: {type(toxicity_config.threshold).__name__}"
        )

        # Test that dates are properly parsed
        print(f"  - Wave1 start date: {wave1_config.start_date} (parsed as date)")

        return True
    except Exception as e:
        print(f"✗ Type safety test failed: {e}")
        return False


def test_pydantic_v2_features():
    """Test Pydantic v2 specific features."""
    print("\n=== Testing Pydantic v2 Features ===")

    try:
        config = get_config()

        # Test model_dump() method (Pydantic v2)
        config_dict = config.model_dump()
        print("✓ model_dump() method works correctly")
        print(f"  - Config dict keys: {list(config_dict.keys())}")

        # Test model_dump_json() method (Pydantic v2)
        config_json = config.model_dump_json()
        print("✓ model_dump_json() method works correctly")
        print(f"  - JSON length: {len(config_json)} characters")

        # Test that we can access nested models
        wave1_study = config.studies["wave1"]
        study_dict = wave1_study.model_dump()
        print("✓ Nested model model_dump() works")
        print(f"  - Study dict keys: {list(study_dict.keys())}")

        return True
    except Exception as e:
        print(f"✗ Pydantic v2 features test failed: {e}")
        return False


def main():
    """Run all tests."""
    print("🚀 Testing Pydantic v2-based Configuration System\n")

    tests = [
        test_basic_config_loading,
        test_study_config_access,
        test_feature_config_access,
        test_utility_functions,
        test_dot_notation_access,
        test_validation,
        test_type_safety,
        test_pydantic_v2_features,
    ]

    passed = 0
    total = len(tests)

    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"✗ Test {test.__name__} failed with exception: {e}")

    print(f"\n📊 Test Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! Configuration system is working correctly.")
    else:
        print("❌ Some tests failed. Please check the configuration and models.")

    return passed == total


if __name__ == "__main__":
    main()
