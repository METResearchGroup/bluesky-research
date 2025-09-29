#!/usr/bin/env python3
"""
Statistical Significance Testing: Toxicity and Outrage Before vs After Cutoff Date

This script performs comprehensive statistical analysis to determine if there are
significant differences in toxicity and outrage levels between users who joined
before vs after September 1, 2024.

Statistical Tests Performed:
- Two-sample t-tests for mean differences
- Effect size calculations (Cohen's d)
- Assumption testing (normality, equal variances)
- Confidence intervals for mean differences
- Non-parametric alternatives (Mann-Whitney U) if needed

Author: AI Assistant
Date: 2025-09-29
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
from scipy import stats
from scipy.stats import shapiro, levene, ttest_ind, mannwhitneyu
import warnings

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore")


def load_combined_profile_data() -> pd.DataFrame:
    """
    Load the combined toxicity and profile data from all chunk files across all timestamp directories.
    Handles errors by skipping problematic files and reporting which files loaded successfully.

    Returns:
        DataFrame with combined toxicity and profile data
    """
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Find the profile data directory
    profiles_dir = os.path.join(script_dir, "sampled_user_profiles")
    if not os.path.exists(profiles_dir):
        raise FileNotFoundError(f"Profile data directory not found: {profiles_dir}")

    # Get all timestamp directories
    timestamp_dirs = [
        d
        for d in os.listdir(profiles_dir)
        if os.path.isdir(os.path.join(profiles_dir, d))
    ]
    if not timestamp_dirs:
        raise FileNotFoundError(
            "No timestamp directories found in sampled_user_profiles"
        )

    print(f"📊 Loading profile data from: {profiles_dir}")
    print(f"   - Found {len(timestamp_dirs)} timestamp directories")

    # Load all parquet files from all timestamp directories
    all_dfs = []
    total_files = 0
    successful_files = []
    failed_files = []

    for timestamp_dir in sorted(timestamp_dirs):
        timestamp_path = os.path.join(profiles_dir, timestamp_dir)
        parquet_files = [
            f for f in os.listdir(timestamp_path) if f.endswith(".parquet")
        ]

        for file in sorted(parquet_files):
            file_path = os.path.join(timestamp_path, file)
            try:
                df = pd.read_parquet(file_path)
                all_dfs.append(df)
                total_files += 1
                successful_files.append(f"{timestamp_dir}/{file}")
                print(f"   ✅ {timestamp_dir}/{file}: {len(df)} users")
            except Exception as e:
                failed_files.append(f"{timestamp_dir}/{file}")
                print(f"   ❌ {timestamp_dir}/{file}: Error - {str(e)[:100]}...")

    if not all_dfs:
        raise FileNotFoundError("No parquet files could be loaded successfully")

    combined_df = pd.concat(all_dfs, ignore_index=True)
    print(f"✅ Loaded {len(combined_df):,} user profiles from {total_files} files")

    if failed_files:
        print(f"⚠️  Failed to load {len(failed_files)} files:")
        for failed_file in failed_files:
            print(f"   - {failed_file}")

    print(f"📊 Summary: {len(successful_files)} successful, {len(failed_files)} failed")

    return combined_df


def process_join_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process join dates by converting to datetime and categorizing by cutoff date.

    Args:
        df: DataFrame with 'created_at' column

    Returns:
        DataFrame with processed join dates and cutoff categories
    """
    print("📅 Processing join dates...")

    df = df.copy()

    # Convert created_at to datetime with error handling
    try:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    except Exception as e:
        print(f"⚠️  Error converting timestamps: {e}")
        # Try alternative parsing methods
        try:
            df["created_at"] = pd.to_datetime(
                df["created_at"], format="ISO8601", errors="coerce"
            )
        except Exception as e2:
            print(f"⚠️  ISO8601 parsing failed: {e2}")
            try:
                df["created_at"] = pd.to_datetime(
                    df["created_at"], format="mixed", errors="coerce"
                )
            except Exception as e3:
                print(f"⚠️  Mixed format parsing failed: {e3}")
                print("   Setting invalid timestamps to NaT")
                df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    # Check for invalid timestamps and report
    invalid_count = df["created_at"].isna().sum()
    if invalid_count > 0:
        print(f"⚠️  Found {invalid_count:,} invalid timestamps (set to NaT)")
        print(f"   - Valid timestamps: {len(df) - invalid_count:,}")
        print(f"   - Invalid timestamps: {invalid_count:,}")

    # Define cutoff date: September 1, 2024
    cutoff_date = pd.to_datetime("2024-09-01")

    # Categorize users by join date relative to cutoff
    def categorize_join_date(x):
        if pd.isna(x):
            return "Unknown"
        # Handle timezone-aware timestamps by converting to naive
        if hasattr(x, "tz") and x.tz is not None:
            x = x.tz_localize(None)
        return "Before Sep 1, 2024" if x < cutoff_date else "Sep 1, 2024 or Later"

    df["join_category"] = df["created_at"].apply(categorize_join_date)

    # Handle NaT values (invalid timestamps)
    df.loc[df["created_at"].isna(), "join_category"] = "Unknown"

    # Count users by category
    category_counts = df["join_category"].value_counts()
    print("📈 Users by join category:")
    for category, count in category_counts.items():
        print(f"   - {category}: {count:,} users")

    return df


def test_assumptions(data_before, data_after, metric_name):
    """
    Test statistical assumptions for t-test.

    Args:
        data_before: Data for 'before' group
        data_after: Data for 'after' group
        metric_name: Name of the metric being tested

    Returns:
        dict: Results of assumption tests
    """
    print(f"🔍 Testing assumptions for {metric_name}...")

    results = {}

    # Test normality using Shapiro-Wilk test
    # Note: For large samples (>5000), Shapiro-Wilk may be too sensitive
    # We'll use it but interpret with caution
    if len(data_before) <= 5000:
        shapiro_before = shapiro(data_before)
        shapiro_after = shapiro(data_after)
        results["normality_before"] = {
            "statistic": shapiro_before.statistic,
            "p_value": shapiro_before.pvalue,
            "normal": shapiro_before.pvalue > 0.05,
        }
        results["normality_after"] = {
            "statistic": shapiro_after.statistic,
            "p_value": shapiro_after.pvalue,
            "normal": shapiro_after.pvalue > 0.05,
        }
    else:
        print(
            f"   ⚠️  Sample size too large ({len(data_before)}), skipping Shapiro-Wilk test"
        )
        results["normality_before"] = {"normal": "not_tested"}
        results["normality_after"] = {"normal": "not_tested"}

    # Test equal variances using Levene's test
    levene_result = levene(data_before, data_after)
    results["equal_variances"] = {
        "statistic": levene_result.statistic,
        "p_value": levene_result.pvalue,
        "equal": levene_result.pvalue > 0.05,
    }

    return results


def calculate_effect_size(data_before, data_after):
    """
    Calculate Cohen's d effect size.

    Args:
        data_before: Data for 'before' group
        data_after: Data for 'after' group

    Returns:
        float: Cohen's d effect size
    """
    n1, n2 = len(data_before), len(data_after)
    s1, s2 = np.std(data_before, ddof=1), np.std(data_after, ddof=1)

    # Pooled standard deviation
    pooled_std = np.sqrt(((n1 - 1) * s1**2 + (n2 - 1) * s2**2) / (n1 + n2 - 2))

    # Cohen's d
    cohens_d = (np.mean(data_after) - np.mean(data_before)) / pooled_std

    return cohens_d


def perform_statistical_tests(data_before, data_after, metric_name):
    """
    Perform comprehensive statistical tests.

    Args:
        data_before: Data for 'before' group
        data_after: Data for 'after' group
        metric_name: Name of the metric being tested

    Returns:
        dict: Comprehensive test results
    """
    print(f"📊 Performing statistical tests for {metric_name}...")

    results = {}

    # Basic descriptive statistics
    results["descriptive"] = {
        "before_mean": np.mean(data_before),
        "before_std": np.std(data_before, ddof=1),
        "before_n": len(data_before),
        "after_mean": np.mean(data_after),
        "after_std": np.std(data_after, ddof=1),
        "after_n": len(data_after),
        "mean_difference": np.mean(data_after) - np.mean(data_before),
    }

    # Test assumptions
    results["assumptions"] = test_assumptions(data_before, data_after, metric_name)

    # Two-sample t-test
    ttest_result = ttest_ind(
        data_after,
        data_before,
        equal_var=results["assumptions"]["equal_variances"]["equal"],
    )
    results["ttest"] = {
        "statistic": ttest_result.statistic,
        "p_value": ttest_result.pvalue,
        "significant": ttest_result.pvalue < 0.05,
        "equal_var": results["assumptions"]["equal_variances"]["equal"],
    }

    # Effect size (Cohen's d)
    results["effect_size"] = calculate_effect_size(data_before, data_after)

    # Confidence interval for mean difference
    n1, n2 = len(data_before), len(data_after)
    s1, s2 = np.std(data_before, ddof=1), np.std(data_after, ddof=1)

    if results["assumptions"]["equal_variances"]["equal"]:
        # Equal variances
        pooled_std = np.sqrt(((n1 - 1) * s1**2 + (n2 - 1) * s2**2) / (n1 + n2 - 2))
        se_diff = pooled_std * np.sqrt(1 / n1 + 1 / n2)
    else:
        # Unequal variances (Welch's t-test)
        se_diff = np.sqrt(s1**2 / n1 + s2**2 / n2)

    # 95% confidence interval
    df = n1 + n2 - 2 if results["assumptions"]["equal_variances"]["equal"] else None
    if df is None:
        # Welch-Satterthwaite equation for degrees of freedom
        df = (s1**2 / n1 + s2**2 / n2) ** 2 / (
            (s1**2 / n1) ** 2 / (n1 - 1) + (s2**2 / n2) ** 2 / (n2 - 1)
        )

    t_critical = stats.t.ppf(0.975, df)
    margin_error = t_critical * se_diff
    mean_diff = results["descriptive"]["mean_difference"]

    results["confidence_interval"] = {
        "lower": mean_diff - margin_error,
        "upper": mean_diff + margin_error,
        "margin_error": margin_error,
    }

    # Non-parametric test (Mann-Whitney U) as alternative
    try:
        mw_result = mannwhitneyu(data_after, data_before, alternative="two-sided")
        results["mann_whitney"] = {
            "statistic": mw_result.statistic,
            "p_value": mw_result.pvalue,
            "significant": mw_result.pvalue < 0.05,
        }
    except Exception as e:
        print(f"   ⚠️  Mann-Whitney U test failed: {e}")
        results["mann_whitney"] = {"error": str(e)}

    return results


def save_results(results_toxicity, results_outrage, output_dir):
    """
    Save statistical test results to CSV files.

    Args:
        results_toxicity: Results for toxicity tests
        results_outrage: Results for outrage tests
        output_dir: Directory to save results
    """
    print("💾 Saving statistical test results...")

    # Create summary results
    summary_data = []

    for metric, results in [
        ("Toxicity", results_toxicity),
        ("Outrage", results_outrage),
    ]:
        summary_data.append(
            {
                "Metric": metric,
                "Before_Mean": results["descriptive"]["before_mean"],
                "Before_Std": results["descriptive"]["before_std"],
                "Before_N": results["descriptive"]["before_n"],
                "After_Mean": results["descriptive"]["after_mean"],
                "After_Std": results["descriptive"]["after_std"],
                "After_N": results["descriptive"]["after_n"],
                "Mean_Difference": results["descriptive"]["mean_difference"],
                "T_Statistic": results["ttest"]["statistic"],
                "T_P_Value": results["ttest"]["p_value"],
                "T_Significant": results["ttest"]["significant"],
                "Equal_Variances": results["assumptions"]["equal_variances"]["equal"],
                "Levene_P_Value": results["assumptions"]["equal_variances"]["p_value"],
                "Cohens_D": results["effect_size"],
                "CI_Lower": results["confidence_interval"]["lower"],
                "CI_Upper": results["confidence_interval"]["upper"],
                "MW_Statistic": results["mann_whitney"].get("statistic", "N/A"),
                "MW_P_Value": results["mann_whitney"].get("p_value", "N/A"),
                "MW_Significant": results["mann_whitney"].get("significant", "N/A"),
            }
        )

    # Save summary results
    summary_df = pd.DataFrame(summary_data)
    summary_file = os.path.join(output_dir, "statistical_test_summary.csv")
    summary_df.to_csv(summary_file, index=False)
    print(f"   ✅ Summary results saved to: {summary_file}")

    # Save detailed results for each metric
    for metric, results in [
        ("Toxicity", results_toxicity),
        ("Outrage", results_outrage),
    ]:
        detailed_data = []

        # Add descriptive statistics
        desc = results["descriptive"]
        detailed_data.extend(
            [
                {
                    "Test_Type": "Descriptive",
                    "Statistic": "Before_Mean",
                    "Value": desc["before_mean"],
                },
                {
                    "Test_Type": "Descriptive",
                    "Statistic": "Before_Std",
                    "Value": desc["before_std"],
                },
                {
                    "Test_Type": "Descriptive",
                    "Statistic": "Before_N",
                    "Value": desc["before_n"],
                },
                {
                    "Test_Type": "Descriptive",
                    "Statistic": "After_Mean",
                    "Value": desc["after_mean"],
                },
                {
                    "Test_Type": "Descriptive",
                    "Statistic": "After_Std",
                    "Value": desc["after_std"],
                },
                {
                    "Test_Type": "Descriptive",
                    "Statistic": "After_N",
                    "Value": desc["after_n"],
                },
                {
                    "Test_Type": "Descriptive",
                    "Statistic": "Mean_Difference",
                    "Value": desc["mean_difference"],
                },
            ]
        )

        # Add assumption tests
        assumptions = results["assumptions"]
        detailed_data.extend(
            [
                {
                    "Test_Type": "Normality",
                    "Statistic": "Before_Shapiro_Statistic",
                    "Value": assumptions["normality_before"].get("statistic", "N/A"),
                },
                {
                    "Test_Type": "Normality",
                    "Statistic": "Before_Shapiro_P_Value",
                    "Value": assumptions["normality_before"].get("p_value", "N/A"),
                },
                {
                    "Test_Type": "Normality",
                    "Statistic": "Before_Normal",
                    "Value": assumptions["normality_before"].get("normal", "N/A"),
                },
                {
                    "Test_Type": "Normality",
                    "Statistic": "After_Shapiro_Statistic",
                    "Value": assumptions["normality_after"].get("statistic", "N/A"),
                },
                {
                    "Test_Type": "Normality",
                    "Statistic": "After_Shapiro_P_Value",
                    "Value": assumptions["normality_after"].get("p_value", "N/A"),
                },
                {
                    "Test_Type": "Normality",
                    "Statistic": "After_Normal",
                    "Value": assumptions["normality_after"].get("normal", "N/A"),
                },
                {
                    "Test_Type": "Equal_Variances",
                    "Statistic": "Levene_Statistic",
                    "Value": assumptions["equal_variances"]["statistic"],
                },
                {
                    "Test_Type": "Equal_Variances",
                    "Statistic": "Levene_P_Value",
                    "Value": assumptions["equal_variances"]["p_value"],
                },
                {
                    "Test_Type": "Equal_Variances",
                    "Statistic": "Equal_Variances",
                    "Value": assumptions["equal_variances"]["equal"],
                },
            ]
        )

        # Add t-test results
        ttest = results["ttest"]
        detailed_data.extend(
            [
                {
                    "Test_Type": "T_Test",
                    "Statistic": "T_Statistic",
                    "Value": ttest["statistic"],
                },
                {
                    "Test_Type": "T_Test",
                    "Statistic": "P_Value",
                    "Value": ttest["p_value"],
                },
                {
                    "Test_Type": "T_Test",
                    "Statistic": "Significant",
                    "Value": ttest["significant"],
                },
                {
                    "Test_Type": "T_Test",
                    "Statistic": "Equal_Variances_Assumed",
                    "Value": ttest["equal_var"],
                },
            ]
        )

        # Add effect size and confidence interval
        detailed_data.extend(
            [
                {
                    "Test_Type": "Effect_Size",
                    "Statistic": "Cohens_D",
                    "Value": results["effect_size"],
                },
                {
                    "Test_Type": "Confidence_Interval",
                    "Statistic": "CI_Lower",
                    "Value": results["confidence_interval"]["lower"],
                },
                {
                    "Test_Type": "Confidence_Interval",
                    "Statistic": "CI_Upper",
                    "Value": results["confidence_interval"]["upper"],
                },
                {
                    "Test_Type": "Confidence_Interval",
                    "Statistic": "Margin_Error",
                    "Value": results["confidence_interval"]["margin_error"],
                },
            ]
        )

        # Add Mann-Whitney U test
        mw = results["mann_whitney"]
        detailed_data.extend(
            [
                {
                    "Test_Type": "Mann_Whitney_U",
                    "Statistic": "MW_Statistic",
                    "Value": mw.get("statistic", "N/A"),
                },
                {
                    "Test_Type": "Mann_Whitney_U",
                    "Statistic": "MW_P_Value",
                    "Value": mw.get("p_value", "N/A"),
                },
                {
                    "Test_Type": "Mann_Whitney_U",
                    "Statistic": "MW_Significant",
                    "Value": mw.get("significant", "N/A"),
                },
            ]
        )

        detailed_df = pd.DataFrame(detailed_data)
        detailed_file = os.path.join(
            output_dir, f"{metric.lower()}_detailed_results.csv"
        )
        detailed_df.to_csv(detailed_file, index=False)
        print(f"   ✅ {metric} detailed results saved to: {detailed_file}")


def print_summary(results_toxicity, results_outrage):
    """
    Print a summary of statistical test results.

    Args:
        results_toxicity: Results for toxicity tests
        results_outrage: Results for outrage tests
    """
    print("\n" + "=" * 80)
    print("📊 STATISTICAL SIGNIFICANCE TEST RESULTS SUMMARY")
    print("=" * 80)

    for metric, results in [
        ("TOXICITY", results_toxicity),
        ("OUTRAGE", results_outrage),
    ]:
        print(f"\n🔬 {metric} ANALYSIS:")
        print("-" * 40)

        # Descriptive statistics
        desc = results["descriptive"]
        print("📈 Descriptive Statistics:")
        print(
            f"   Before Sep 1, 2024: M={desc['before_mean']:.4f}, SD={desc['before_std']:.4f}, n={desc['before_n']:,}"
        )
        print(
            f"   Sep 1, 2024 or Later: M={desc['after_mean']:.4f}, SD={desc['after_std']:.4f}, n={desc['after_n']:,}"
        )
        print(f"   Mean Difference: {desc['mean_difference']:.4f}")

        # Assumptions
        assumptions = results["assumptions"]
        print("\n🔍 Assumption Tests:")
        print(
            f"   Equal Variances (Levene): p={assumptions['equal_variances']['p_value']:.4f}, Equal={assumptions['equal_variances']['equal']}"
        )
        if assumptions["normality_before"].get("normal") != "not_tested":
            print(
                f"   Normality Before: p={assumptions['normality_before']['p_value']:.4f}, Normal={assumptions['normality_before']['normal']}"
            )
            print(
                f"   Normality After: p={assumptions['normality_after']['p_value']:.4f}, Normal={assumptions['normality_after']['normal']}"
            )
        else:
            print("   Normality: Not tested (sample size > 5000)")

        # T-test results
        ttest = results["ttest"]
        significance = "SIGNIFICANT" if ttest["significant"] else "NOT SIGNIFICANT"
        print("\n📊 Two-Sample T-Test:")
        print(
            f"   t({desc['before_n'] + desc['after_n'] - 2}) = {ttest['statistic']:.4f}"
        )
        print(f"   p = {ttest['p_value']:.6f}")
        print(f"   Result: {significance}")

        # Effect size
        effect_size = results["effect_size"]
        effect_interpretation = (
            "small"
            if abs(effect_size) < 0.2
            else "medium"
            if abs(effect_size) < 0.8
            else "large"
        )
        print("\n📏 Effect Size (Cohen's d):")
        print(f"   d = {effect_size:.4f} ({effect_interpretation} effect)")

        # Confidence interval
        ci = results["confidence_interval"]
        print("\n📊 95% Confidence Interval for Mean Difference:")
        print(f"   [{ci['lower']:.4f}, {ci['upper']:.4f}]")

        # Mann-Whitney U test
        mw = results["mann_whitney"]
        if "error" not in mw:
            mw_significance = "SIGNIFICANT" if mw["significant"] else "NOT SIGNIFICANT"
            print("\n📊 Mann-Whitney U Test (Non-parametric):")
            print(f"   U = {mw['statistic']:.0f}")
            print(f"   p = {mw['p_value']:.6f}")
            print(f"   Result: {mw_significance}")

        print("-" * 40)


def main():
    """
    Main function to orchestrate the statistical significance analysis.
    """
    print("📊 Starting Statistical Significance Analysis")
    print("=" * 60)

    try:
        # Load data
        df = load_combined_profile_data()

        # Process join dates
        df = process_join_dates(df)

        # Filter out "Unknown" entries for analysis
        analysis_df = df[df["join_category"] != "Unknown"].copy()

        if analysis_df.empty:
            print("❌ No valid data available for analysis")
            return

        # Extract data for each group
        before_data = analysis_df[analysis_df["join_category"] == "Before Sep 1, 2024"]
        after_data = analysis_df[analysis_df["join_category"] == "Sep 1, 2024 or Later"]

        print("\n📊 Data Preparation:")
        print(f"   Before Sep 1, 2024: {len(before_data):,} users")
        print(f"   Sep 1, 2024 or Later: {len(after_data):,} users")

        # Extract values and remove any NaN values
        before_toxicity = before_data["average_toxicity"].dropna().values
        after_toxicity = after_data["average_toxicity"].dropna().values
        before_outrage = before_data["average_outrage"].dropna().values
        after_outrage = after_data["average_outrage"].dropna().values

        print(f"   Before toxicity: {len(before_toxicity):,} valid values")
        print(f"   After toxicity: {len(after_toxicity):,} valid values")
        print(f"   Before outrage: {len(before_outrage):,} valid values")
        print(f"   After outrage: {len(after_outrage):,} valid values")

        # Perform statistical tests
        results_toxicity = perform_statistical_tests(
            before_toxicity, after_toxicity, "Toxicity"
        )

        results_outrage = perform_statistical_tests(
            before_outrage, after_outrage, "Outrage"
        )

        # Create output directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        output_dir = os.path.join(
            script_dir,
            "statistical_analysis",
            "two_sample_t_test",
            timestamp,
        )
        os.makedirs(output_dir, exist_ok=True)

        # Save results
        save_results(results_toxicity, results_outrage, output_dir)

        # Print summary
        print_summary(results_toxicity, results_outrage)

        print("\n🎉 Statistical significance analysis completed successfully!")
        print(f"📁 Results saved to: {output_dir}")

    except Exception as e:
        print(f"❌ Error during statistical analysis: {e}")
        raise


if __name__ == "__main__":
    main()
