#!/usr/bin/env python3
"""
Dependency checker for the scalable backfill system.

This script:
1. Checks for required Python packages
2. Verifies Redis is installed and running
3. Checks for the Bluesky backfill code
4. Helps users install any missing dependencies
"""

import importlib
import os
import subprocess
import sys
from typing import Dict, List, Tuple

REQUIRED_PACKAGES = {
    "redis": "4.5.0",
    "tqdm": "4.65.0",
    "psutil": "5.9.0",
    "requests": "2.28.0",
    "matplotlib": "3.7.0",
    "numpy": "1.23.0",
    "rich": "13.0.0",
    "tabulate": "0.9.0",
}

OPTIONAL_PACKAGES = {
    "atproto": "0.0.36",  # Only needed for real API calls
}


def check_python_version() -> bool:
    """Check if the Python version is 3.10 or higher.

    Returns:
        True if the Python version is sufficient, False otherwise
    """
    major, minor, _ = sys.version_info
    
    if major < 3 or (major == 3 and minor < 10):
        print("❌ Python 3.10+ is required")
        print(f"   Current version: {sys.version}")
        return False
    
    print(f"✅ Python version: {sys.version}")
    return True


def check_package(package_name: str, min_version: str) -> bool:
    """Check if a package is installed and meets the minimum version.

    Args:
        package_name: Name of the package to check
        min_version: Minimum required version

    Returns:
        True if the package is installed and meets the minimum version, False otherwise
    """
    try:
        # Try to import the package
        pkg = importlib.import_module(package_name)
        
        # Get the version
        version = getattr(pkg, "__version__", "0.0.0")
        
        # Compare versions
        installed_parts = [int(x) for x in version.split(".")]
        required_parts = [int(x) for x in min_version.split(".")]
        
        # Pad with zeros for comparison
        while len(installed_parts) < len(required_parts):
            installed_parts.append(0)
        while len(required_parts) < len(installed_parts):
            required_parts.append(0)
            
        if installed_parts < required_parts:
            print(f"❌ {package_name} {version} (min: {min_version})")
            return False
            
        print(f"✅ {package_name} {version}")
        return True
        
    except ImportError:
        print(f"❌ {package_name} not installed")
        return False
    except Exception as e:
        print(f"❌ Error checking {package_name}: {e}")
        return False


def check_redis_server() -> bool:
    """Check if Redis server is available.

    Returns:
        True if Redis server is available, False otherwise
    """
    # First check if redis-cli is installed
    try:
        subprocess.run(
            ["which", "redis-server"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
        print("✅ Redis server is installed")
    except subprocess.CalledProcessError:
        print("❌ Redis server is not installed")
        return False
        
    # Then check if redis-server is running
    try:
        import redis
        client = redis.Redis(host="localhost", port=6379)
        client.ping()
        print("✅ Redis server is running")
        return True
    except:
        print("❌ Redis server is not running")
        return False


def check_bluesky_code() -> bool:
    """Check if the Bluesky backfill code is available.

    Returns:
        True if the backfill code is found, False otherwise
    """
    bluesky_code_path = "services/backfill/sync"
    
    if os.path.exists(bluesky_code_path):
        print(f"✅ Bluesky backfill code found at {bluesky_code_path}")
        return True
    else:
        print(f"❌ Bluesky backfill code not found at {bluesky_code_path}")
        return False


def generate_install_commands(missing_packages: Dict[str, str]) -> str:
    """Generate pip install commands for missing packages.

    Args:
        missing_packages: Dictionary mapping package names to versions

    Returns:
        Command to install missing packages
    """
    if not missing_packages:
        return ""
        
    packages = [f"{pkg}=={ver}" for pkg, ver in missing_packages.items()]
    return f"pip install {' '.join(packages)}"


def main() -> None:
    """Main function."""
    print("Checking dependencies for the scalable backfill system...")
    print("=" * 60)
    
    # Check Python version
    python_ok = check_python_version()
    
    print("\nRequired packages:")
    print("-" * 60)
    
    # Check required packages
    missing_required = {}
    for package, version in REQUIRED_PACKAGES.items():
        if not check_package(package, version):
            missing_required[package] = version
    
    print("\nOptional packages:")
    print("-" * 60)
    
    # Check optional packages
    missing_optional = {}
    for package, version in OPTIONAL_PACKAGES.items():
        if not check_package(package, version):
            missing_optional[package] = version
    
    print("\nSystem dependencies:")
    print("-" * 60)
    
    # Check Redis
    redis_ok = check_redis_server()
    
    # Check Bluesky code
    bluesky_ok = check_bluesky_code()
    
    # Summary
    print("\nSummary:")
    print("=" * 60)
    
    if not python_ok:
        print("❌ Python version check failed")
        print("   Please upgrade to Python 3.10 or higher")
        
    if missing_required:
        print("❌ Missing required packages:")
        for package in missing_required:
            print(f"   - {package}")
            
        print("\nInstall missing required packages with:")
        print(generate_install_commands(missing_required))
    else:
        print("✅ All required packages are installed")
        
    if missing_optional:
        print("\n⚠️ Missing optional packages:")
        for package in missing_optional:
            print(f"   - {package}")
            
        print("\nInstall optional packages with:")
        print(generate_install_commands(missing_optional))
        
    if not redis_ok:
        print("\n❌ Redis is not available")
        print("   Install Redis and start the server:")
        print("   - On macOS: brew install redis && brew services start redis")
        print("   - On Ubuntu: sudo apt install redis-server && sudo systemctl start redis-server")
        print("   - On Windows: https://redis.io/docs/getting-started/installation/install-redis-on-windows/")
        
    if not bluesky_ok:
        print("\n❌ Bluesky backfill code not found")
        print("   Make sure you're running this script from the project root directory")
        print("   and the Bluesky backfill code is at services/backfill/sync")
        
    # Overall status
    if python_ok and not missing_required and redis_ok and bluesky_ok:
        print("\n✅ All critical dependencies are satisfied")
        print("   You can run the scalable backfill system")
        
        if missing_optional:
            print("   Note: Some optional dependencies are missing")
    else:
        print("\n❌ Some critical dependencies are missing")
        print("   Please resolve the issues above before running the system")


if __name__ == "__main__":
    main() 