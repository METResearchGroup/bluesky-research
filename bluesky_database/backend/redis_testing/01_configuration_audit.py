#!/usr/bin/env python3
"""
Redis Configuration Audit Script

This script audits the current Redis configuration and compares it against
MET-001 requirements for the Bluesky data pipeline buffer use case.

Usage:
    python 01_configuration_audit.py
"""

import redis
import json
import os
import sys
from typing import Dict, Any, List
from datetime import datetime


class RedisConfigurationAuditor:
    """Audits Redis configuration against MET-001 requirements."""

    def __init__(self, host: str = "localhost", port: int = 6379, password: str = None):
        """Initialize the auditor with Redis connection parameters."""
        self.host = host
        self.port = port
        self.password = password
        self.redis_client = None
        self.audit_results = {
            "timestamp": datetime.now().isoformat(),
            "requirements": {},
            "current_config": {},
            "gaps": [],
            "recommendations": [],
        }

        # MET-001 Requirements (from phase_1_tickets.md)
        self.met001_requirements = {
            "memory": {
                "maxmemory": "2gb",
                "maxmemory_policy": "allkeys-lru",
                "description": "2GB memory allocation for 8-hour buffer capacity",
            },
            "persistence": {
                "appendonly": "yes",
                "appendfsync": "everysec",
                "auto_aof_rewrite_percentage": "100",
                "auto_aof_rewrite_min_size": "64mb",
                "description": "AOF-only persistence for buffer use case",
            },
            "performance": {
                "tcp_keepalive": "300",
                "timeout": "0",
                "tcp_backlog": "511",
                "databases": "1",
                "save": '""',  # Disable RDB
                "description": "Performance optimization for buffer operations",
            },
            "network": {
                "bind": "127.0.0.1",
                "port": "6379",
                "protected_mode": "yes",
                "description": "Network security configuration",
            },
        }

    def connect_to_redis(self) -> bool:
        """Establish connection to Redis server."""
        try:
            self.redis_client = redis.Redis(
                host=self.host,
                port=self.port,
                password=self.password,
                decode_responses=True,
                socket_connect_timeout=10,
                socket_timeout=10,
            )

            # Test connection
            self.redis_client.ping()
            print(f"✅ Connected to Redis at {self.host}:{self.port}")
            return True

        except redis.ConnectionError as e:
            print(f"❌ Failed to connect to Redis: {e}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error connecting to Redis: {e}")
            return False

    def get_redis_config(self) -> Dict[str, Any]:
        """Extract current Redis configuration."""
        try:
            # Get all configuration parameters
            config = self.redis_client.config_get("*")

            # Get additional info
            info = self.redis_client.info()

            # Get memory info
            memory_info = self.redis_client.info("memory")

            # Get server info
            server_info = self.redis_client.info("server")

            return {
                "config": config,
                "info": info,
                "memory_info": memory_info,
                "server_info": server_info,
            }

        except Exception as e:
            print(f"❌ Error getting Redis configuration: {e}")
            return {}

    def analyze_memory_configuration(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze memory-related configuration."""
        analysis = {
            "maxmemory": {
                "current": config.get("config", {}).get("maxmemory", "Not set"),
                "required": self.met001_requirements["memory"]["maxmemory"],
                "status": "Unknown",
            },
            "maxmemory_policy": {
                "current": config.get("config", {}).get("maxmemory-policy", "Not set"),
                "required": self.met001_requirements["memory"]["maxmemory_policy"],
                "status": "Unknown",
            },
            "used_memory": config.get("memory_info", {}).get(
                "used_memory_human", "Unknown"
            ),
            "maxmemory_human": config.get("memory_info", {}).get(
                "maxmemory_human", "Unknown"
            ),
            "memory_fragmentation_ratio": config.get("memory_info", {}).get(
                "mem_fragmentation_ratio", "Unknown"
            ),
        }

        # Normalize and check maxmemory in bytes to avoid string-format mismatch
        current_max_bytes = self._parse_memory_to_bytes(
            str(analysis["maxmemory"]["current"])
        )
        required_max_bytes = self._parse_memory_to_bytes(
            str(analysis["maxmemory"]["required"])
        )

        if current_max_bytes == required_max_bytes and current_max_bytes is not None:
            analysis["maxmemory"]["status"] = "✅ Correct"
        elif analysis["maxmemory"]["current"] == "Not set":
            analysis["maxmemory"]["status"] = "❌ Not configured"
        else:
            analysis["maxmemory"]["status"] = "⚠️ Different"

        # Check maxmemory-policy
        if (
            analysis["maxmemory_policy"]["current"]
            == analysis["maxmemory_policy"]["required"]
        ):
            analysis["maxmemory_policy"]["status"] = "✅ Correct"
        elif analysis["maxmemory_policy"]["current"] == "Not set":
            analysis["maxmemory_policy"]["status"] = "❌ Not configured"
        else:
            analysis["maxmemory_policy"]["status"] = "⚠️ Different"

        return analysis

    @staticmethod
    def _parse_memory_to_bytes(value: str) -> int | None:
        """Convert human-readable memory sizes (e.g., '2gb', '64mb', '1024') to bytes.

        Returns None if parsing fails.
        """
        try:
            v = value.strip().lower()
            if v in ("not set", "unknown", ""):
                return None
            # If purely numeric, assume bytes
            if v.isdigit():
                return int(v)
            units = {
                "b": 1,
                "kb": 1024,
                "k": 1024,
                "mb": 1024**2,
                "m": 1024**2,
                "gb": 1024**3,
                "g": 1024**3,
                "tb": 1024**4,
                "t": 1024**4,
            }
            # split numeric prefix and unit suffix
            num_part = ""
            unit_part = ""
            for ch in v:
                if ch.isdigit() or ch == ".":
                    num_part += ch
                else:
                    unit_part += ch
            num = float(num_part) if num_part else 0.0
            unit_part = unit_part.strip()
            multiplier = units.get(unit_part, None)
            if multiplier is None:
                return None
            return int(num * multiplier)
        except Exception:
            return None

    def analyze_persistence_configuration(
        self, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Analyze persistence-related configuration."""
        analysis = {
            "appendonly": {
                "current": config.get("config", {}).get("appendonly", "Not set"),
                "required": self.met001_requirements["persistence"]["appendonly"],
                "status": "Unknown",
            },
            "appendfsync": {
                "current": config.get("config", {}).get("appendfsync", "Not set"),
                "required": self.met001_requirements["persistence"]["appendfsync"],
                "status": "Unknown",
            },
            "auto_aof_rewrite_percentage": {
                "current": config.get("config", {}).get(
                    "auto-aof-rewrite-percentage", "Not set"
                ),
                "required": self.met001_requirements["persistence"][
                    "auto_aof_rewrite_percentage"
                ],
                "status": "Unknown",
            },
            "auto_aof_rewrite_min_size": {
                "current": config.get("config", {}).get(
                    "auto-aof-rewrite-min-size", "Not set"
                ),
                "required": self.met001_requirements["persistence"][
                    "auto_aof_rewrite_min_size"
                ],
                "status": "Unknown",
            },
            "aof_enabled": config.get("info", {}).get("aof_enabled", "Unknown"),
            "aof_rewrite_in_progress": config.get("info", {}).get(
                "aof_rewrite_in_progress", "Unknown"
            ),
            "aof_current_size": config.get("info", {}).get(
                "aof_current_size", "Unknown"
            ),
            "aof_base_size": config.get("info", {}).get("aof_base_size", "Unknown"),
        }

        # Check each setting
        for key in [
            "appendonly",
            "appendfsync",
            "auto_aof_rewrite_percentage",
            "auto_aof_rewrite_min_size",
        ]:
            if analysis[key]["current"] == analysis[key]["required"]:
                analysis[key]["status"] = "✅ Correct"
            elif analysis[key]["current"] == "Not set":
                analysis[key]["status"] = "❌ Not configured"
            else:
                analysis[key]["status"] = "⚠️ Different"

        return analysis

    def analyze_performance_configuration(
        self, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Analyze performance-related configuration."""
        analysis = {
            "tcp_keepalive": {
                "current": config.get("config", {}).get("tcp-keepalive", "Not set"),
                "required": self.met001_requirements["performance"]["tcp_keepalive"],
                "status": "Unknown",
            },
            "timeout": {
                "current": config.get("config", {}).get("timeout", "Not set"),
                "required": self.met001_requirements["performance"]["timeout"],
                "status": "Unknown",
            },
            "tcp_backlog": {
                "current": config.get("config", {}).get("tcp-backlog", "Not set"),
                "required": self.met001_requirements["performance"]["tcp_backlog"],
                "status": "Unknown",
            },
            "databases": {
                "current": config.get("config", {}).get("databases", "Not set"),
                "required": self.met001_requirements["performance"]["databases"],
                "status": "Unknown",
            },
            "save": {
                "current": config.get("config", {}).get("save", "Not set"),
                "required": self.met001_requirements["performance"]["save"],
                "status": "Unknown",
            },
        }

        # Check each setting
        for key in ["tcp_keepalive", "timeout", "tcp_backlog", "databases", "save"]:
            if analysis[key]["current"] == analysis[key]["required"]:
                analysis[key]["status"] = "✅ Correct"
            elif analysis[key]["current"] == "Not set":
                analysis[key]["status"] = "❌ Not configured"
            else:
                analysis[key]["status"] = "⚠️ Different"

        return analysis

    def identify_gaps(
        self,
        memory_analysis: Dict,
        persistence_analysis: Dict,
        performance_analysis: Dict,
    ) -> List[str]:
        """Identify configuration gaps against MET-001 requirements."""
        gaps = []

        # Check memory configuration
        if memory_analysis["maxmemory"]["status"] != "✅ Correct":
            gaps.append(
                f"Memory limit: {memory_analysis['maxmemory']['status']} - Current: {memory_analysis['maxmemory']['current']}, Required: {memory_analysis['maxmemory']['required']}"
            )

        if memory_analysis["maxmemory_policy"]["status"] != "✅ Correct":
            gaps.append(
                f"Memory policy: {memory_analysis['maxmemory_policy']['status']} - Current: {memory_analysis['maxmemory_policy']['current']}, Required: {memory_analysis['maxmemory_policy']['required']}"
            )

        # Check persistence configuration
        for key in [
            "appendonly",
            "appendfsync",
            "auto_aof_rewrite_percentage",
            "auto_aof_rewrite_min_size",
        ]:
            if persistence_analysis[key]["status"] != "✅ Correct":
                gaps.append(
                    f"Persistence {key}: {persistence_analysis[key]['status']} - Current: {persistence_analysis[key]['current']}, Required: {persistence_analysis[key]['required']}"
                )

        # Check performance configuration
        for key in ["tcp_keepalive", "timeout", "tcp_backlog", "databases", "save"]:
            if performance_analysis[key]["status"] != "✅ Correct":
                gaps.append(
                    f"Performance {key}: {performance_analysis[key]['status']} - Current: {performance_analysis[key]['current']}, Required: {performance_analysis[key]['required']}"
                )

        return gaps

    def generate_recommendations(self, gaps: List[str]) -> List[str]:
        """Generate optimization recommendations based on gaps."""
        recommendations = []

        if not gaps:
            recommendations.append(
                "✅ All MET-001 requirements are met. Consider performance tuning for optimal buffer operations."
            )
        else:
            recommendations.append(
                "🔧 Configuration updates needed to meet MET-001 requirements:"
            )
            for gap in gaps:
                recommendations.append(f"  - {gap}")

            recommendations.append("")
            recommendations.append("📋 Recommended actions:")
            recommendations.append("  1. Update redis.conf with MET-001 specifications")
            recommendations.append("  2. Restart Redis to apply new configuration")
            recommendations.append("  3. Verify configuration changes took effect")
            recommendations.append("  4. Test performance under load")

        return recommendations

    def run_audit(self) -> Dict[str, Any]:
        """Run the complete configuration audit."""
        print("🔍 Redis Configuration Audit")
        print("=" * 50)

        # Connect to Redis
        if not self.connect_to_redis():
            return self.audit_results

        # Get current configuration
        print("\n📊 Extracting current Redis configuration...")
        current_config = self.get_redis_config()

        if not current_config:
            print("❌ Failed to extract Redis configuration")
            return self.audit_results

        # Analyze configurations
        print("\n🔍 Analyzing memory configuration...")
        memory_analysis = self.analyze_memory_configuration(current_config)

        print("🔍 Analyzing persistence configuration...")
        persistence_analysis = self.analyze_persistence_configuration(current_config)

        print("🔍 Analyzing performance configuration...")
        performance_analysis = self.analyze_performance_configuration(current_config)

        # Identify gaps
        print("\n🔍 Identifying configuration gaps...")
        gaps = self.identify_gaps(
            memory_analysis, persistence_analysis, performance_analysis
        )

        # Generate recommendations
        print("\n💡 Generating recommendations...")
        recommendations = self.generate_recommendations(gaps)

        # Compile results
        self.audit_results.update(
            {
                "requirements": self.met001_requirements,
                "current_config": current_config,
                "analysis": {
                    "memory": memory_analysis,
                    "persistence": persistence_analysis,
                    "performance": performance_analysis,
                },
                "gaps": gaps,
                "recommendations": recommendations,
            }
        )

        return self.audit_results

    def print_report(self, results: Dict[str, Any]):
        """Print a formatted audit report."""
        print("\n" + "=" * 60)
        print("📋 REDIS CONFIGURATION AUDIT REPORT")
        print("=" * 60)
        print(f"Timestamp: {results['timestamp']}")
        print(
            f"Redis Version: {results['current_config'].get('server_info', {}).get('redis_version', 'Unknown')}"
        )
        print(
            f"Connected Clients: {results['current_config'].get('info', {}).get('connected_clients', 'Unknown')}"
        )

        # Memory Analysis
        print("\n🧠 MEMORY CONFIGURATION")
        print("-" * 30)
        memory = results["analysis"]["memory"]
        print(
            f"Max Memory: {memory['maxmemory']['status']} ({memory['maxmemory']['current']})"
        )
        print(
            f"Memory Policy: {memory['maxmemory_policy']['status']} ({memory['maxmemory_policy']['current']})"
        )
        print(f"Used Memory: {memory['used_memory']}")
        print(f"Max Memory Human: {memory['maxmemory_human']}")
        print(f"Fragmentation Ratio: {memory['memory_fragmentation_ratio']}")

        # Persistence Analysis
        print("\n💾 PERSISTENCE CONFIGURATION")
        print("-" * 30)
        persistence = results["analysis"]["persistence"]
        print(
            f"Append Only: {persistence['appendonly']['status']} ({persistence['appendonly']['current']})"
        )
        print(
            f"Append Fsync: {persistence['appendfsync']['status']} ({persistence['appendfsync']['current']})"
        )
        print(f"AOF Enabled: {persistence['aof_enabled']}")
        print(f"AOF Current Size: {persistence['aof_current_size']}")
        print(f"AOF Base Size: {persistence['aof_base_size']}")

        # Performance Analysis
        print("\n⚡ PERFORMANCE CONFIGURATION")
        print("-" * 30)
        performance = results["analysis"]["performance"]
        print(
            f"TCP Keepalive: {performance['tcp_keepalive']['status']} ({performance['tcp_keepalive']['current']})"
        )
        print(
            f"Timeout: {performance['timeout']['status']} ({performance['timeout']['current']})"
        )
        print(
            f"TCP Backlog: {performance['tcp_backlog']['status']} ({performance['tcp_backlog']['current']})"
        )
        print(
            f"Databases: {performance['databases']['status']} ({performance['databases']['current']})"
        )
        print(
            f"Save: {performance['save']['status']} ({performance['save']['current']})"
        )

        # Gaps
        print("\n⚠️ CONFIGURATION GAPS")
        print("-" * 30)
        if results["gaps"]:
            for gap in results["gaps"]:
                print(f"• {gap}")
        else:
            print("✅ No configuration gaps identified")

        # Recommendations
        print("\n💡 RECOMMENDATIONS")
        print("-" * 30)
        for recommendation in results["recommendations"]:
            print(recommendation)

        print("\n" + "=" * 60)

    def save_report(self, results: Dict[str, Any], filename: str = None):
        """Save the audit report to a JSON file."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"redis_config_audit_{timestamp}.json"

        filepath = os.path.join(os.path.dirname(__file__), filename)

        try:
            with open(filepath, "w") as f:
                json.dump(results, f, indent=2)
            print(f"\n💾 Audit report saved to: {filepath}")
        except Exception as e:
            print(f"\n❌ Failed to save audit report: {e}")


def main():
    """Main function to run the Redis configuration audit."""
    print("🚀 Redis Configuration Audit for Bluesky Data Pipeline")
    print("=" * 60)

    # Get Redis connection parameters from environment
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_password = os.getenv("REDIS_PASSWORD")

    # Create auditor
    auditor = RedisConfigurationAuditor(
        host=redis_host, port=redis_port, password=redis_password
    )

    # Run audit
    results = auditor.run_audit()

    # Print report
    auditor.print_report(results)

    # Save report
    auditor.save_report(results)

    # Return exit code based on gaps
    if results["gaps"]:
        print("\n❌ Configuration gaps found. Review recommendations above.")
        return 1
    else:
        print("\n✅ Configuration audit passed. All MET-001 requirements met.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
