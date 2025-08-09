#!/usr/bin/env python3
"""
DataWriter Flow Deployment Script

This script deploys the DataWriter flow to Prefect with scheduling and monitoring.

Author: AI Assistant
Date: 2025-08-08
"""

import sys

from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from bluesky_database.backend.prefect.datawriter_flow import datawriter_flow


def deploy_datawriter_flow():
    """Deploy the DataWriter flow with scheduling"""

    # Create deployment
    deployment = Deployment.build_from_flow(
        flow=datawriter_flow,
        name="datawriter-flow",
        version="1.0.0",
        work_pool_name="default",
        schedule=CronSchedule(cron="*/5 * * * *"),  # Every 5 minutes
        parameters={
            "stream_names": [
                "bluesky_posts",
                "bluesky_likes",
                "bluesky_reposts",
                "bluesky_follows",
                "bluesky_blocks",
            ],
            "batch_size": 1000,
            "output_dir": "./data",
            "max_stream_length": 100000,
        },
        tags=["datawriter", "bluesky", "production"],
        description="DataWriter flow for processing Redis Streams to partitioned Parquet files",
    )

    # Apply the deployment
    deployment_id = deployment.apply()

    print("✅ DataWriter flow deployed successfully!")
    print(f"📋 Deployment ID: {deployment_id}")
    print("⏰ Schedule: Every 5 minutes")
    print("🏊 Work Pool: default")
    print("🏷️ Tags: datawriter, bluesky, production")

    return deployment_id


def main():
    """Main function to deploy the flow"""
    print("🚀 Deploying DataWriter Flow to Prefect...")

    try:
        deployment_id = deploy_datawriter_flow()
        print("\n🎉 Deployment completed successfully!")
        print("📊 You can monitor the flow at: http://localhost:4200")
        print(f"📋 Deployment ID: {deployment_id}")

    except Exception as e:
        print(f"❌ Deployment failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
