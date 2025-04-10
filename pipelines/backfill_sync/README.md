# Backfill Sync Pipeline

## Overview
The backfill sync pipeline is designed to give us an up-to-date database of records from Bluesky by syncing missing records from the Bluesky PDSes. It ensures that all necessary data is backfilled and up-to-date, allowing for consistent and reliable data access across the system.

## Purpose
The primary purpose of the backfill sync is to process and synchronize user data records, either by backfilling all records or determining which records need to be backfilled based on a specified date range.

## Handler Functionality
The main functionality of the backfill sync pipeline is controlled by the `handler.py` file. This file contains the `lambda_handler` function, which serves as the entry point for the pipeline's operations.

### Lambda Handler
- **Modes of Operation**: The handler supports two modes: `backfill` and `determine_dids_to_backfill`.
  - **Backfill Mode**: In this mode, the handler processes a list of Decentralized Identifiers (DIDs) to synchronize their data records. It can handle all users or a specified list of DIDs.
  - **Determine DIDs to Backfill Mode**: This mode is used to identify which DIDs need backfilling based on a given date range.

- **Process Flow**:
  1. **Event Handling**: The handler begins by checking the event input to determine the mode of operation and the DIDs to process.
  2. **DID Processing**: It utilizes the `process_dids` function to parse and prepare the list of DIDs for synchronization.
  3. **Backfill Records**: For the `backfill` mode, it constructs a payload and calls the `backfill_records` function to perform the synchronization.
  4. **Determine DIDs**: In the `determine_dids_to_backfill` mode, it calls the `determine_dids_to_backfill` function to identify the necessary DIDs based on the date range.
  5. **Error Handling**: The handler includes robust error handling to log and report any issues encountered during the process.

Most of the underlying functionality is handled in `services/backfill/sync`.

This README provides a comprehensive overview of the backfill sync pipeline, detailing its purpose, functionality, and the operations performed within the handler.
