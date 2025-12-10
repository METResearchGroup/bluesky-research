"""Manages batch exporting of cache data to persistent storage."""

from typing import TypedDict
from lib.log.logger import get_logger

from services.sync.stream.exporters.study_user_exporter import StudyUserActivityExporter
from services.sync.stream.exporters.in_network_exporter import (
    InNetworkUserActivityExporter,
)
from services.sync.stream.cache_management import CacheDirectoryManager, FileUtilities


logger = get_logger(__file__)


class BatchExportResult(TypedDict):
    """Result of batch export operation."""

    study_user_filepaths: list[str]
    in_network_filepaths: list[str]


class BatchExporter:
    """Orchestrates batch export of cache data to persistent storage."""

    def __init__(
        self,
        study_user_exporter: StudyUserActivityExporter,
        in_network_exporter: InNetworkUserActivityExporter,
        directory_manager: CacheDirectoryManager,
        file_utilities: FileUtilities,
        clear_filepaths: bool = False,
        clear_cache: bool = True,
    ):
        """Initialize batch exporter.

        Args:
            study_user_exporter: Exporter for study user activity
            in_network_exporter: Exporter for in-network user activity
            directory_manager: Manager for cache directory lifecycle
            file_utilities: File I/O utilities for file operations
            clear_filepaths: If True, delete processed files after export
            clear_cache: If True, delete cache after export (rebuilds structure)
        """
        self.study_user_exporter = study_user_exporter
        self.in_network_exporter = in_network_exporter
        self.directory_manager = directory_manager
        self.file_utilities = file_utilities
        self.clear_filepaths = clear_filepaths
        self.clear_cache = clear_cache

    def export_batch(self) -> BatchExportResult:
        """Export all cached data to persistent storage.

        Returns:
            Dict with keys 'study_user_filepaths' and 'in_network_filepaths'
        """
        logger.info("Starting batch export...")

        # Export study user activity
        logger.info("Exporting study user activity data...")
        try:
            study_user_filepaths = self.study_user_exporter.export_activity_data()
            logger.info(
                f"Exported {len(study_user_filepaths)} study user activity files."
            )
        except Exception as e:
            logger.error(f"Error exporting study user activity: {e}")
            raise

        # Export in-network user activity
        logger.info("Exporting in-network user activity data...")
        try:
            in_network_filepaths = self.in_network_exporter.export_activity_data()
            logger.info(
                f"Exported {len(in_network_filepaths)} in-network user activity files."
            )
        except Exception as e:
            logger.error(f"Error exporting in-network user activity: {e}")
            raise

        # Optionally clear processed files
        if self.clear_filepaths:
            logger.info(
                f"Clearing {len(study_user_filepaths + in_network_filepaths)} processed files..."
            )
            all_filepaths = study_user_filepaths + in_network_filepaths
            self.file_utilities.delete_files(all_filepaths)
            logger.info("Cleared processed files.")

        # Optionally clear and rebuild cache
        if self.clear_cache:
            logger.info("Clearing cache and rebuilding structure...")
            self.directory_manager.delete_all()
            self.directory_manager.rebuild_all()
            logger.info("Cache cleared and rebuilt.")

        logger.info("Batch export completed successfully.")

        return BatchExportResult(
            study_user_filepaths=study_user_filepaths,
            in_network_filepaths=in_network_filepaths,
        )


def export_batch(
    batch_exporter: BatchExporter | None = None,
    clear_filepaths: bool = False,
    clear_cache: bool = True,
) -> BatchExportResult:
    """Public API for batch export.

    Args:
        batch_exporter: BatchExporter instance. If None, creates one via setup.
        clear_filepaths: If True, delete processed files after export
        clear_cache: If True, delete cache after export (rebuilds structure)

    Returns:
        BatchExportResult with processed filepaths
    """
    if batch_exporter is None:
        from services.sync.stream.setup import setup_batch_export_system

        batch_exporter = setup_batch_export_system()

    # Update exporter configuration
    batch_exporter.clear_filepaths = clear_filepaths
    batch_exporter.clear_cache = clear_cache
    return batch_exporter.export_batch()
