MAP_SERVICE_TO_SERVICE_TEMPLATE = {
    "repartition_service": """
        [Name]
        repartition_service

        [Details]
        This service is used for repartitioning a service's data. For a given partition_date, it moves the old records to a new location.

        [Parameters]
        1. start_date
        2. end_date
        3. service
        4. new_service_partition_key="preprocessing_timestam"

        [Steps]
        1. For a given partition date, load in the data. Use “load_data_from_local_storage”.
        2. For the given partition date, copy the old data from {integration}/cache/partition_date=<partition_date> to old_{integration}/cache/partition_date=<partition_date>, where {integration} is the old prefix. For example, if "local_prefix": os.path.join(root_local_data_directory, "uris_to_preprocessing_timestam"), then move the old data from os.path.join(root_local_data_directory, "uris_to_preprocessing_timestam", "cache", partition_date=<partition_date>) to os.path.join(root_local_data_directory, "old_uris_to_preprocessing_timestam", "cache", partition_date=<partition_date>) . Create these paths if they don’t exist. This needs to be a copy of the old files.
        3. For the given partition date, verify that the data in the old and new paths are exactly the same. For example, os.path.join(root_local_data_directory, "uris_to_preprocessing_timestam", "cache", partition_date=<partition_date>)  and os.path.join(root_local_data_directory, "old_uris_to_preprocessing_timestam", "cache", partition_date=<partition_date>) are exactly equal (same length, same data, etc).
        4. For the given partition date, write data to the path tmp_{prefix} based on the “local_prefix” key of service_constants.py for that service (e.g., if the "local_prefix": os.path.join(root_local_data_directory, "uris_to_preprocessing_timestam"), then write to os.path.join(root_local_data_directory, "tmp_uris_to_preprocessing_timestam"). Create these paths if they don’t exist.
        5. For the given partition date, verify that the old_{path} and tmp_{path} are the exact same. This should mean we have two copies of the old data.
        6. For the given partition date, once the above is asserted, delete the data in the old master path. For example, delete os.path.join(root_local_data_directory, "uris_to_preprocessing_timestam", "cache", partition_date=<partition_date>) once we know that the same data is in os.path.join(root_local_data_directory, "old_uris_to_preprocessing_timestam", "cache", partition_date=<partition_date>) and os.path.join(root_local_data_directory, "tmp_uris_to_preprocessing_timestam", "cache", partition_date=<partition_date>}).
        7. Then, read the data from os.path.join(root_local_data_directory, "tmp_uris_to_preprocessing_timestam", "cache", partition_date=<partition_date>}). Then, using the new partition key provided, replace the “timestamp_field” in that service’s entry in “MAP_SERVICE_TO_METADATA”. Then, run “export_data_to_local_storage” for that service.
        8. Get the new record counts in the old_{integration} path and the new path, to see the output of the repartitioning. Print this to stdout.
    """
}
