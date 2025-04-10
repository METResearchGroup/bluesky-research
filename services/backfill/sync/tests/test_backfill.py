"""Tests for backfill.py."""

import gc
import json
import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock, call
import multiprocessing

from services.backfill.sync.backfill import (
    get_plc_directory_doc,
    identify_post_type,
    identify_record_type,
    validate_record_timestamp,
    transform_backfilled_record,
    get_bsky_records_for_user,
    do_backfill_for_user,
    assign_default_backfill_synctimestamp,
    do_backfill_for_users,
    do_backfill_for_users_parallel,
    run_batched_backfill,
)
from services.backfill.sync.constants import (
    default_start_timestamp,
    default_end_timestamp,
    valid_types,
    default_batch_size,
)
from lib.constants import timestamp_format


class TestGetPlcDirectoryDoc:
    """Tests for get_plc_directory_doc function.
    
    This class tests fetching PLC directory documents for DIDs, verifying:
    - Proper URL construction
    - Handling of API responses
    - JSON conversion
    """
    
    @pytest.fixture
    def mock_requests_get(self):
        """Mock requests.get function."""
        with patch("requests.get") as mock:
            mock_response = Mock()
            mock_response.json.return_value = {
                "@context": [
                    "https://www.w3.org/ns/did/v1",
                    "https://w3id.org/security/multikey/v1",
                    "https://w3id.org/security/suites/secp256k1-2019/v1"
                ],
                "alsoKnownAs": ["at://markptorres.bsky.social"],
                "id": "did:plc:w5mjarupsl6ihdrzwgnzdh4y",
                "service": [
                    {
                        "id": "#atproto_pds",
                        "serviceEndpoint": "https://puffball.us-east.host.bsky.network",
                        "type": "AtprotoPersonalDataServer"
                    }
                ],
                "verificationMethod": [
                    {
                        "controller": "did:plc:w5mjarupsl6ihdrzwgnzdh4y",
                        "id": "did:plc:w5mjarupsl6ihdrzwgnzdh4y#atproto",
                        "publicKeyMultibase": "zQ3shrqW7PgHYsfsXrhz4i5eXEUAgWdkpZrqK2gsB1ZBSd9NY",
                        "type": "Multikey"
                    }
                ]
            }
            mock.return_value = mock_response
            yield mock
    
    def test_get_plc_directory_doc_success(self, mock_requests_get):
        """Test successful retrieval of PLC directory document.
        
        Should make a GET request to the correct URL and return the parsed JSON.
        """
        did = "did:plc:w5mjarupsl6ihdrzwgnzdh4y"
        result = get_plc_directory_doc(did)
        
        # Verify the correct URL was used
        mock_requests_get.assert_called_once_with(f"https://plc.directory/{did}")
        
        # Verify result structure
        assert isinstance(result, dict)
        assert result["id"] == did
        assert "service" in result
        assert len(result["service"]) > 0
        assert "serviceEndpoint" in result["service"][0]


class TestIdentifyPostType:
    """Tests for identify_post_type function.
    
    This class tests identification of post types (standalone or reply), verifying:
    - Correct classification of posts with or without reply fields
    """
    
    def test_identify_post_type_standalone(self):
        """Test identification of standalone post.
        
        Should return "post" when there's no reply field.
        """
        post = {"text": "This is a standalone post"}
        result = identify_post_type(post)
        assert result == "post"
    
    def test_identify_post_type_reply(self):
        """Test identification of reply post.
        
        Should return "reply" when there's a reply field.
        """
        post = {
            "text": "This is a reply",
            "reply": {
                "root": {"uri": "at://original-post-uri"},
                "parent": {"uri": "at://parent-post-uri"}
            }
        }
        result = identify_post_type(post)
        assert result == "reply"


class TestIdentifyRecordType:
    """Tests for identify_record_type function.
    
    This class tests identification of record types, verifying:
    - Correct extraction of record type from $type field
    - Special handling for post types (normal post vs. reply)
    """
    
    @patch("services.backfill.sync.backfill.identify_post_type")
    def test_identify_record_type_post(self, mock_identify_post_type):
        """Test identification of post record type.
        
        Should call identify_post_type for post records.
        """
        mock_identify_post_type.return_value = "post"
        record = {"$type": "app.bsky.feed.post"}
        result = identify_record_type(record)
        
        mock_identify_post_type.assert_called_once_with(record)
        assert result == "post"
    
    @patch("services.backfill.sync.backfill.identify_post_type")
    def test_identify_record_type_reply(self, mock_identify_post_type):
        """Test identification of reply record type.
        
        Should call identify_post_type for post records that are replies.
        """
        mock_identify_post_type.return_value = "reply"
        record = {
            "$type": "app.bsky.feed.post",
            "reply": {
                "root": {"uri": "at://original-post-uri"},
                "parent": {"uri": "at://parent-post-uri"}
            }
        }
        result = identify_record_type(record)
        
        mock_identify_post_type.assert_called_once_with(record)
        assert result == "reply"
    
    def test_identify_record_type_non_post(self):
        """Test identification of non-post record type.
        
        Should extract the last part of the $type field for non-post records.
        """
        record = {"$type": "app.bsky.feed.like"}
        result = identify_record_type(record)
        assert result == "like"


class TestValidateRecordTimestamp:
    """Tests for validate_record_timestamp function.
    
    This class tests validation of record timestamps, verifying:
    - Records within time range are accepted
    - Records outside time range are rejected
    """
    
    @patch("services.backfill.sync.backfill.convert_bsky_dt_to_pipeline_dt")
    def test_validate_record_timestamp_within_range(self, mock_convert):
        """Test validation of timestamp within range.
        
        Should return True for timestamps within the specified range.
        """
        mock_convert.return_value = "2024-10-15-12:00:00"
        record = {"createdAt": "2024-10-15T12:00:00Z"}
        
        result = validate_record_timestamp(
            record,
            start_timestamp="2024-09-27-00:00:00",
            end_timestamp="2024-12-02-00:00:00"
        )
        
        mock_convert.assert_called_once_with(record["createdAt"])
        assert result is True
    
    @patch("services.backfill.sync.backfill.convert_bsky_dt_to_pipeline_dt")
    def test_validate_record_timestamp_before_range(self, mock_convert):
        """Test validation of timestamp before range.
        
        Should return False for timestamps before the specified range.
        """
        mock_convert.return_value = "2024-09-26-23:59:59"
        record = {"createdAt": "2024-09-26T23:59:59Z"}
        
        result = validate_record_timestamp(
            record,
            start_timestamp="2024-09-27-00:00:00",
            end_timestamp="2024-12-02-00:00:00"
        )
        
        mock_convert.assert_called_once_with(record["createdAt"])
        assert result is False
    
    @patch("services.backfill.sync.backfill.convert_bsky_dt_to_pipeline_dt")
    def test_validate_record_timestamp_after_range(self, mock_convert):
        """Test validation of timestamp after range.
        
        Should return False for timestamps after the specified range.
        """
        mock_convert.return_value = "2024-12-02-00:00:01"
        record = {"createdAt": "2024-12-02T00:00:01Z"}
        
        result = validate_record_timestamp(
            record,
            start_timestamp="2024-09-27-00:00:00",
            end_timestamp="2024-12-02-00:00:00"
        )
        
        mock_convert.assert_called_once_with(record["createdAt"])
        assert result is False
    
    @patch("services.backfill.sync.backfill.convert_bsky_dt_to_pipeline_dt")
    def test_validate_record_timestamp_default_range(self, mock_convert):
        """Test validation with default timestamp range.
        
        Should use default constants when no range is specified.
        """
        mock_convert.return_value = "2024-10-15-12:00:00"
        record = {"createdAt": "2024-10-15T12:00:00Z"}
        
        result = validate_record_timestamp(record)
        
        mock_convert.assert_called_once_with(record["createdAt"])
        assert result is True


class TestTransformBackfilledRecord:
    """Tests for transform_backfilled_record function.
    
    This class tests transformation of backfilled records, verifying:
    - Record type and synctimestamp are added
    - Timestamps outside study range are properly handled
    """
    
    @patch("services.backfill.sync.backfill.convert_bsky_dt_to_pipeline_dt")
    def test_transform_backfilled_record_in_range(self, mock_convert):
        """Test transformation of record within study range.
        
        Should add record_type and synctimestamp fields without modification.
        """
        mock_convert.return_value = "2024-10-15-12:00:00"
        record = {"createdAt": "2024-10-15T12:00:00Z", "text": "Test post."}
        record_type = "post"
        
        result = transform_backfilled_record(
            record,
            record_type,
            "2024-09-27-00:00:00",
            "2024-12-02-00:00:00"
        )
        
        assert result["record_type"] == record_type
        assert result["synctimestamp"] == "2024-10-15-12:00:00"
    
    @patch("services.backfill.sync.backfill.convert_bsky_dt_to_pipeline_dt")
    @patch("services.backfill.sync.backfill.validate_record_timestamp")
    @patch("services.backfill.sync.backfill.assign_default_backfill_synctimestamp")
    def test_transform_backfilled_record_outside_range(
        self, mock_assign_default, mock_validate, mock_convert
    ):
        """Test transformation of record outside study range.
        
        Should assign a default synctimestamp for records outside the study range.
        """
        mock_convert.return_value = "2024-08-15-12:00:00"
        mock_validate.return_value = False
        mock_assign_default.return_value = "2024-08-15-00:00:00"
        
        record = {"createdAt": "2024-08-15T12:00:00Z", "text": "Test post."}
        record_type = "post"
        
        result = transform_backfilled_record(
            record,
            record_type,
            "2024-09-27-00:00:00",
            "2024-12-02-00:00:00"
        )
        
        mock_validate.assert_called_once()
        mock_assign_default.assert_called_once_with(synctimestamp="2024-08-15-12:00:00")
        assert result["record_type"] == record_type
        assert result["synctimestamp"] == "2024-08-15-00:00:00"


class TestGetBskyRecordsForUser:
    """Tests for get_bsky_records_for_user function.
    
    This class tests fetching Bluesky records for a user, verifying:
    - Proper construction of API request URL
    - Processing of CAR file content
    - Handling of error responses
    """
    
    @pytest.fixture
    def mock_plc_doc(self):
        """Mock get_plc_directory_doc function."""
        with patch("services.backfill.sync.backfill.get_plc_directory_doc") as mock:
            mock.return_value = {
                "service": [
                    {
                        "serviceEndpoint": "https://puffball.us-east.host.bsky.network"
                    }
                ]
            }
            yield mock
    
    @pytest.fixture
    def mock_requests_get(self):
        """Mock requests.get function."""
        with patch("requests.get") as mock:
            mock_response = Mock()
            mock_response.content = b"mock_content"
            mock_response.status_code = 200
            mock.return_value = mock_response
            yield mock
    
    @pytest.fixture
    def mock_car(self):
        """Mock CAR.from_bytes function."""
        with patch("atproto.CAR.from_bytes") as mock:
            mock_car = Mock()
            mock_car.blocks = {
                "block1": {"$type": "app.bsky.feed.post", "text": "Test post 1"},
                "block2": {"$type": "app.bsky.feed.like"}
            }
            mock.return_value = mock_car
            yield mock
    
    def test_get_bsky_records_for_user(self, mock_plc_doc, mock_requests_get, mock_car):
        """Test fetching Bluesky records for a user.
        
        Should retrieve PLC document, construct correct URL, and process CAR file.
        """
        did = "did:plc:test"
        result = get_bsky_records_for_user(did)
        
        # Verify PLC document was fetched
        mock_plc_doc.assert_called_once_with(did)

        # Verify request was made with correct URL
        expected_url = "https://puffball.us-east.host.bsky.network/xrpc/com.atproto.sync.getRepo?did=did:plc:test"
        mock_requests_get.assert_called_once_with(expected_url)
        
        # Verify CAR file was processed
        mock_car.assert_called_once_with(b"mock_content")
        
        # Verify result
        assert len(result) == 2
        assert all(isinstance(item, dict) for item in result)
    
    def test_get_bsky_records_for_user_error_response(self, mock_plc_doc, mock_requests_get, mock_car, caplog):
        """Test handling of error response when fetching Bluesky records.
        
        Should log error details and return empty list when status code is not 200.
        """
        # Configure mock to return a 400 error
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.content = b'{"error": "Bad Request"}'
        mock_requests_get.return_value = mock_response
        
        did = "did:plc:test"
        result = get_bsky_records_for_user(did)
        
        # Verify PLC document was fetched
        mock_plc_doc.assert_called_once_with(did)
        
        # Verify request was made
        mock_requests_get.assert_called_once()
        
        # Verify CAR.from_bytes was not called
        mock_car.assert_not_called()

        # Verify empty list was returned
        assert result == []


class TestDoBackfillForUser:
    """Tests for do_backfill_for_user function.
    
    This class tests backfilling for a single user, verifying:
    - Records are fetched, validated, and transformed
    - Records are properly grouped by type
    - Count statistics are accurately tracked
    """
    
    @pytest.fixture
    def mock_get_records(self):
        """Mock get_bsky_records_for_user function."""
        with patch("services.backfill.sync.backfill.get_bsky_records_for_user") as mock:
            mock.return_value = [
                {"$type": "app.bsky.feed.post", "createdAt": "2024-10-15T12:00:00Z"},
                {"$type": "app.bsky.feed.like", "createdAt": "2024-10-16T12:00:00Z"},
                {"$type": "app.bsky.graph.follow", "createdAt": "2024-08-01T12:00:00Z"},
                {"$type": "app.bsky.generator", "createdAt": "2024-10-17T12:00:00Z"},
                {
                    "$type": "app.bsky.feed.post",
                    "reply": {"root": {"uri": "test"}},
                    "createdAt": "2024-10-18T12:00:00Z"
                }
            ]
            yield mock
    
    @pytest.fixture
    def mock_transform(self):
        """Mock transform_backfilled_record function."""
        with patch("services.backfill.sync.backfill.transform_backfilled_record") as mock:
            def side_effect(record, record_type, start_timestamp, end_timestamp):
                record = record.copy()
                record["record_type"] = record_type
                record["synctimestamp"] = "2024-10-15-12:00:00"
                return record
            mock.side_effect = side_effect
            yield mock
    
    @pytest.fixture
    def mock_plc_doc(self):
        """Mock get_plc_directory_doc function."""
        with patch("services.backfill.sync.backfill.get_plc_directory_doc") as mock:
            mock.return_value = {
                "alsoKnownAs": ["at://test.bsky.social"],
                "service": [{"serviceEndpoint": "https://test-pds.bsky.app"}]
            }
            yield mock
    
    @pytest.fixture
    def mock_create_metadata(self):
        """Mock create_user_backfill_metadata function."""
        with patch("services.backfill.sync.backfill.create_user_backfill_metadata") as mock:
            mock.return_value = MagicMock()
            yield mock
    
    def test_do_backfill_for_user(
        self, mock_get_records, mock_transform, mock_plc_doc, mock_create_metadata
    ):
        """Test backfilling for a single user.
        
        Should fetch, validate, transform, and group records by type.
        """
        did = "did:plc:test"
        counts, records, metadata = do_backfill_for_user(
            did,
            "2024-09-27-00:00:00",
            "2024-12-02-00:00:00"
        )
        
        # Verify records were fetched
        mock_get_records.assert_called_once_with(did)
        
        # Verify PLC doc was fetched to get handle and PDS endpoint
        mock_plc_doc.assert_called_once_with(did)
        
        # Verify count statistics
        assert counts == {"post": 1, "like": 1, "follow": 1, "reply": 1}
        
        # Verify records are grouped by type
        assert set(records.keys()) == {"post", "like", "follow", "reply"}
        assert len(records["post"]) == 1
        assert len(records["like"]) == 1
        assert len(records["follow"]) == 1
        assert len(records["reply"]) == 1
        
        # Verify each record was transformed
        assert mock_transform.call_count == 4
        for record_list in records.values():
            for record in record_list:
                assert "record_type" in record
                assert "synctimestamp" in record
        
        # Verify metadata was created
        mock_create_metadata.assert_called_once_with(
            did=did,
            record_count_map=counts,
            bluesky_handle="test.bsky.social",
            pds_service_endpoint="https://test-pds.bsky.app"
        )
        assert metadata == mock_create_metadata.return_value


class TestAssignDefaultBackfillSynctimestamp:
    """Tests for assign_default_backfill_synctimestamp function.
    
    This class tests assignment of default synctimestamps, verifying:
    - Correct timestamp assignment based on day of month
    - Proper handling of month and year boundaries
    - Time component is reset to 00:00:00
    """
    
    def test_assign_default_before_mid_month(self):
        """Test assignment for date before mid-month.
        
        Should assign the 15th of the same month.
        """
        synctimestamp = "2024-10-05-12:34:56"
        result = assign_default_backfill_synctimestamp(synctimestamp)
        
        # Should be assigned to the 15th of the same month at 00:00:00
        expected = "2024-10-15-00:00:00"
        assert result == expected
    
    def test_assign_default_after_mid_month(self):
        """Test assignment for date after mid-month.
        
        Should assign the 1st of the next month.
        """
        synctimestamp = "2024-10-16-12:34:56"
        result = assign_default_backfill_synctimestamp(synctimestamp)
        
        # Should be assigned to the 1st of the next month at 00:00:00
        expected = "2024-11-01-00:00:00"
        assert result == expected
    
    def test_assign_default_december(self):
        """Test assignment for date in December.
        
        Should handle year boundary correctly.
        """
        synctimestamp = "2024-12-16-12:34:56"
        result = assign_default_backfill_synctimestamp(synctimestamp)
        
        # Should be assigned to the 1st of January of the next year at 00:00:00
        expected = "2025-01-01-00:00:00"
        assert result == expected


class TestDoBackfillForUsers:
    """Tests for do_backfill_for_users function.
    
    This class tests backfilling for multiple users, verifying:
    - Backfill is performed for each user
    - Results are properly aggregated
    - Rate limiting is applied
    """
    
    @pytest.fixture
    def mock_do_backfill_for_user(self):
        """Mock do_backfill_for_user function."""
        with patch("services.backfill.sync.backfill.do_backfill_for_user") as mock:
            # Return different results for different users
            def side_effect(did, start_timestamp, end_timestamp):
                metadata = MagicMock()
                if did == "did:plc:user1":
                    return (
                        {"post": 2, "like": 1},
                        {
                            "post": [{"text": "Test post 1"}, {"text": "Test post 2"}],
                            "like": [{"uri": "test:like:1"}]
                        },
                        metadata
                    )
                else:
                    return (
                        {"post": 1, "follow": 1},
                        {
                            "post": [{"text": "Test post 3"}],
                            "follow": [{"subject": "did:plc:other"}]
                        },
                        metadata
                    )
            mock.side_effect = side_effect
            yield mock
    
    def test_do_backfill_for_users(self, mock_do_backfill_for_user):
        """Test backfilling for multiple users.
        
        Should process each user and aggregate results correctly.
        """
        dids = ["did:plc:user1", "did:plc:user2"]
        
        counts_map, records_map, metadata_list = do_backfill_for_users(
            dids,
            "2024-09-27-00:00:00",
            "2024-12-02-00:00:00"
        )
        
        # Verify backfill was performed for each user
        assert mock_do_backfill_for_user.call_count == 2
        mock_do_backfill_for_user.assert_has_calls([
            call("did:plc:user1", start_timestamp="2024-09-27-00:00:00", end_timestamp="2024-12-02-00:00:00"),
            call("did:plc:user2", start_timestamp="2024-09-27-00:00:00", end_timestamp="2024-12-02-00:00:00")
        ])
        
        # Verify counts map
        assert counts_map == {
            "did:plc:user1": {"post": 2, "like": 1},
            "did:plc:user2": {"post": 1, "follow": 1}
        }
        
        # Verify records map
        assert set(records_map.keys()) == {"post", "like", "follow"}
        assert len(records_map["post"]) == 3
        assert len(records_map["like"]) == 1
        assert len(records_map["follow"]) == 1
        
        # Verify metadata list
        assert len(metadata_list) == 2
    
    def test_do_backfill_for_users_default_timestamps(self, mock_do_backfill_for_user):
        """Test backfilling with default timestamps.
        
        Should use default constants when no timestamps are specified.
        """
        dids = ["did:plc:user1"]
        
        do_backfill_for_users(dids)
        
        # Verify default timestamps were used
        mock_do_backfill_for_user.assert_called_once_with(
            "did:plc:user1",
            start_timestamp=default_start_timestamp,
            end_timestamp=default_end_timestamp
        )


class TestRunBatchedBackfill:
    """Tests for run_batched_backfill function.
    
    This class tests batch processing of backfill operations, verifying:
    - DIDs are properly batched
    - Backfill is performed for each batch
    - Records are written to cache after each batch
    - Metadata is aggregated correctly
    """
    
    @pytest.fixture
    def mock_create_batches(self):
        """Mock create_batches function."""
        with patch("services.backfill.sync.backfill.create_batches") as mock:
            mock.return_value = [
                ["did:plc:user1", "did:plc:user2"],
                ["did:plc:user3"]
            ]
            yield mock
    
    @pytest.fixture
    def mock_do_backfill_for_users(self):
        """Mock do_backfill_for_users function."""
        with patch("services.backfill.sync.backfill.do_backfill_for_users") as mock:
            # Return different results for different batches
            def side_effect(dids, start_timestamp, end_timestamp):
                user_metadata = [MagicMock() for _ in dids]
                if "did:plc:user1" in dids:
                    return (
                        {
                            "did:plc:user1": {"post": 2},
                            "did:plc:user2": {"like": 1}
                        },
                        {
                            "post": [{"text": "Post 1"}, {"text": "Post 2"}],
                            "like": [{"uri": "like:1"}]
                        },
                        user_metadata
                    )
                else:
                    return (
                        {"did:plc:user3": {"follow": 1}},
                        {"follow": [{"subject": "other:user"}]},
                        user_metadata
                    )
            mock.side_effect = side_effect
            yield mock
    
    @pytest.fixture
    def mock_write_records(self):
        """Mock write_records_to_cache function."""
        with patch("services.backfill.sync.backfill.write_records_to_cache") as mock:
            yield mock
    
    @pytest.fixture
    def mock_gc(self):
        """Mock gc.collect function."""
        with patch("gc.collect") as mock:
            yield mock
    
    @pytest.fixture
    def mock_logger(self):
        """Mock logger."""
        with patch("services.backfill.sync.backfill.logger") as mock:
            yield mock
    
    @pytest.fixture
    def mock_track_performance(self):
        """Mock track_performance decorator."""
        with patch("services.backfill.sync.backfill.track_performance") as mock:
            # Make track_performance decorator a no-op
            mock.return_value = lambda f: f
            yield mock
    
    def test_run_batched_backfill(
        self,
        mock_create_batches,
        mock_do_backfill_for_users,
        mock_write_records,
        mock_gc,
        mock_logger,
        mock_track_performance
    ):
        """Test batch processing of backfill operations.
        
        Should process each batch, write to cache, and aggregate metadata.
        """
        dids = ["did:plc:user1", "did:plc:user2", "did:plc:user3"]
        batch_size = 2
        
        result = run_batched_backfill(
            dids,
            batch_size,
            "2024-09-27-00:00:00",
            "2024-12-02-00:00:00",
            run_parallel=False # parallel runs work in prod, but unit testing is annoyingly hard, so we skip that.
        )

        # Verify batches were created
        mock_create_batches.assert_called_once_with(batch_list=dids, batch_size=batch_size)
        
        # Verify backfill was performed for each batch
        assert mock_do_backfill_for_users.call_count == 2
        mock_do_backfill_for_users.assert_has_calls([
            call(
                dids=["did:plc:user1", "did:plc:user2"],
                start_timestamp="2024-09-27-00:00:00",
                end_timestamp="2024-12-02-00:00:00"
            ),
            call(
                dids=["did:plc:user3"],
                start_timestamp="2024-09-27-00:00:00",
                end_timestamp="2024-12-02-00:00:00"
            )
        ])
        
        # Verify records were written to cache after each batch
        assert mock_write_records.call_count == 2
        mock_write_records.assert_has_calls([
            call(
                type_to_record_maps={
                    "post": [{"text": "Post 1"}, {"text": "Post 2"}],
                    "like": [{"uri": "like:1"}]
                },
                batch_size=batch_size
            ),
            call(
                type_to_record_maps={"follow": [{"subject": "other:user"}]},
                batch_size=batch_size
            )
        ])
        
        # Verify garbage collection was performed after each batch
        assert mock_gc.call_count == 2
        
        # Verify logging
        assert mock_logger.info.call_count >= 2

        # Verify result structure
        assert result["total_batches"] == 2
        assert result["did_to_backfill_counts_map"] == {
            "did:plc:user1": {"post": 2},
            "did:plc:user2": {"like": 1},
            "did:plc:user3": {"follow": 1}
        }
        assert result["total_processed_users"] == 3
        assert result["total_users"] == 3
        assert "user_backfill_metadata" in result
        assert len(result["user_backfill_metadata"]) == 3
    
    def test_run_batched_backfill_default_parameters(
        self,
        mock_create_batches,
        mock_do_backfill_for_users,
        mock_write_records,
        mock_gc,
        mock_logger,
        mock_track_performance
    ):
        """Test batch processing with default parameters.
        
        Should use default values when parameters are not specified.
        """
        dids = ["did:plc:user1", "did:plc:user2", "did:plc:user3"]
        
        result = run_batched_backfill(
            dids,
            run_parallel=False # parallel runs work in prod, but unit testing is annoyingly hard, so we skip that.
        )

        # Verify batches were created with default batch size
        mock_create_batches.assert_called_once_with(batch_list=dids, batch_size=default_batch_size)

        # Verify backfill was performed for each batch with default timestamps
        assert mock_do_backfill_for_users.call_count == 2
        mock_do_backfill_for_users.assert_has_calls([
            call(
                dids=["did:plc:user1", "did:plc:user2"],
                start_timestamp=None,
                end_timestamp=None
            ),
            call(
                dids=["did:plc:user3"],
                start_timestamp=None,
                end_timestamp=None
            )
        ])
        
        # Verify result structure includes new fields
        assert "total_processed_users" in result
        assert "total_users" in result
        assert "user_backfill_metadata" in result
    
    def test_run_batched_backfill_with_parallel_flag(
        self,
        mock_create_batches,
        mock_do_backfill_for_users,
        mock_write_records,
        mock_gc,
        mock_logger,
        mock_track_performance
    ):
        """Test batch processing with parallel flag.
        
        Should use appropriate function based on run_parallel flag.
        """
        dids = ["did:plc:user1", "did:plc:user2", "did:plc:user3"]
        batch_size = 2
        
        # Test parallel execution
        with patch("services.backfill.sync.backfill.do_backfill_for_users_parallel") as mock_parallel:
            mock_parallel.return_value = (
                {"did:plc:user1": {"post": 1}},
                {"post": [{"text": "test"}]},
                [MagicMock()]
            )
            
            result_parallel = run_batched_backfill(
                dids,
                batch_size=batch_size,
                start_timestamp="2024-09-27-00:00:00",
                end_timestamp="2024-12-02-00:00:00",
                run_parallel=True
            )
            
            # Verify parallel function was used
            assert mock_parallel.call_count > 0
            assert mock_do_backfill_for_users.call_count == 0
        
        # Test sequential execution
        mock_do_backfill_for_users.reset_mock()
        with patch("services.backfill.sync.backfill.do_backfill_for_users_parallel") as mock_parallel:
            result_sequential = run_batched_backfill(
                dids,
                batch_size=batch_size,
                start_timestamp="2024-09-27-00:00:00",
                end_timestamp="2024-12-02-00:00:00",
                run_parallel=False
            )
            
            # Verify sequential function was used
            assert mock_parallel.call_count == 0
            assert mock_do_backfill_for_users.call_count > 0 