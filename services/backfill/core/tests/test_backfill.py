"""Tests for backfill.py."""

import pytest
from unittest.mock import Mock, patch, MagicMock, call

from services.backfill.core.backfill import (
    get_plc_directory_doc, get_bsky_records_for_user
)


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
        with patch("services.backfill.core.backfill.get_plc_directory_doc") as mock:
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
