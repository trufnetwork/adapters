import os
import pytest
from unittest.mock import patch, MagicMock
from github import GithubException

from tsn_adapters.tasks.github import read_repo_csv_file

@pytest.fixture
def mock_github():
    with patch('github.Github') as mock_g:
        yield mock_g

def test_read_repo_csv_file_missing_token():
    with patch('os.getenv', return_value=None):
        with pytest.raises(Exception) as exc_info:
            read_repo_csv_file('owner/repo', 'path/to/file.csv')
        assert "Repository not found (404). It might be private or a GitHub token is missing." in str(exc_info.value)
