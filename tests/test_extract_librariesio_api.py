"""Tests for extract_librariesio_api.py.

    Test partially written by github copilot. - JH


Covers:
- Pure logic: parse_owner_repo
- Settings path resolution (Phase 1)
- I/O functions with tmp_path: load_repos, load_already_fetched, _save_batch (Phase 3)
- HTTP layer via mocked requests: fetch_repo_metadata
- No-stdout assertions: verify logger is used instead of print() (Phase 4)
"""
import requests as req
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from capstone.extraction.extract_librariesio_api import (
    _save_batch,
    fetch_repo_metadata,
    load_already_fetched,
    load_repos,
    load_repos_from_unique_packages,
    parse_owner_repo,
)
from settings import load_settings


# ---------------------------------------------------------------------------
# parse_owner_repo — pure logic, no I/O
# ---------------------------------------------------------------------------


class TestParseOwnerRepo:
    def test_valid_github_url(self):
        assert parse_owner_repo("github.com/numpy/numpy") == ("numpy", "numpy")

    def test_valid_org_with_hyphen(self):
        assert parse_owner_repo("github.com/my-org/my-repo") == ("my-org", "my-repo")

    def test_valid_simple(self):
        assert parse_owner_repo("github.com/owner/repo") == ("owner", "repo")

    def test_invalid_missing_repo(self):
        with pytest.raises(ValueError):
            parse_owner_repo("github.com/only-owner")

    def test_invalid_empty_owner(self):
        with pytest.raises(ValueError):
            parse_owner_repo("github.com//repo")

    def test_invalid_empty_repo(self):
        with pytest.raises(ValueError):
            parse_owner_repo("github.com/owner/")


# ---------------------------------------------------------------------------
# Settings path properties (Phase 1 regression guard)
# ---------------------------------------------------------------------------


class TestSettings:
    def test_librariesio_parquet_path_resolves(self):
        s = load_settings()
        assert s.librariesio_parquet_path.endswith("librariesio/librariesio.parquet")

    def test_pypi_scorecards_path_resolves(self):
        s = load_settings()
        assert s.pypi_scorecards_path.endswith("source_data/pypi_scorecards")

    def test_librariesio_parquet_path_is_under_librariesio_dir(self):
        s = load_settings()
        assert s.librariesio_parquet_path.startswith(s.librariesio_path)


# ---------------------------------------------------------------------------
# load_repos — file I/O with tmp_path
# ---------------------------------------------------------------------------


class TestLoadRepos:
    def test_returns_unique_repos(self, tmp_path):
        pl.DataFrame({"repo_name": ["github.com/a/b", "github.com/c/d"]}).write_parquet(tmp_path / "f1.parquet")
        pl.DataFrame({"repo_name": ["github.com/a/b", "github.com/e/f"]}).write_parquet(tmp_path / "f2.parquet")
        repos = load_repos(str(tmp_path))
        assert len(repos) == 3

    def test_raises_if_no_parquet_files(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_repos(str(tmp_path))

    def test_no_stdout_output(self, tmp_path, capsys):
        """After refactoring, load_repos must not write to stdout (uses logger)."""
        pl.DataFrame({"repo_name": ["github.com/a/b"]}).write_parquet(tmp_path / "f1.parquet")
        load_repos(str(tmp_path))
        assert capsys.readouterr().out == ""


class TestLoadReposFromUniquePackages:
    def test_returns_unique_repos(self, tmp_path):
        path = tmp_path / "unique_packages.parquet"
        pl.DataFrame({"github_repo": ["github.com/a/b", "github.com/c/d", "github.com/a/b"]}).write_parquet(path)
        repos = load_repos_from_unique_packages(str(path))
        assert set(repos) == {"github.com/a/b", "github.com/c/d"}

    def test_raises_if_file_missing(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_repos_from_unique_packages(str(tmp_path / "missing.parquet"))

    def test_no_stdout_output(self, tmp_path, capsys):
        """After refactoring, load_repos_from_unique_packages must not write to stdout (uses logger)."""
        path = tmp_path / "unique_packages.parquet"
        pl.DataFrame({"github_repo": ["github.com/a/b"]}).write_parquet(path)
        load_repos_from_unique_packages(str(path))
        assert capsys.readouterr().out == ""


# ---------------------------------------------------------------------------
# load_already_fetched — file I/O with tmp_path
# ---------------------------------------------------------------------------


class TestLoadAlreadyFetched:
    def test_returns_empty_set_if_file_missing(self, tmp_path):
        result = load_already_fetched(str(tmp_path / "nonexistent.parquet"))
        assert result == set()

    def test_returns_lowercased_names(self, tmp_path):
        out = tmp_path / "existing.parquet"
        pl.DataFrame({"full_name": ["Owner/Repo", "Other/Lib"]}).write_parquet(out)
        result = load_already_fetched(str(out))
        assert result == {"owner/repo", "other/lib"}

    def test_returns_set_type(self, tmp_path):
        out = tmp_path / "existing.parquet"
        pl.DataFrame({"full_name": ["a/b"]}).write_parquet(out)
        assert isinstance(load_already_fetched(str(out)), set)


# ---------------------------------------------------------------------------
# _save_batch — file I/O with tmp_path
# ---------------------------------------------------------------------------

_NULL_COLS = [
    "description", "fork", "created_at", "updated_at", "pushed_at", "size",
    "stargazers_count", "language", "has_issues", "has_wiki", "has_pages",
    "forks_count", "open_issues_count", "default_branch", "subscribers_count",
    "license", "contributions_count", "has_readme", "has_changelog",
    "has_contributing", "has_license", "has_coc", "has_threat_model", "has_audit",
    "status", "rank", "github_contributions_count", "github_id",
]


def _make_row(full_name: str) -> dict:
    return {col: None for col in _NULL_COLS} | {"full_name": full_name}


class TestSaveBatch:
    def test_creates_file_on_first_write(self, tmp_path):
        out = str(tmp_path / "out.parquet")
        _save_batch([_make_row("a/b")], out, set())
        df = pl.read_parquet(out)
        assert len(df) == 1
        assert df["full_name"][0] == "a/b"

    def test_appends_to_existing_file(self, tmp_path):
        out = str(tmp_path / "out.parquet")
        _save_batch([_make_row("a/b")], out, set())
        _save_batch([_make_row("c/d")], out, {"a/b"})
        df = pl.read_parquet(out)
        assert len(df) == 2
        assert set(df["full_name"].to_list()) == {"a/b", "c/d"}

    def test_no_stdout_output(self, tmp_path, capsys):
        """After refactoring, _save_batch must not write to stdout (uses logger)."""
        out = str(tmp_path / "out.parquet")
        _save_batch([_make_row("a/b")], out, set())
        assert capsys.readouterr().out == ""


# ---------------------------------------------------------------------------
# fetch_repo_metadata — mocked HTTP
# ---------------------------------------------------------------------------


class TestFetchRepoMetadata:
    def test_success_returns_json(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"full_name": "owner/repo", "rank": 10}
        with patch("requests.get", return_value=mock_resp):
            result = fetch_repo_metadata("owner", "repo", "fake-key", max_retries=1)
        assert result == {"full_name": "owner/repo", "rank": 10}

    def test_404_returns_none(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_resp.text = "not found"
        with patch("requests.get", return_value=mock_resp):
            result = fetch_repo_metadata("owner", "repo", "fake-key", max_retries=1)
        assert result is None

    def test_timeout_exhausted_returns_none(self):
        with patch("requests.get", side_effect=req.exceptions.Timeout):
            with patch("time.sleep"):  # avoid slowing tests down
                result = fetch_repo_metadata("owner", "repo", "fake-key", max_retries=2)
        assert result is None

    def test_ssl_error_exhausted_returns_none(self):
        with patch("requests.get", side_effect=req.exceptions.SSLError("ssl eof")):
            with patch("time.sleep"):  # avoid slowing tests down
                result = fetch_repo_metadata("owner", "repo", "fake-key", max_retries=2)
        assert result is None

    def test_connection_error_exhausted_returns_none(self):
        with patch("requests.get", side_effect=req.exceptions.ConnectionError("conn reset")):
            with patch("time.sleep"):  # avoid slowing tests down
                result = fetch_repo_metadata("owner", "repo", "fake-key", max_retries=2)
        assert result is None

    def test_no_stdout_on_http_error(self, capsys):
        """After refactoring, HTTP errors must not write to stdout (uses logger)."""
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.text = "server error"
        with patch("requests.get", return_value=mock_resp):
            fetch_repo_metadata("owner", "repo", "fake-key", max_retries=1)
        assert capsys.readouterr().out == ""

    def test_no_stdout_on_timeout(self, capsys):
        """After refactoring, timeouts must not write to stdout (uses logger)."""
        with patch("requests.get", side_effect=req.exceptions.Timeout):
            with patch("time.sleep"):
                fetch_repo_metadata("owner", "repo", "fake-key", max_retries=1)
        assert capsys.readouterr().out == ""
