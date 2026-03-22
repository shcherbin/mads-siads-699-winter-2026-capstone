"""Shared pytest configuration.

Sets the minimum required environment variables so that `load_settings()` succeeds
in test runs without a real .env file being present.
"""
import os

os.environ.setdefault("MADS_CAPSTONE_ENV", "test")
os.environ.setdefault("MADS_CAPSTONE_VERSION", "0.0.0")
