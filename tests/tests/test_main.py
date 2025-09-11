import pytest
from ec_manager.main import main   # adjust if main.py has functions/classes

def test_sample():
    """Simple test to check pytest works"""
    assert 1 + 1 == 2

def test_main_import():
    """Check if main.py can be imported"""
    assert main is not None
