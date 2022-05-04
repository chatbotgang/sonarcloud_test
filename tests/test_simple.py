import pytest
from simple import simple_func

class TestSimple:
  def test_simple(self):
    assert simple_func(3) == 6
