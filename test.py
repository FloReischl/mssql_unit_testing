
import pytest
from mrtest.db_methods import *

class TestFoo:
    def test_foo(self):
        """very very very very very very long description."""
        pass


if __name__ == '__main__':
    pytest.main([ __file__
        ,'-k', 'test_foo'
        ,'-ra'
    ])
