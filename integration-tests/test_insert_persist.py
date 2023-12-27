import lib
import os
import pytest
@pytest.fixture(autouse=True)
def run_before_and_after():
    try:
        os.remove("dbfile.db") 
    except:
        pass
    yield
    os.remove("dbfile.db") 

def test_insert_persist():
    outs = lib.db(["insert 1 user1 person", "insert 2 user2 person", ".exit"])
    outs = lib.db(["select", ".exit"])
    assert outs == ['sqlite>(1, user1, person)', '(2, user2, person)', 'sqlite>']

