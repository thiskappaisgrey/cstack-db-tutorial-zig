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

def test_btree_print():
    outs = lib.db([   "insert 3 user3 person",  "insert 2 user2 person", "insert 1 user1 person", ".btree", ".exit"])
    assert ['sqlite>Executed.', 'sqlite>Executed.', 'sqlite>Executed.', 'sqlite>Tree:', 'leaf (size 3)', '  - 0 : 1', '  - 1 : 2', '  - 2 : 3', 'sqlite>'] == outs
def test_dup_id():
    outs = lib.db([   "insert 1 user1 person", "insert 1 user1 person", ".exit"])
    assert ['sqlite>Executed.', 'sqlite>Could not insert. Error: Duplicate Key', 'sqlite>'] == outs
 

# assert [] == outs
# print(outs)
