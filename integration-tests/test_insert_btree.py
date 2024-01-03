import lib
import pytest
import os

@pytest.fixture(autouse=True)
def run_before_and_after():
    try:
        os.remove("dbfile.db") 
    except:
        pass
    yield
    os.remove("dbfile.db") 

def test_insert_split():
    cs = list(map(lambda i: f"insert {i} user#{i} person#{i}@example.com", range(1, 15))) + [".exit"]
    outs = lib.db(cs, True)
    assert outs == ['sqlite>Executed. Key is: 1', 'sqlite>Executed. Key is: 2', 'sqlite>Executed. Key is: 3', 'sqlite>Executed. Key is: 4', 'sqlite>Executed. Key is: 5', 'sqlite>Executed. Key is: 6', 'sqlite>Executed. Key is: 7', 'sqlite>Executed. Key is: 8', 'sqlite>Executed. Key is: 9', 'sqlite>Executed. Key is: 10', 'sqlite>Executed. Key is: 11', 'sqlite>Executed. Key is: 12', 'sqlite>Executed. Key is: 13', 'sqlite>Executed. Key is: 14', 'sqlite>']
# test_insert_split()
