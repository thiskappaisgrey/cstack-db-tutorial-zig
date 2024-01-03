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
    print(outs)
    # assert outs == ['sqlite>Executed.', 'sqlite>Executed.', 'sqlite>Executed.', 'sqlite>Executed.', 'sqlite>Executed.', 'sqlite>Executed.', 'sqlite>Executed.', 'sqlite>Executed.', 'sqlite>Executed.', 'sqlite>Executed.', 'sqlite>Executed.', 'sqlite>Executed.', 'sqlite>Executed.', 'sqlite>Executed.', 'sqlite>']
test_insert_split()
