import lib
import pytest
import os

# Remove the dbfile before and after each test..
@pytest.fixture(autouse=True)
def run_before_and_after():
    try:
        os.remove("dbfile.db") 
    except:
        pass
    yield
    os.remove("dbfile.db") 
def test_insert():
    outs = lib.db(["insert 1 user1 person@example.com", "select", ".exit"])
    
    assert outs == ['sqlite>Executed.',  "sqlite>(1, user1, person@example.com)",  "sqlite>"] 


def test_table_full():
    cs = list(map(lambda i: f"insert {i} user#{i} person#{i}@example.com", range(1, 1403))) + [".exit"]
    outs = lib.db(cs)
    assert outs[-2] == "sqlite>Could not insert into table"

def test_long_strings():
    long_username = "a"*32
    long_email = "a"*255
    script = [
            f"insert 1 {long_username} {long_email}",
            "select",
            ".exit"
            ]
    outs = lib.db(script)
    assert  [f'sqlite>Executed.',  f"sqlite>(1, {long_username}, {long_email})",  "sqlite>"] == outs

def test_too_long_strings():
    long_username = "a"*33
    long_email = "a"*256
    script = [
            f"insert 1 {long_username} {long_email}",
            "select",
            ".exit"
            ]
    outs = lib.db(script)
    assert  [f'sqlite>String is too long',"sqlite>sqlite>"] == outs
def test_bad_int():
    script = [
            f"insert hello hello hello",
            f"insert -1 hello hello",
            ".exit"
            ]
    outs = lib.db(script)
    assert  [f'sqlite>Could not parse int argument', 'sqlite>Could not parse int argument', "sqlite>"] == outs
# insert 100
# def test_insert_100():
    
# test_exit()
