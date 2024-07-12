from main import dumpJson, getData, getSeasonStats
import pytest

#testing dumpJson inputs
def test_JsonDump1():
    bob = 5
    assert dumpJson(bob, 3) is False
    
def test_JsonDump2():
    assert dumpJson("test1.json", 3) is True

def test_JsonDump3():
    assert dumpJson(False, "hi") is False

#testing getSeasonStats inputs
def test_SeasonStats1():
    assert getSeasonStats(2023, 'bob') is None

def test_SeasonStats2():
    assert getSeasonStats(2023, 8) is None

#testing getData
def test_getData():
    assert getData("thisisafailure") is None