from dataDump import dumpJson, getData, getSeasonStats
import pytest
import unittest
from unittest.mock import patch, Mock


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

#API Mocking for requests functions
@patch('dataDump.requests.get')
def test_getData1(mock_get):
    mock_response = Mock()
    mock_response.status_code = 429
    mock_get.return_value = mock_response
    assert getData("/leagues") is None

@patch('dataDump.requests.get')
def test_getData2(mock_get):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'key': 'value'}
    mock_get.return_value = mock_response
    assert getData("/leagues") == {'key': 'value'}