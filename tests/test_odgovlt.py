from ckanext.harvest.harvesters.ckanharvester import CKANHarvester
from ckanext.harvest.harvesters.ckanharvester import DatetimeEncoder
from datetime import datetime
import json
from sqlalchemy import create_engine
from sqlalchemy import MetaData


def test_info():
    ckan_object = CKANHarvester()
    info = ckan_object.info()
    assert isinstance(info, dict)
    assert isinstance(info['name'], str)
    assert isinstance(info['title'], str)
    assert isinstance(info['description'], str)
    assert isinstance(info['form_config_interface'], str)


def test_DatetimeEncoder():
    dictionary_data1 = {
            'test': 'test',
            'test1': datetime(1977, 2, 19),
            'test2': 'data',
            'test3': datetime(3089, 8, 1),
            'test4': 'name',
            'test5': 'datetime',
            'test6': datetime(1900, 9, 30)}
    dictionary_data2 = {
            'test': 'test',
            'test1': datetime(1977, 2, 19).strftime('%Y-%m-%dT%H:%M:%S'),
            'test2': 'data',
            'test3': datetime(3089, 8, 1).strftime('%Y-%m-%dT%H:%M:%S'),
            'test4': 'name',
            'test5': 'datetime',
            'test6': datetime(1900, 9, 30).strftime('%Y-%m-%dT%H:%M:%S')}
    json_data1 = json.dumps(dictionary_data1, cls=DatetimeEncoder)
    json_data2 = json.dumps(dictionary_data2)
    dictionary_data3 = json.loads(json_data1)
    assert isinstance(dictionary_data3['test1'], unicode)
    assert isinstance(dictionary_data3['test3'], unicode)
    assert isinstance(dictionary_data3['test6'], unicode)
    assert json_data1 == json_data2


def test_gather_stage(mocker):
    ckan_object = CKANHarvester()
    HarvestObject = mocker.patch('ckanext.harvest.harvesters.\
ckanharvester.HarvestObject')
    mocker.patch('ckanext.harvest.harvesters.ckanharvester.config')
    mocker.patch('ckanext.harvest.harvesters.ckanharvester.create_engine')
    mocker.patch('ckanext.harvest.harvesters.ckanharvester.MetaData')
    assert isinstance(ckan_object.gather_stage(HarvestObject), list)


def test_fetch_stage(mocker):
    ckan_object = CKANHarvester()
    HarvestObject = mocker.patch('ckanext.harvest.harvesters.\
ckanharvester.HarvestObject')
    assert ckan_object.fetch_stage(HarvestObject)


def test_import_stage():
    con = create_engine('postgresql://ckan_default:OwkOyWpWvGYHYdv\
LAiQOe7W7DIPGVPpG@localhost/ckan_default')
    meta = MetaData(bind=con, reflect=True)
    harvest_object_error = meta.tables['harvest_object_error']
    clause = harvest_object_error.select()
    errors = []
    for row in con.execute(clause):
        errors.append(row)
    assert not errors
