# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from sqlalchemy import create_engine
from sqlalchemy import MetaData
from ckanext.harvest.model import HarvestObject
import logging
from ckanext.harvest.harvesters.base import HarvesterBase
import json
from datetime import datetime

log = logging.getLogger(__name__)


class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            try:
                return obj.strftime('%Y-%m-%dT%H:%M:%S')
            except ValueError:
                # strftime gives ValueError for 0000-00-00 00:00:00 datetimes.
                return None
        else:
            return super(DatetimeEncoder, obj).default(obj)


class OdgovltHarvester(HarvesterBase):

    def _connect_to_database(self, dburi):
        con = create_engine(dburi)
        meta = MetaData(bind=con, reflect=True)
        return con, meta

    def info(self):
        return {
            'name': 'opendata-gov-lt',
            'title': 'opendata.gov.lt',
            'description': 'Harvest opendata.gov.lt',
            'form_config_interface': 'Text'
        }

    def gather_stage(self, harvest_job):
        log.debug('In opendatagov gather_stage')
        con, meta = self._connect_to_database(harvest_job.source.url)

        class Tables(object):
            rinkmena = meta.tables['t_rinkmena']

        clause = Tables.rinkmena.select()
        ids = []
        database_data = dict()
        for row in con.execute(clause):
            for row_name in row.keys():
                database_data[row_name] = row[row_name]
            id = database_data['ID']
            obj = HarvestObject(
                         guid=id,
                         job=harvest_job,
                         content=json.dumps(
                              database_data,
                              cls=DatetimeEncoder))
            obj.save()
            ids.append(obj.id)
        return ids

    def fetch_stage(self, harvest_object):
        return True

    def import_stage(self, harvest_object):
        data_to_import = json.loads(harvest_object.content)
        package_dict = {
            'id': harvest_object.guid,
            'title': data_to_import['PAVADINIMAS'],
            'notes': data_to_import['SANTRAUKA'],
        }
        return self._create_or_update_package(package_dict, harvest_object)
