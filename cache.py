import sqlalchemy as sa
import datetime
import logging

logging.info('Starting logger for cache')
log = logging.getLogger(__name__)


class Cache(object):

    def __init__(self, db='sqlite:///cache.db'):
        global conn
        global metadata
        global data
        conn = sa.create_engine(db)
        conn.echo = False
        metadata = sa.MetaData(conn)
        metadata.reflect(bind=conn)
        try:
            create_data = sa.Table(
                'data', metadata,
                sa.Column('id', sa.Integer, primary_key=True),
                sa.Column('url', sa.String(40), unique=True),
                sa.Column('date_accessed', sa.DateTime),
                sa.Column('cached_forever', sa.Boolean)
            )
            create_data.create()
        except sa.exc.InvalidRequestError:
            log.exception('Table already exists.')
        data = metadata.tables['data']

    def __contains__(self, key):
        clause = data.select()
        for row in conn.execute(clause):
            if isinstance(key, int) and row['id'] == key:
                return True
            elif row['url'] == key:
                return True
        return False

    def __reset__(self):
        self.time_now = datetime.datetime.now()
        remove = data.delete().where(
             self.time_now - data.c.date_accessed == 0)
        self.res = conn.execute(remove)
        self.res.close()

    def update(self, new_url, cached_forever=False):
        update = data.insert().prefix_with('OR IGNORE')
        self.res = update.execute(
             url=new_url,
             date_accessed=datetime.datetime.now(),
             cached_forever=cached_forever)
        self.res.close()

    def remove_old(self, days=30):
        self.time_now = datetime.datetime.now()
        remove = data.delete().where(
             self.time_now - data.c.date_accessed == days).where(
             data.c.cached_forever is False)
        self.res = conn.execute(remove)
        self.res.close()

    def get(self, key_or_name):
        if isinstance(key_or_name, int):
            clause = data.select().where(data.c.id == key_or_name)
        else:
            clause = data.select().where(data.c.url == key_or_name)
        for row in conn.execute(clause):
            return row

    def get_all(self):
        clause = data.select()
        data_to_return = []
        for row in conn.execute(clause):
            data_to_return.append(dict(row))
        return data_to_return
