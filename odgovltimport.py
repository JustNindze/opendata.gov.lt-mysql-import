# coding: utf-8

from __future__ import print_function
from __future__ import unicode_literals

import re
import string
import logging

import unidecode
import sqlalchemy as sa

from ckan.plugins import toolkit
from ckan.logic import NotFound


logger = logging.getLogger(__name__)


def slugify(title=None, length=100):
    if not title:
        return ''

    # Replace all non-ascii characters to ascii equivalents.
    slug = unidecode.unidecode(title)

    # Make slug.
    slug = str(re.sub(r'[^\w\s-]', '', slug).strip().lower())
    slug = re.sub(r'[-\s]+', '-', slug)

    # Make sure, that slug is not longer that specied in `length`.
    begining_chars = length // 5
    if len(slug) > length:
        words = slug.split('-')
        a, b = [], []
        while words and len('-'.join(a + b)) < length:
            if len('-'.join(a)) <= (len('-'.join(b)) + begining_chars):
                a.append(words.pop(0))
            else:
                b.insert(0, words.pop())
        if b:
            slug = '-'.join(a) + '--' + '-'.join(b)
        else:
            slug = '-'.join(a)

    return slug[:length + begining_chars]


def tagify(tag):
    spl = re.split(r'\W+', tag, flags=re.UNICODE)
    return ' '.join(spl)


def fixcase(value):
    if len(value) > 1 and value[:2].isalpha() and value[0].isupper() and value[1].islower():
        return value[0].lower() + value[1:]
    else:
        return value


class CkanAPI(object):
    """Wrapper around CKAN API actions.

    See: http://docs.ckan.org/en/latest/api/index.html#action-api-reference
    """

    def __getattr__(self, name):
        return lambda context={}, **kwargs: toolkit.get_action(name)(context, kwargs)


SOURCE_ID_KEY = 'Šaltinio ID'
CODE_KEY = 'Kodas'
ADDRESS_KEY = 'Adresas'


def get_package_tags(r_zodziai):
    if r_zodziai:
        tags = map(fixcase, map(string.strip, r_zodziai.replace(';', ',').split(',')))
        for tag in filter(None, tags):
            name = tagify(tag).lower()

            if len(name) > 100:
                logger.warn("skip very long tag: %r", tag)
            else:
                yield {
                    'name': name,
                }


class CkanSync(object):

    def __init__(self, ckanapi, db, conn):
        self.api = ckanapi
        self.db = db
        self.conn = conn
        self.execute = conn.execute

        class Tables(object):
            user = db.tables['t_user']
            istaiga = db.tables['t_istaiga']
            rinkmena = db.tables['t_rinkmena']

        self.t = Tables

        self.importbot = self.sync_importbot_user()

    def sync_importbot_user(self):
        from ckan import model

        username = 'importbot'
        user = model.User.by_name(username)

        if not user:
            # TODO: How to deal with passwords?
            user = model.User(name=username, password='secret')
            user.sysadmin = True
            model.Session.add(user)
            model.repo.commit_and_remove()

        return self.api.user_show(id=username)

    def sync_user(self, user_id):
        user = self.execute(
            sa.select([self.t.user]).where(self.t.user.c.ID == user_id)
        ).fetchone()

        if user:
            user_data = {
                'name': user.LOGIN,
                # TODO: Passwrods are encoded with md5 hash, I need to look if
                #       CKAN supports md5 hashed passwords. If not, then users will
                #       have to change their passwords.
                'email': user.EMAIL,
                'password': user.PASS,
                'fullname': ' '.join([user.FIRST_NAME, user.LAST_NAME]),
            }
        else:
            user_data = {
                'name': 'unknown',
                # TODO: What email should I use?
                'email': 'unknown@example.com',
                # TODO: Maybe dynamically generate a password?
                'password': 'secret',
                'fullname': 'Unknown User',
            }

        ckan_user = next((
            u for u in self.api.user_list(q=user_data['name'])
            if u['name'] == user_data['name']
        ), None)

        if ckan_user is None:
            context = {'user': self.importbot['name']}
            ckan_user = self.api.user_create(context, **user_data)

        user_data['id'] = ckan_user['id']

        return user_data

    def sync_organization(self, istaiga_id):
        organization = self.execute(
            sa.select([self.t.istaiga]).where(self.t.istaiga.c.ID == istaiga_id)
        ).fetchone()

        if organization:
            organization_data = {
                # PAVADINIMAS
                'name': slugify(organization.PAVADINIMAS),
                'title': organization.PAVADINIMAS,

                'state': 'active',

                'extras': [
                    # ID
                    {'key': SOURCE_ID_KEY, 'value': organization.ID},

                    # KODAS
                    {'key': CODE_KEY, 'value': organization.KODAS},

                    # ADRESAS
                    {'key': ADDRESS_KEY, 'value': organization.ADRESAS},
                ],
            }
        else:
            organization_data = {
                'name': 'unknown',
                'title': 'Unknown organization',
                'state': 'active',
            }

        try:
            ckan_organization = self.api.organization_show(id=organization_data['name'])
        except NotFound:
            ckan_organization = None

        if ckan_organization is None:
            context = {'user': self.importbot['name']}
            ckan_organization = self.api.organization_create(context, **organization_data)

        organization_data['id'] = ckan_organization['id']

        return organization_data

    def sync_datasets(self):
        existing_datasets = {}
        for name in self.api.package_list():
            ds = self.api.package_show(id=name)
            import_source_id = next((x['value'] for x in ds['extras'] if x['key'] == SOURCE_ID_KEY), None)
            if import_source_id:
                existing_datasets[int(import_source_id)] = ds

        for row in self.execute(sa.select([self.t.rinkmena])):
            user = self.sync_user(row.USER_ID)
            organization = self.sync_organization(row.istaiga_id)
            self.api.organization_member_create(
                {'user': self.importbot['name']},
                id=organization['name'],
                username=user['name'],
                role='editor',
            )

            context = {
                'user': user['name'],
            }

            if row.ID not in existing_datasets:
                self.api.package_create(
                    context,

                    # PAVADINIMAS
                    name=slugify(row.PAVADINIMAS),
                    title=row.PAVADINIMAS,

                    # SANTRAUKA
                    notes=row.SANTRAUKA,

                    # TINKLAPIS
                    url=row.TINKLAPIS,

                    # USER_ID
                    maintainer=user['fullname'],

                    # K_EMAIL
                    maintainer_email=row.K_EMAIL,

                    # istaiga_id
                    owner_org=organization['name'],

                    # R_ZODZIAI
                    tags=list(get_package_tags(row.R_ZODZIAI)),

                    private=False,
                    state='active',
                    type='dataset',

                    extras=[
                        # ID
                        {'key': SOURCE_ID_KEY, 'value': row.ID},

                        # KODAS
                        {'key': CODE_KEY, 'value': row.KODAS},
                    ],
                )


class OpenDataGovLtCommand(toolkit.CkanCommand):
    """Synchronize old MySQL data with new CKAN database.

    Usage:

        paster --plugin=odgovlt-mysql-import odgovltsync -c ../deployment/ckan/development.ini

    """

    summary = __doc__.splitlines()[0]
    usage = __doc__

    def command(self):
        self._load_config()

        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

        ckanapi = CkanAPI()
        print(ckanapi.status_show())

        engine = sa.create_engine('mysql+pymysql://sirex:@localhost/rinkmenos?charset=utf8')
        db = sa.MetaData()
        db.reflect(bind=engine)
        conn = engine.connect()

        sync = CkanSync(ckanapi, db, conn)
        sync.sync_datasets()
