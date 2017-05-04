import couch
from constants import *
from tornado.escape import json_decode, json_encode, url_escape
from tornado import gen
import functools

class CustomBlockingCouch(couch.BlockingCouch):
    def add_user(self, user):
        try:
            url = self.db_name + '/_security'
            body = json_encode({
                'admins': {
                    'names': [],
                    'roles': []
                },
                'members': {
                    "names": ["client"],
                    "roles": ["reader"]
                }
            })
            r = self._http_put(url, body)
            return(r)
        except:
            raise


    def __getattribute__(self, name):
        try:
            attr = object.__getattribute__(self, name)
        except AttributeError:
            raise AttributeError("'{}' object has no attribute '{}'".format(
                self.__class__.__name__, name))

        if name == 'close'  or name.startswith('_') or not hasattr(
                attr, '__call__'):
            return attr

        def wrapper(clb, *args, **kwargs):
            fn = functools.partial(clb, *args, **kwargs)
            return self.io_loop.run_sync(fn)

        return functools.partial(wrapper, attr)

    @gen.coroutine
    def pull_db(self, source, db_name, continuous=True, filter=None, create_target=False):
        """Replicate changes from a source database to current (target)
        database.
        """

        doc = {
            '_id': source["database"] + '_' + db_name,
            'source': source["host_auth"] + '/' + source["database"],
            'target': self.couch_url + db_name,
            'continuous': continuous,
            'create_target': create_target
        }
        if filter != None:
            doc["filter"] = filter

        body = json_encode(doc)
        try:
            old_doc = yield self._http_get('_replicator/' + source["database"] + '_' + db_name)
            url = '_replicator/{0}?rev={1}'.format(url_escape(old_doc['_id']), old_doc['_rev'])
            r = yield self._http_delete(url)
        except Exception as err:
            pass
        finally:
            try:
                r = yield self._http_post('_replicator', body, request_timeout=120.0)
            except Exception as err:
                r = None
                print(err)

        return r

    @gen.coroutine
    def init_channel_views(self):
        ddoc = {
            "_id": "_design/articles",
            "views": {
                "active": {
                    "map": "function(doc) {if(!doc.frozen&&doc.type=='article'){emit(doc.pubDate, doc._id);}}"
                }
            }
        }
        try:
            result = self.get_doc(ddoc["_id"])
        except couch.NotFound:
            result = self.save_doc(ddoc)
class CustomAsincCouch(couch.AsyncCouch):
    def __init__(self, db_name='', couch_url='http://127.0.0.1:5984/',
                 io_loop=None, **request_args):
        request_args["connect_timeout"] = 240
        request_args["request_timeout"] = 240
        # request_args={connect_timeout = 120, request_timeout = 120}
        super().__init__(db_name=db_name, couch_url=couch_url,
                         io_loop=io_loop, **request_args)



    @gen.coroutine
    def init_channel_views(self):
        ddoc={
            "_id": "_design/articles",
            "views": {
                "articles": {
                    "map": "function(doc) {if(!doc.frozen&&doc.type=='article'){emit(doc.pubDate, doc._id);}}"
                }
            }
        }
        # try:
        #     result = yield self.get_doc(ddoc["_id"])
        # except couch.NotFound:
        try:
            result = yield self.save_doc(ddoc)
        except Exception as err:
            print(err)

    @gen.coroutine
    def purge_doc(self,doc):
        if '_rev' not in doc or '_id' not in doc:
            raise KeyError('Missing id or revision information in doc')
        body = json_encode({doc["_id"]:[doc["_rev"]]})
        url = '{0}/_purge'.format(
            self.db_name)
        r = yield self._http_post(url,body)
        raise gen.Return(r)

    @gen.coroutine
    def add_empty_user(self):
        try:
            url = self.db_name + '/_security'
            body = json_encode({
                'admins': {
                    'names': [],
                    'roles': []
                },
                'members': {
                    "names": ["client"],
                    "roles": ["reader"]
                }
            })
            r = yield self._http_put(url, body)
            raise gen.Return(r)
        except:
            raise

    @gen.coroutine
    def add_user(self, user, pwd, response_doc):
        try:
            result_pwd = None
            users_db = CustomAsincCouch('_users', self.couch_url)  #"http://" + COUCH_USERS_CREDS["creds"] + "@" + COUCH_SERVERS[server_id]["host"])
            try:
                user_doc = yield users_db.get_doc("org.couchdb.user:" + user)
                result = yield users_db.delete_doc(user_doc)
            except couch.NotFound:
                pass
            body = {
                "_id": "org.couchdb.user:" + user,
                "name": user,
                "type": "user",
                "roles": [],
                "password": self.db_name + '_' + pwd
            }
            try:
                result = yield users_db.save_doc(body)
                try:
                    response_doc["r3"] = pwd
                    result = yield self.save_doc(response_doc)
                    result_pwd = pwd
                except:
                    raise
            except couch.Conflict:
                pass
            except Exception:
                raise

            url = self.db_name + '/_security'
            body = json_encode({
                'admins': {
                    'names': [user],
                    'roles': []
                },
                'members': {
                    "names": [user],
                    "roles": ["reader"]
                }
            })
            try:
                r = yield self._http_put(url, body)
            except couch.Conflict:
                pass
        except Exception as err:
            print(err)
            return None
        return result_pwd

    @gen.coroutine
    def copy_channel_to_mobile(self):
        """Replicate changes from a source database to current (target)
        database.
        """
        body = json_encode({
            'source': self.couch_url + self.db_name,
            'target': self.couch_url+self.db_name+'_m',
            'create_target': True,
            'doc_ids':[self.db_name]
        })
        r = yield self._http_post('_replicate', body, request_timeout=120.0)
        body = json_encode({
            'source': self.couch_url + self.db_name,
            'target': self.couch_url + self.db_name + '_50',
            'create_target': True,
            'doc_ids': [self.db_name]
        })
        r1 = yield self._http_post('_replicate', body, request_timeout=120.0)
        body = json_encode({
            'source': self.couch_url + self.db_name,
            'target': self.couch_url + self.db_name + '_m_50',
            'create_target': True,
            'doc_ids': [self.db_name]
        })
        r2 = yield self._http_post('_replicate', body, request_timeout=120.0)
        raise gen.Return(r2)

    @gen.coroutine
    def pull_db(self, source, db_name, continuous=True, filter=None, create_target=False):
        """Replicate changes from a source database to current (target)
        database.
        """
        # Basic
        # cmVwbGljYXRvcjpBcHNVcmNJUT9e

        doc = {
            '_id': source["database"] + '_' + db_name,
            'source': source["host_auth"] + '/' + source["database"],
            'target': self.couch_url + db_name,
            'continuous': continuous,
            'create_target': create_target
        }
        if filter != None:
            doc["filter"] = filter

        body = json_encode(doc)
        try:
            old_doc = yield self._http_get('_replicator/' + source["database"] + '_' + db_name)
            url = '_replicator/{0}?rev={1}'.format(url_escape(old_doc['_id']), old_doc['_rev'])
            r = yield self._http_delete(url)
        except Exception as err:
            pass
        finally:
            try:
                r = yield self._http_post('_replicator', body, request_timeout=120.0)
            except Exception as err:
                r = None
                print(err)

        return r

    @gen.coroutine
    def push_db(self, target, db_name, continuous=True, filter=None, create_target=False):
        """Replicate changes from a source database to current (target)
        database.
        """
        # Basic
        # cmVwbGljYXRvcjpBcHNVcmNJUT9e

        doc = {
            '_id': db_name + '_' + target["database"],
            'target': target["host_auth"] + '/' + target["database"],
            'source': self.couch_url + db_name,
            'continuous': continuous,
            'create_target': create_target
        }

        if filter != None:
            doc["filter"] = filter

        body = json_encode(doc)
        try:
            old_doc = yield self._http_get('_replicator/' + db_name + '_' + target["database"])
            url = '_replicator/{0}?rev={1}'.format(url_escape(old_doc['_id']), old_doc['_rev'])
            r = yield self._http_delete(url)
        except Exception as err:
            pass
        finally:
            try:
                r = yield self._http_post('_replicator', body, request_timeout=120.0)
            except Exception as err:
                r = None
                print(err)
        return r