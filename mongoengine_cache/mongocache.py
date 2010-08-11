# -*- mode: python; coding: utf-8; -*-

import pymongo
from datetime import datetime, timedelta
from mongoengine.connection import _get_db

try:
    import cPickle as pickle
except ImportError:
    import pickle
import logging

from django.core.exceptions import ImproperlyConfigured
from django.utils.encoding import smart_unicode, smart_str
from django.conf import settings

logger = logging.getLogger('mongodb_cache')
#logger.addHandler(logging.StreamHandler())
#logger.setLevel(logging.INFO)

class InvalidCacheBackendError(ImproperlyConfigured):
    pass

class CacheClass(object):
    def __init__(self,*args,**kwargs):
        self.default_timeout = settings.CACHE_MIDDLEWARE_SECONDS
        self.debug = settings.DEBUG
        self._cache = _get_db().cache
        self._cache.ensure_index('key', unique=True)

    def expired(self, timeout):
        return datetime.now() + timedelta(seconds=timeout or self.default_timeout)

    def add(self, key, value, timeout=None):
        """
        Set a value in the cache if the key does not already exist. If
        timeout is given, that timeout will be used for the key; otherwise
        the default cache timeout will be used.

        Returns True if the value was stored, False otherwise.
        """

        key = smart_str(key)
        value = pickle.dumps(value)
        try:
            obj = {'key': key, 'value': value, 'expired': self.expired(timeout)}
            self._cache.save(obj, safe=True)
        except pymongo.errors.OperationFailure:
            return False
        else:
            return True

    def get(self, key, default=None):
        """
        Fetch a given key from the cache. If the key does not exist, return
        default, which itself defaults to None.
        """

        key = smart_str(key)
        obj = {'key': key}
        obj = self._cache.find_one(obj)
        if obj is None:
            return default
        else:
            if obj['expired'] < datetime.now():
                self._cache.remove({'key': obj['key']})
                return default
            else:
                if self.debug:
                    logger.info('Success cache hit for key: %s' % key)
                value = pickle.loads(obj['value'].encode('utf-8'))
                if isinstance(value, basestring):
                    return smart_unicode(value)
                else:
                    return value

    def set(self, key, value, timeout=None):
        """
        Set a value in the cache. If timeout is given, that timeout will be
        used for the key; otherwise the default cache timeout will be used.
        """

        key = smart_str(key)
        if not self.add(key, value, timeout):
            value = pickle.dumps(value)
            obj = {'key': key, 'value': value, 'expired': self.expired(timeout)}
            self._cache.update({'key': key}, obj)

    def delete(self, key):
        """
        Delete a key from the cache, failing silently.
        """

        key = smart_str(key)
        self._cache.remove({'key': key})

    def get_many(self, keys):
        """
        Fetch a bunch of keys from the cache. For certain backends (memcached,
        pgsql) this can be *much* faster when fetching multiple values.

        Returns a dict mapping each key in keys to its value. If the given
        key is missing, it will be missing from the response dict.
        """
        d = {}
        for k in keys:
            val = self.get(k)
            if val is not None:
                d[k] = val
        return d

    def has_key(self, key):
        """
        Returns True if the key is in the cache and has not expired.
        """
        return self.get(key) is not None

    def incr(self, key, delta=1):
        """
        Add delta to value in the cache. If the key does not exist, raise a
        ValueError exception.
        """
        if key not in self:
            raise ValueError, "Key '%s' not found" % key
        new_value = self.get(key) + delta
        self.set(key, new_value)
        return new_value

    def decr(self, key, delta=1):
        """
        Subtract delta from value in the cache. If the key does not exist, raise
        a ValueError exception.
        """
        return self.incr(key, -delta)

    def __contains__(self, key):
        """
        Returns True if the key is in the cache and has not expired.
        """
        # This is a separate method, rather than just a copy of has_key(),
        # so that it always has the same functionality as has_key(), even
        # if a subclass overrides it.
        return self.has_key(key)

    def _get_num_entries(self):
        return self._cache.count()
