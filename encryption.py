#!/usr/bin/python

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
swift_encryption
===============

OpenStack Swift middleware to provide at-rest encryption for objects.

``encryption`` allows Swift to offer at-rest encryption.  Once this middleware
is in place, all data stored in the cluster will be encrypted, according to
the parameters provided in the rest of the configuration.  This feature can:
 * protect against threats such as attackers stealing hard drives from the
   cluster
 * remove concerns about disposing of dead drives with sensitive data
 * help provide secure deletion/expiry, when per-object encryption keys are
   shredded

``encryption`` middleware must be paired with a key-management middleware
component.  The key management middleware component is responsible for
determining the key associated with a given /account/container/object path.
Example key management strategies might include:
 * trivial and insecure: storing a fixed key in plaintext in the middleware
   config
 * constant, POST-initialized: external service is responsible for POSTing
   the same key to all proxies before the first object request; same key is
   then used for all objects
 * programmatic and dynamic: middleware provides a function which accepts
   /account/container/object and returns a key
 * etc.

Note that only object data is encrypted.  Metadata, container listings, etc.
are not encrypted.

Key management middleware is responsible for setting the WSGI environment's
``encryption_params`` sub-dictionary, including at a minimum a dictionary
key named ``key_generator``.  ``encryption`` middleware is
responsible for calling ``key_generator`` to get a key, and
encrypting/decrypting the requested object using that key.

Any GET or PUT object requests which are issued when the WSGI environment's
encryption params are not set will result in a ``503 Service Unavailable``.

The ``encryption`` and key-management components should be placed after
auth middleware in the pipeline, to prevent unauthorized requests from wasting
proxy resources.
    [pipeline:main]
    pipeline = catch_errors cache tempauth trivial_key_mgmt encryption proxy-server

Caveats:

 * Encryption is CPU-intensive.  Adding this middleware to your pipeline will
   greatly increase the CPU demands of your proxy servers.

 * You must use a key-management middleware component in your pipeline also.
"""
import time
from itertools import chain, ifilter
from swift.common import swob, wsgi
from swift.common.swob import wsgify
from swift.common.utils import register_swift_info, get_logger
from swift.proxy.controllers.base import get_container_info

# Object metadata header that specifies the current revision
REVS_HEADER = 'X-Object-Meta-Revision-Reference'


class EncryptionMiddleware(object):
    """Automatically encrypt/decrypt all objects stored/retrieved on disk.

    See module doc for a full description.
    """

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.logger = get_logger(conf, name='encryption')


    @wsgify
    def __call__(self, req):
        if req.method not in ('GET', 'PUT'):
            return self.app

        version, account, container, obj = req.split_path(1, 4, True)
        if not obj:
          # account or container GET/PUT
          return self.app

        params = req.environ['encryption_params']
        key = params['key_generator'](req)
        encrypt = 
        # ^^^

        not_blank = lambda somestr: somestr != ''
        bz = BZ2Compressor()
        compressed = chain((bz.compress(i) for i in env['wsgi.input']),
                           (bz.flush() for i in (1,)))
        env['wsgi.input'] = ifilter(not_blank, compressed)
        return self.app(env, start_response)

        if 'encryption_params' not in req.environ:
            raise swob.HTTPServiceUnavailable(
                'At-rest encryption improperly configured')

        if req.method != 'PUT':
            # We'll need the manifest info for anything other than a PUT
            # (For a PUT, we'll just overwrite it)
            revname = self.subreq.get_revision_name_from_manifest(req)
            self.logger.info('Retrieved manifest: /%s/%s/%s -> %s/%s' % (
                    acct, cont, obj, revs_cont, revname))

        if req.method in ('HEAD', 'GET'):
            # Return the result of sending the request to the new URL
            new_path = '/%s/%s/%s/%s' % (ver, acct, revs_cont, revname)
            req.environ['PATH_INFO'] = new_path
            return self.app

        # This is a POST/PUT request on an object with a revisions container.

        # First step: Send the request to the revisions container.
        if req.environ['REQUEST_METHOD'] == 'PUT':
            # Create a new object in the Revisions-Location container
            new_obj = '%s.%s' % (obj, time.time())
        else:
            # We're POSTing data.  Which object in the revisions container?
            #   Whatever the manifest tells us.
            new_obj = revname
        status, headers, body = self.subreq.put_or_post(
            req.environ, ver, acct, revs_cont, new_obj)

        # Was this a PUT?  If so, update the manifest.
        if req.method == 'PUT':
            self.subreq.update_manifest(req, new_obj)
            self.logger.info('Updated manifest: /%s/%s/%s -> %s/%s' % (
                    acct, cont, obj, revs_cont, new_obj))

        # For either PUT or POST, return the result of the operation in the
        # revisions container, assuming we haven't yet errored out.
        return swob.Response(status=status, headers=headers, body=body)


def filter_factory(global_conf, **local_conf):
    """Returns a WSGI filter app for use with paste.deploy."""
    register_swift_info('revisions')
    conf = dict(global_conf, **local_conf)
    return lambda app: RevisionsMiddleware(app, conf)
