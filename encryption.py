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
key named ``secret_generator``.  ``encryption`` middleware is
responsible for calling ``secret_generator`` to get a key, and
encrypting/decrypting the requested object using that key.

Any GET or PUT object requests which are issued when the WSGI environment's
encryption params are not set will result in a ``503 Service Unavailable``.

The ``encryption`` and key-management components should be placed after
auth middleware in the pipeline, to prevent unauthorized requests from wasting
proxy resources.
    [pipeline:main]
    pipeline = catch_errors cache tempauth trivial_key_mgmt encryption proxy-server

The ``encryption`` middleware configuration section takes two optional
parameters: ``cipher_name`` and ``cipher_mode``.  Both come from the pycrypto
package's Crypto module.  Valid cipher names include:
  AES, ARC2, ARC4, Blowfish, CAST, DES, DES3, PKCS1_OAEP, PKCS1_v1_5, XOR
Valid modes include:
  CBC, CFB, CTR, ECB, OFB

Sample configuration section:

[filter:encryption]
cipher_name = AES
cipher_mode = CTR

In absence of a strong reason, we recommend going with the defaults of AES and
CTR.  CTR allows Swift's range requests to be efficient even on encrypted
data.

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

try:
    from Crypto import Cipher
except ImportError:
    raise HTTPInternalServerError('pycrypto not installed on proxy server')


class EncryptionMiddleware(object):
    """Automatically encrypt/decrypt all objects stored/retrieved on disk.

    See module doc for a full description.
    """

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.logger = get_logger(conf, name='encryption')
        self.cipher_name = conf.get('cipher_name', 'AES')
        self.cipher_modename = conf.get('cipher_mode', 'CTR')

        try:
            module = __import__('Crypto.Cipher.%s' % self.cipher_name)
            setattr('Cipher', self.cipher_name, module)
            self.cipher_class = getattr(module, '%sCipher' % self.cipher_class)
            self.cipher_mode = module.get('MODE_%s' % self.cipher_modename)
        except ImportError:
            raise HTTPInternalServerError('Failed to import Crypto.Cipher.%s'
                                          % self.cipher_name)

    @wsgify
    def __call__(self, req):
        if req.method not in ('GET', 'PUT'):
            return self.app

        version, account, container, obj = req.split_path(1, 4, True)
        if not obj:
          # account or container GET/PUT
          return self.app

        if 'encryption_params' not in req.environ:
            raise swob.HTTPServiceUnavailable(
                'At-rest encryption improperly configured')

        params = req.environ['encryption_params']
        secrets = params['secret_generator'](req)
        if type(secrets) is tuple and len(secrets) == 2:
            key, iv = secrets
        elif type(secrets) is bytes:
            key, iv = secrets, None
        else:
            raise swob.HTTPInternalServerError(
                'encryption: secrets() returned unexpected value')

        # TODO:
        #  * pad input to block length if necessary
        #  * deal with offsets in range requests
        cipher = self.cipher_class.new(key, self.cipher_mode, iv=iv)
        env['wsgi.input'] = (cipher.encrypt(i) for i in env['wsgi.input'])
        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    """Returns a WSGI filter app for use with paste.deploy."""
    conf = dict(global_conf, **local_conf)
    conf.setdefault('cipher_name', 'AES')
    conf.setdefault('cipher_mode', 'CTR')
    register_swift_info('encryption', conf)
    return lambda app: EncryptionMiddleware(app, conf)
