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

Caveats:

 * Encryption is CPU-intensive.  Adding this middleware to your pipeline will
   greatly increase the CPU demands of your proxy servers.

 * You must use a key-management middleware component in your pipeline also.
