#!/usr/bin/python
"""
Functionally test revisions middleware in a running Swift cluster.

This program runs against a previously installed Swift cluster with the
revisions middleware configured and in the proxy-server pipeline.  Many
parameters (URL, username, password, etc.) can be set via the command line,
but come with reasonable defaults.  Run with "-h" for command-line help.

A successful run will have "ALL TESTS PASS" printed at the end.  A failing
run will end with a line that contains "FAIL".
"""
import sys
import time
import argparse
import json
import socket
import swiftclient
try:
    from revisions import REVS_HEADER
except ImportError:
    REVS_HEADER = 'X-Object-Meta-Revision-Reference'

# These global vars will be used throughout the functional testing
OPTS, CONN = None, None

# Trivial helper functions to print out status
def ok(msg): print 'OK: %s' % msg
def fail(msg):
    print 'FAIL: %s' % msg
    sys.exit(1)


def parse_options(data_from=None):
    global OPTS
    desc = 'Test revisions middleware against a running Swift installation'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("--user", help="username for X-Auth-User",
                        default="test:tester")
    parser.add_argument("--key", help="key/password for X-Auth-Key",
                        default="testing")
    parser.add_argument("--server", help="proxy server root URL and port",
                        default="http://127.0.0.1:8080")
    parser.add_argument("--auth-url", help="server path to authorization URL",
                        default="auth/v1.0")
    pcn_help = 'container where revisioned object will appear to be stored'
    parser.add_argument("--primary-container-name", help=pcn_help,
                        default="r_primary", dest='cont')
    rcn_help = 'container where all revisions will be stored'
    parser.add_argument("--revisions-container-name", help=rcn_help,
                        default="r_revisions", dest='revs_cont')
    parser.add_argument("--test-object-name", help='name of test object',
                        default="object1", dest='obj')
    parser.add_argument("--timeout", help='seconds to wait for Swift response',
                        type=float, default=15.0)
    OPTS = parser.parse_args(args=data_from)


def get_cluster_info():
    """
    Retrieve the Swift cluster info from the API endpoint
    Return None on exceptions
    (python-swiftclient should really have a method like this...)
    """
    url = '%s/info' % OPTS.server
    parsed, conn = swiftclient.http_connection(url)
    try:
        conn.request('GET', parsed.path, '')
        resp = conn.getresponse()
        body = resp.read()
        resp.close()
        return json.loads(body)
    except socket.error:
        return None   # can't connect
    except ValueError:
        return None   # bad JSON, e.g. empty response


def verify_revisions_middleware_or_timeout():
    """
    Use Swift cluster info to verify revisions middleware in the pipeline.
    Retry for a little while, in case Swift is being restarted.
    """
    give_up_at = time.time() + OPTS.timeout
    cluster_info = None
    while cluster_info is None and time.time() < give_up_at:
        cluster_info = get_cluster_info()
        if cluster_info is None:
            time.sleep(1)
    if cluster_info is None:
        fail('Could not reach Swift proxy server at %s.  Aborting.' % (
            OPTS.server))
    ok('cluster info retrieved from %s/info' % OPTS.server)
    if 'revisions' not in cluster_info:
        fail('revisions middleware not installed in cluster')
    ok('revisions middleware installed in cluster')


def get_connection():
    """
    Set the global var CONN by instantiating a swiftclient.Connection object
    """
    global CONN
    auth_url = '%s/%s' % (OPTS.server, OPTS.auth_url)
    CONN = swiftclient.Connection(
        authurl=auth_url, user=OPTS.user, key=OPTS.key)
    ok('Using account %r, token %r' % CONN.get_auth())


def initialize_containers():
    """
    Create primary and revisions containers, if they don't already exist.
    Set X-Container-Meta-Revisions-Location on the primary container.
    """
    try:
        headers = CONN.head_container(OPTS.cont)
        ok('Test container %r was previously created' % OPTS.cont)
    except swiftclient.exceptions.ClientException as ce:
        if ce.http_status == 404:
            ok('No pre-existing test container %r' % OPTS.cont)
            CONN.put_container(OPTS.cont)
            ok('Created test container %r' % OPTS.cont)
        else:
            fail('test container HEAD request failed: %s' % ce.http_status)

    # Set revisions container -- might redundantly overwrite metadata
    container_meta = {'X-Container-Meta-Revisions-Location': OPTS.revs_cont}
    CONN.post_container(OPTS.cont, container_meta)

    try:
        headers = CONN.head_container(OPTS.revs_cont)
        ok('Revisions container %r was previously created' % OPTS.cont)
    except swiftclient.exceptions.ClientException as ce:
        if ce.http_status == 404:
            ok('No pre-existing revisions container %r' % OPTS.revs_cont)
            CONN.put_container(OPTS.revs_cont)
            ok('Created revisions container %r' % OPTS.revs_cont)
        else:
            fail('test container HEAD request failed: %s' % ce.http_status)


def empty_containers():
    """
    Delete all objects from primary and revisions containers.
    (Containers might have existed before this func test started running.)
    """
    for container in (OPTS.cont, OPTS.revs_cont):
        try:
            headers, contents = CONN.get_container(container)
        except swiftclient.exceptions.ClientException as ce:
            fail('Could not GET %r: %s' % (container, ce.http_status))

        for objname in (i['name'] for i in contents):
            ok('Deleting %s/%s' % (container, objname))
            CONN.delete_object(container, objname)


def put_object(data):
    """
    Store the input data in a PUT to the object.
    (Object path is based on command-line params.)
    """
    try:
        etag = CONN.put_object(OPTS.cont, OPTS.obj, data)
        ok('Wrote data to %s/%s: %r' % (OPTS.cont, OPTS.obj, data))
    except swiftclient.exceptions.ClientException as ce:
        fail('could not write to %s/%s: %s' % (
                OPTS.cont, OPTS.obj, ce.http_status))


def revision_container_contents():
    """
    Return a list of the contents (objects) of the revisions container.
    """
    try:
        headers, data = CONN.get_container(OPTS.revs_cont)
        ok('Retrieved listing from %r' % OPTS.revs_cont)
    except swiftclient.exceptions.ClientException as ce:
        fail('GET failed on container %r: %s' % (
                OPTS.revs_cont, ce.http_status))
    return data


def verify_revision_container_count(howmany):
    """
    Fail out if the revisions container does not have :howmany: objects
    """
    data = revision_container_contents()
    if len(data) == howmany:
        ok('Verified that %s currently contains %d revisions' % (
                OPTS.revs_cont, howmany))
    else:
        fail('%s contains %d revisions (expected %d)' % (
                OPTS.revs_cont, len(data), howmany))


def verify_object_name_in_revisions_container(objname):
    """
    Fail out if :objname: is not an object in the revisions container.
    """
    data = revision_container_contents()
    revisions = [i['name'] for i in data]
    if objname in revisions:
        ok('Object %r in revisions container %r' % (objname, OPTS.revs_cont))
    else:
        fail('Object %r NOT in revisions container %r -- contents were %r' %
             (objname, OPTS.revs_cont, data))


def head_with_manifest(cont, obj):
    """
    Return the headers of the empty manifest object.
    Manifest object's location is dictated by command-line params.

    python-swiftclient Connection can't take query params, so we need this
    helper method.
    """
    conn = CONN.http_conn[1]
    url = '%s/%s/%s?manifest=true' % (CONN.http_conn[0].path, cont, obj)
    headers = {'X-Auth-Token': CONN.token}
    conn.request('HEAD', url, headers=headers)
    resp = conn.getresponse()
    body = resp.read()
    if resp.status < 200 or resp.status >= 300:
        fail('HEAD to %s/%s with manifest returned %s' % (
                cont, obj, resp.status))
    resp_headers = {}
    for header, value in resp.getheaders():
        resp_headers[header.lower()] = value
    return resp_headers


def empty_put_with_manifest(cont, obj, headers):
    """
    Perform an empty PUT (headers only) to the empty manifest object.
    Manifest object's location is dictated by command-line params.

    python-swiftclient Connection can't take query params, so we need this
    helper method.
    """
    conn = CONN.http_conn[1]
    url = '%s/%s/%s?manifest=true' % (CONN.http_conn[0].path, cont, obj)
    headers.update({'X-Auth-Token': CONN.token})
    conn.request('PUT', url, headers=headers)
    resp = conn.getresponse()
    body = resp.read()
    if resp.status < 200 or resp.status >= 300:
        fail('PUT to %s/%s with manifest returned %s' % (
                cont, obj, resp.status))


def most_recent_revision_name():
    """
    Retrieve the manifest, return the object name (revision) that it points to.
    """
    try:
        headers = head_with_manifest(OPTS.cont, OPTS.obj)
        ok('Retrieved manifest headers for %s/%s' % (OPTS.cont, OPTS.obj))
    except swiftclient.exceptions.ClientException as ce:
        fail('HEAD failed on object %s/%s: %s' % (
                OPTS.cont, OPTS.obj, ce.http_status))
    if REVS_HEADER.lower() not in headers:
        fail('%r not in headers for object %s/%s:\n%r' % (
                REVS_HEADER, OPTS.cont, OPTS.obj, headers))
    revname = headers.get(REVS_HEADER.lower())
    ok('Most recent revision of %s/%s is %s' % (OPTS.cont, OPTS.obj, revname))
    return revname


def verify_object_contents(expected):
    """
    Download the object and verify that its contents match :expected:
    Object's name and location are given by command-line params.
    """
    try:
        headers, data = CONN.get_object(OPTS.cont, OPTS.obj)
        ok('Retrieved object %s/%s' % (OPTS.cont, OPTS.obj))
    except swiftclient.exceptions.ClientException as ce:
        fail('GET failed on object %s/%s: %s' % (
                OPTS.cont, OPTS.obj, ce.http_status))
    if data != expected:
        fail('Expected contents of %r, retrieved %r' % (expected, data))
    ok('Contents were as expected, %r' % expected)


def delete_object():
    """
    Delete the object named on the command line, and fail out on errors.
    """
    try:
        CONN.delete_object(OPTS.cont, OPTS.obj)
        ok('Deleted object %s/%s' % (OPTS.cont, OPTS.obj))
    except swiftclient.exceptions.ClientException as ce:
        fail('GET failed on object %s/%s: %s' % (
                OPTS.cont, OPTS.obj, ce.http_status))


def verify_404():
    """
    Fail out if the object named on the command line is actually present.
    """
    try:
        headers = CONN.head_object(OPTS.cont, OPTS.obj)
        fail('HEAD on %s/%s should have returned 404, but returned 20X' % (
                OPTS.cont, OPTS.obj))
    except swiftclient.exceptions.ClientException as ce:
        if ce.http_status == 404:
            ok('HEAD on %s/%s returned 404 as expected' % (
                    OPTS.cont, OPTS.obj))
        else:
            fail('HEAD on %s/%s returned %s instead of 404' % (
                    OPTS.cont, OPTS.obj, ce.http_status))


def explicitly_set_manifest(revname):
    """
    Forcibly point the manifest to :revname:.
    This simulates an undelete (or reversion to an earlier revision).
    """
    empty_put_with_manifest(OPTS.cont, OPTS.obj, {REVS_HEADER: revname})
    ok('Explicitly set manifest to revision %r' % revname)


def post_new_header(headers):
    """
    Send a POST with a new header to the object in the primary container.
    This should add the header to the referenced object in the revisions
    container, and should NOT alter the manifest.

    :param headers: dict containing {header-name: header value} pairs
    """
    try:
        etag = CONN.post_object(OPTS.cont, OPTS.obj, headers)
        ok('Posted new headers to %s/%s: %r' % (OPTS.cont, OPTS.obj, headers))
    except swiftclient.exceptions.ClientException as ce:
        fail('could not POST to %s/%s: %s' % (
                OPTS.cont, OPTS.obj, ce.http_status))


def verify_manifest_does_not_contain_header(headers):
    """
    Confirm the POSTing to the primary container did not alter the manifest.

    :param headers: one-element dict containing the header that was POSTed
    """
    man_headers = head_with_manifest(OPTS.cont, OPTS.obj)
    new_header = headers.keys()[0]
    if new_header in man_headers:
        fail('Header %r appeared in manifest, not object' % new_header)
    else:
        ok('Header %r was not written to the manifest' % new_header)


def verify_revision_contains_header(revname, headers):
    """
    Confirm the POSTing to the primary container altered the current revision.
    This both checks the revisions container explicitly, and also checks the
    result of doing a HEAD on the primary container.

    :param headers: one-element dict containing the header that was POSTed
    """
    new_header = headers.keys()[0]
    try:
        obj_headers = CONN.head_object(OPTS.revs_cont, revname)
    except swiftclient.exceptions.ClientException as ce:
        fail('could not HEAD %s/%s: %s' % (
                OPTS.revs_cont, revname, ce.http_status))
    obj_header_val = obj_headers.get(new_header.lower())
    if obj_header_val == 'Bbb':
        ok('Header %r was written to correct revision' % new_header)
    else:
        fail('Header %r was %r in %s/%s' % (
                new_header, obj_header_val, OPTS.revs_cont, revname))
    try:
        obj_headers = CONN.head_object(OPTS.cont, OPTS.obj)
    except swiftclient.exceptions.ClientException as ce:
        fail('could not HEAD %s/%s: %s' % (
                OPTS.cont, OPTS.obj, ce.http_status))
    obj_header_val = obj_headers.get(new_header.lower())
    if obj_header_val == 'Bbb':
        ok('Header %r visible in primary container' % new_header)
    else:
        fail('Header %r was %r in %s/%s' % (
                new_header, obj_header_val, OPTS.revs_cont, revname))


if __name__ == '__main__':
    # Setup
    parse_options()
    verify_revisions_middleware_or_timeout()
    get_connection()
    initialize_containers()
    empty_containers()

    # Basic checks
    put_object('Test object, revision 1')
    verify_revision_container_count(1)
    revname1 = most_recent_revision_name()
    verify_object_name_in_revisions_container(revname1)
    verify_object_contents('Test object, revision 1')

    # Overwrite object, check revisions behavior
    put_object('Test object, revision 2')
    verify_revision_container_count(2)
    revname2 = most_recent_revision_name()
    verify_object_name_in_revisions_container(revname2)
    verify_object_contents('Test object, revision 2')

    # Delete object, check revisions behavior
    delete_object()
    verify_404()
    verify_revision_container_count(2)

    # Check reversion by explicitly setting the manifest
    explicitly_set_manifest(revname1)
    verify_object_contents('Test object, revision 1')
    explicitly_set_manifest(revname2)
    verify_object_contents('Test object, revision 2')

    # Check that POSTs affect the current revision
    headers = {'X-Object-Meta-Aaa': 'Bbb'}
    post_new_header(headers)
    verify_manifest_does_not_contain_header(headers)
    verify_revision_contains_header(revname2, headers)

    ok('=== ALL TESTS PASSED ===\n')
