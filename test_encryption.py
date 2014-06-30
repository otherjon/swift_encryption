#!/usr/bin/python

import unittest
import mock
import revisions

from swift.common import swob


def make_req(path='https://swift.example.com/v1/a/c/o'):
    req = swob.Request.blank(path)
    req.environ['HTTP_X_AUTH_TOKEN'] = 'AUTH_tk000'
    return req


class RevisionsModuleTest(unittest.TestCase):
    def test_constants(self):
        self.assertTrue(revisions.REVS_HEADER.startswith('X-Object-Meta-'))

    def test_close_if_possible(self):
        closeme = mock.Mock(spec=['close'])
        closeme.close = mock.Mock()
        revisions.close_if_possible(closeme)
        self.assertEqual([()], closeme.close.mock_calls)
        dontcloseme = mock.Mock(spec=[])
        revisions.close_if_possible(dontcloseme)  # should not raise exception


class SubrequestTest(unittest.TestCase):
    def setUp(self):
        self.app = mock.Mock()
        self.subreq = revisions.Subrequest(self.app)
        self.app_calls_seen = []
        self.app_call_output = [('200 OK', {}, ['body', 'text'])]
        def mock_app_call(env):
            self.app_calls_seen.append(env)
            status_str, headers, body = self.app_call_output[0]
            self.app_call_output = self.app_call_output[1:]
            self.subreq._response_status = status_str
            self.subreq._response_headers = headers
            return body
        self.subreq._app_call = mock_app_call

    def test_get_revision_name_from_manifest(self):
        req = make_req()

        # Common case: REVS_HEADER present, should return its value
        mock_headers = {revisions.REVS_HEADER: 'revision.name'}
        self.app_call_output = [('200 OK', mock_headers, [])]
        revname = self.subreq.get_revision_name_from_manifest(req)
        self.assertEqual('revision.name', revname)

        # Error case: REVS_HEADER absent, should raise PreconditionFailed
        self.app_call_output = [('200 OK', {}, [])]
        with self.assertRaises(swob.HTTPException) as cm:
            # can't assertRaises HTTPPreconditionFailed -- it's not really an
            # exception, it's a partial!
            self.subreq.get_revision_name_from_manifest(req)
        self.assertEqual('412 Precondition Failed', cm.exception.status)

    def test_update_manifest(self):
        req = make_req()
        self.subreq.update_manifest(req, 'new.revision.name')
        self.assertEqual(1, len(self.app_calls_seen))
        action = self.app_calls_seen[0]

        # It's tough to assert that the PUT worked -- we'll verify the env
        expected = {
            'REQUEST_METHOD': 'PUT',
            'CONTENT_LENGTH': 0,
            'HTTP_X_AUTH_TOKEN': 'AUTH_tk000',
            'swift.source': 'revisions',
            'HTTP_X_OBJECT_META_REVISION_REFERENCE': 'new.revision.name',
        }
        for key, expected_val in expected.items():
            self.assertEqual(expected_val, action[key])

    def test_put_or_post(self):
        req = make_req()
        mock_headers = {'hdr1': 'val1', 'hdr2': 'val2'}
        mock_body = ['mock', 'body']
        self.app_call_output = [('299 Spiffy', mock_headers, mock_body)]
        result = self.subreq.put_or_post(req.environ, 'v1', 'aa', 'cc', 'oo')
        self.assertEqual(3, len(result))
        status, headers, body = result
        self.assertEqual(299, status)
        self.assertEqual(mock_headers, headers)
        self.assertEqual(''.join(mock_body), body)


class RevisionsMiddlewareTest(unittest.TestCase):
    def setUp(self):
        self.app = mock.Mock()
        self.ware = revisions.RevisionsMiddleware(self.app, {'empty': 'conf'})

    def test_params_for_intercepting_this_request(self):
        default_env = make_req().environ
        self.ware.get_revisions_container = lambda req: 'revs_cont'
        params = self.ware.params_for_intercepting_this_request  # convenience

        # The request environment above should want intercepting, and should
        # return valid params
        test_env = default_env.copy()
        result = params(swob.Request(test_env))
        self.assertIsNotNone(result)
        ver, acct, cont, obj, revs_cont = result
        self.assertEqual('v1', ver)
        self.assertEqual('a', acct)
        self.assertEqual('c', cont)
        self.assertEqual('o', obj)
        self.assertEqual('revs_cont', revs_cont)

        # Anything other than HEAD, GET, PUT, POST should return None
        test_env = default_env.copy()
        test_env['REQUEST_METHOD'] = 'OPTIONS'
        self.assertIsNone(params(swob.Request(test_env)))

        # Should return None if QUERY_STRING contains "manifest"
        test_env = default_env.copy()
        test_env['REQUEST_METHOD'] = 'GET'
        test_env['QUERY_STRING'] = 'a=1&manifest=true&b=2'
        self.assertIsNone(params(swob.Request(test_env)))
        test_env['REQUEST_METHOD'] = 'HEAD'
        test_env['QUERY_STRING'] = 'manifest=1'
        self.assertIsNone(params(swob.Request(test_env)))
        test_env['REQUEST_METHOD'] = 'PUT'
        test_env['QUERY_STRING'] = 'a=1&manifest'

        # Non-object requests should return None
        test_env = make_req('/v1/a').environ
        self.assertIsNone(params(swob.Request(test_env)))
        test_env = make_req('/v1/a/c').environ
        self.assertIsNone(params(swob.Request(test_env)))

        # Should return None if revisions_container not present
        self.ware.get_revisions_container = lambda req: None
        test_env = default_env.copy()
        self.assertIsNone(params(swob.Request(test_env)))

    def test_get_revisions_container(self):
        container_info = {
            'status': 200,
            'meta': {'revisions-location': 'revs_cont'},
        }
        req = make_req()
        with mock.patch('revisions.get_container_info') as gci:
            # With container_info above, everything should work
            gci.return_value = container_info
            result = self.ware.get_revisions_container(req)
            self.assertEqual('revs_cont', result)

            # With no revisions-location, should return None
            container_info['meta'] = {'junk': 'junk'}
            self.assertIsNone(self.ware.get_revisions_container(req))

            # With non-200-series status, should raise HTTPException
            container_info['meta'] = {'revisions-location': 'revs_cont'}
            container_info['status'] = 401
            with self.assertRaises(swob.HTTPException) as cm:
                result = self.ware.get_revisions_container(req)
            self.assertEqual('401 Unauthorized', cm.exception.status)


if __name__ == '__main__':
    unittest.main()
