# -*- coding: utf-8 -*-
from unittest import TestCase
import mimetypes
from scrapydd.poster import encode
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
import sys
from six import ensure_binary

def unix2dos(s):
    return s.replace(b"\n", b"\r\n")

class TestEncode_String(TestCase):
    def test_simple(self):
        expected = unix2dos(b"""--XXXXXXXXX
Content-Disposition: form-data; name="foo"
Content-Type: text/plain; charset=utf-8

bar
""")
        self.assertEqual(expected,
                encode.encode_string("XXXXXXXXX", "foo", "bar"))

    def test_quote_name_space(self):
        expected = unix2dos(b"""--XXXXXXXXX
Content-Disposition: form-data; name="foo baz"
Content-Type: text/plain; charset=utf-8

bar
""")
        self.assertEqual(expected,
                encode.encode_string("XXXXXXXXX", "foo baz", "bar"))

    def test_quote_name_phparray(self):
        expected = unix2dos(b"""--XXXXXXXXX
Content-Disposition: form-data; name="files[]"
Content-Type: text/plain; charset=utf-8

bar
""")
        self.assertEqual(expected,
                encode.encode_string("XXXXXXXXX", "files[]", "bar"))

    def test_quote_unicode_name(self):
        expected = unix2dos(b"""--XXXXXXXXX
Content-Disposition: form-data; name="=?utf-8?b?4piD?="
Content-Type: text/plain; charset=utf-8

bar
""")
        self.assertEqual(expected,
                encode.encode_string("XXXXXXXXX", u"\N{SNOWMAN}", "bar"))

    def test_quote_value(self):
        expected = unix2dos(b"""--XXXXXXXXX
Content-Disposition: form-data; name="foo"
Content-Type: text/plain; charset=utf-8

bar baz@bat
""")
        self.assertEqual(expected,
                encode.encode_string("XXXXXXXXX", "foo", "bar baz@bat"))

    def test_boundary(self):
        expected = unix2dos(b"""--ABC+DEF
Content-Disposition: form-data; name="foo"
Content-Type: text/plain; charset=utf-8

bar
""")
        self.assertEqual(expected,
                encode.encode_string("ABC DEF", "foo", "bar"))

    def test_unicode(self):
        expected = ensure_binary(unix2dos(b"""--XXXXXXXXX
Content-Disposition: form-data; name="foo"
Content-Type: text/plain; charset=utf-8

b\xc3\xa1r
"""))
        self.assertEqual(expected,
                encode.encode_string("XXXXXXXXX", "foo", u"bár"))


class TestEncode_File(TestCase):
    def test_simple(self):
        expected = unix2dos(b"""--XXXXXXXXX
Content-Disposition: form-data; name="foo"
Content-Type: text/plain; charset=utf-8

""")
        self.assertEqual(expected,
                encode.encode_file_header("XXXXXXXXX", "foo", 42))

    def test_content_type(self):
        expected = unix2dos(b"""--XXXXXXXXX
Content-Disposition: form-data; name="foo"
Content-Type: text/html

""")
        self.assertEqual(expected,
                encode.encode_file_header("XXXXXXXXX", "foo", 42, filetype="text/html"))

    def test_filename_simple(self):
        expected = unix2dos(b"""--XXXXXXXXX
Content-Disposition: form-data; name="foo"; filename="test.txt"
Content-Type: text/plain; charset=utf-8

""")
        self.assertEqual(expected,
                encode.encode_file_header("XXXXXXXXX", "foo", 42,
                    "test.txt"))

    def test_quote_filename(self):
        expected = unix2dos(b"""--XXXXXXXXX
Content-Disposition: form-data; name="foo"; filename="test file.txt"
Content-Type: text/plain; charset=utf-8

""")
        self.assertEqual(expected,
                encode.encode_file_header("XXXXXXXXX", "foo", 42,
                    "test file.txt"))

        expected = unix2dos(b"""--XXXXXXXXX
Content-Disposition: form-data; name="foo"; filename="test\\"file.txt"
Content-Type: text/plain; charset=utf-8

""")
        self.assertEqual(expected,
                encode.encode_file_header("XXXXXXXXX", "foo", 42,
                    "test\"file.txt"))

    def test_unicode_filename(self):
        expected = unix2dos(b"""--XXXXXXXXX
Content-Disposition: form-data; name="foo"; filename="&#9731;.txt"
Content-Type: text/plain; charset=utf-8

""")
        self.assertEqual(expected,
                encode.encode_file_header("XXXXXXXXX", "foo", 42,
                    u"\N{SNOWMAN}.txt"))

class TestEncodeAndQuote(TestCase):
    def test(self):
        self.assertEqual(b"foo+bar", encode.encode_and_quote("foo bar"))
        self.assertEqual(b"foo%40bar", encode.encode_and_quote("foo@bar"))
        self.assertEqual(b"%28%C2%A9%29+2008",
                encode.encode_and_quote(u"(©) 2008"))

class TestMultiparam(TestCase):
    def test_from_params(self):
        fp = open("tests/poster/test_encode.py")
        expected = [encode.MultipartParam("foo", "bar"),
                    encode.MultipartParam("baz", fileobj=fp,
                        filename=fp.name,
                        filetype=mimetypes.guess_type(fp.name)[0])]

        self.assertEqual(encode.MultipartParam.from_params(
            [("foo", "bar"), ("baz", fp)]), expected)

        self.assertEqual(encode.MultipartParam.from_params(
            (("foo", "bar"), ("baz", fp))), expected)

        self.assertEqual(encode.MultipartParam.from_params(
            {"foo": "bar", "baz": fp}), expected)

        self.assertEqual(encode.MultipartParam.from_params(
            [expected[0], expected[1]]), expected)

    def test_from_params_dict(self):

        p = encode.MultipartParam('file', fileobj=open("tests/poster/test_encode.py"))
        params = {"foo": "bar", "file": p}

        expected = [encode.MultipartParam("foo", "bar"), p]
        retval = encode.MultipartParam.from_params(params)

        expected.sort()
        retval.sort()

        self.assertEqual(retval, expected)

    def test_from_params_assertion(self):
        p = encode.MultipartParam('file', fileobj=open("tests/poster/test_encode.py"))
        params = {"foo": "bar", "baz": p}

        self.assertRaises(AssertionError, encode.MultipartParam.from_params,
                params)

    def test_simple(self):
        p = encode.MultipartParam("foo", "bar")
        boundary = "XYZXYZXYZ"
        expected = unix2dos(b"""--XYZXYZXYZ
Content-Disposition: form-data; name="foo"
Content-Type: text/plain; charset=utf-8

bar
--XYZXYZXYZ--
""")
        self.assertEqual(p.encode(boundary), expected[:-len(boundary)-6])
        self.assertEqual(p.get_size(boundary), len(expected)-len(boundary)-6)
        self.assertEqual(encode.get_body_size([p], boundary),
                len(expected))
        self.assertEqual(encode.get_headers([p], boundary),
                {'Content-Length': str(len(expected)),
                 'Content-Type': 'multipart/form-data; boundary=%s' % boundary})

        datagen, headers = encode.multipart_encode([p], boundary)
        self.assertEqual(headers, {'Content-Length': str(len(expected)),
                 'Content-Type': 'multipart/form-data; boundary=%s' % boundary})
        self.assertEqual(b"".join(datagen), expected)

    def test_multiple_keys(self):
        params = encode.MultipartParam.from_params(
                [("key", "value1"), ("key", "value2")])
        boundary = "XYZXYZXYZ"
        datagen, headers = encode.multipart_encode(params, boundary)
        encoded = b"".join(datagen)

        expected = unix2dos(b"""--XYZXYZXYZ
Content-Disposition: form-data; name="key"
Content-Type: text/plain; charset=utf-8

value1
--XYZXYZXYZ
Content-Disposition: form-data; name="key"
Content-Type: text/plain; charset=utf-8

value2
--XYZXYZXYZ--
""")
        self.assertEqual(encoded, expected)


    def test_stringio(self):
        fp = StringIO("file data")
        params = encode.MultipartParam.from_params( [("foo", fp)] )
        boundary = "XYZXYZXYZ"
        datagen, headers = encode.multipart_encode(params, boundary)
        encoded = b"".join(datagen)

        expected = unix2dos(b"""--XYZXYZXYZ
Content-Disposition: form-data; name="foo"
Content-Type: text/plain; charset=utf-8

file data
--XYZXYZXYZ--
""")
        self.assertEqual(encoded, expected)

    def test_reset_string(self):
        p = encode.MultipartParam("foo", "bar")
        boundary = "XYZXYZXYZ"

        datagen, headers = encode.multipart_encode([p], boundary)

        expected = unix2dos(b"""--XYZXYZXYZ
Content-Disposition: form-data; name="foo"
Content-Type: text/plain; charset=utf-8

bar
--XYZXYZXYZ--
""")

        self.assertEquals(b"".join(datagen), expected)
        datagen.reset()
        self.assertEquals(b"".join(datagen), expected)

    def test_reset_multiple_keys(self):
        params = encode.MultipartParam.from_params(
                [("key", "value1"), ("key", "value2")])
        boundary = "XYZXYZXYZ"
        datagen, headers = encode.multipart_encode(params, boundary)
        expected = unix2dos(b"""--XYZXYZXYZ
Content-Disposition: form-data; name="key"
Content-Type: text/plain; charset=utf-8

value1
--XYZXYZXYZ
Content-Disposition: form-data; name="key"
Content-Type: text/plain; charset=utf-8

value2
--XYZXYZXYZ--
""")
        encoded = b"".join(datagen)
        self.assertEqual(encoded, expected)
        datagen.reset()
        encoded = b"".join(datagen)
        self.assertEqual(encoded, expected)

    def test_reset_file(self):
        fp = StringIO("file data")
        params = encode.MultipartParam.from_params( [("foo", fp)] )
        boundary = "XYZXYZXYZ"
        datagen, headers = encode.multipart_encode(params, boundary)

        expected = unix2dos(b"""--XYZXYZXYZ
Content-Disposition: form-data; name="foo"
Content-Type: text/plain; charset=utf-8

file data
--XYZXYZXYZ--
""")
        encoded = b"".join(datagen)
        self.assertEqual(encoded, expected)
        datagen.reset()
        encoded = b"".join(datagen)
        self.assertEqual(encoded, expected)

    def test_MultipartParam_cb(self):
        log = []
        def cb(p, current, total):
            log.append( (p, current, total) )
        p = encode.MultipartParam("foo", "bar", cb=cb)
        boundary = "XYZXYZXYZ"

        datagen, headers = encode.multipart_encode([p], boundary)

        b"".join(datagen)

        l = p.get_size(boundary)
        self.assertEquals(log[-1], (p, l, l))

    def test_MultipartParam_file_cb(self):
        log = []
        def cb(p, current, total):
            log.append( (p, current, total) )
        p = encode.MultipartParam("foo", fileobj=open(__file__, 'rb'),
                cb=cb)
        boundary = encode.gen_boundary()

        content = b''.join(list(p.iter_encode(boundary)))

        l = p.get_size(boundary)
        self.assertEquals(log[-1], (p, l, l))

    def test_multipart_encode_cb(self):
        log = []
        def cb(p, current, total):
            log.append( (p, current, total) )
        p = encode.MultipartParam("foo", "bar")
        boundary = "XYZXYZXYZ"

        datagen, headers = encode.multipart_encode([p], boundary, cb=cb)

        b"".join(datagen)

        l = int(headers['Content-Length'])
        self.assertEquals(log[-1], (None, l, l))

class TestGenBoundary(TestCase):
    def testGenBoundary(self):
        boundary1 = encode.gen_boundary()
        boundary2 = encode.gen_boundary()

        self.assertNotEqual(boundary1, boundary2)
        self.assert_(len(boundary1) > 0)
