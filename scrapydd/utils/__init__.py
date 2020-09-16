from distutils.util import strtobool


def str2bool(v):
    return bool(strtobool(v))


def ensure_binary(s):
    if isinstance(s, str):
        return bytearray(s, 'utf8')
    if isinstance(s, bytes):
        return s

    raise Exception('Not supported type %s' % type(s))