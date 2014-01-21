leftBrace = b'{'[0]
rightBrace = b'}'[0]
quote = b"'"[0]
doublequote = b'"'[0]
comma = b','[0]

def decodeString(s):
    if not s: return None
    if s[0] is quote: return s[1:-1].decode('utf-8')
    if s[0] is doublequote: return s[1:-1].decode('utf-8')
    return s.decode('utf-8')

def decodeArray(v,decodeLeaf):
    if v == b'}': return []
    v = iter(v)
    result = []
    pending = b''
    for c in v:
        if c is leftBrace:
            result.append(decodeArray(v))
        elif c is rightBrace:
            result.append(decodeLeaf(pending))
            return result
        elif c is comma:
            result.append(decodeLeaf(pending))
            pending = b''
        else:
            pending += bytes((c,))
    return result

def decode(v,decodeLeaf):
    assert(v[0]==leftBrace)
    if v == b'{NULL}': return ()
    return decodeArray(v[1:],decodeLeaf)
