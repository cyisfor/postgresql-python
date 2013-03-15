def decodeLeaf(s):
    if not s: return None
    if s[0]=="'": return s[1:-1]
    try: return int(s)
    except ValueError:
        try: return float(s)
        except ValueError: pass

def decodeArray(v):
    v = iter(v)
    result = []
    pending = ''
    for c in v:
        if c is '{':
            result.append(decodeArray(v))
        elif c is ',':
            result.append(decodeLeaf(pending))
        else:
            pending += c

def decode(v):
    assert(v[0]=='{')
    return decodeArray(v[1:])
