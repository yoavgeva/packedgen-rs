import pytest
import opthash

MAPS = [opthash.ElasticHashMap, opthash.FunnelHashMap]
SETS = [opthash.ElasticHashSet, opthash.FunnelHashSet]


class RaisingKey:
    armed = False

    def __init__(self, value):
        self.value = value

    def __hash__(self):
        return 42

    def __eq__(self, other):
        if self.armed:
            raise ValueError("comparison failed")
        return self.value == other.value


@pytest.mark.parametrize("cls", [dict] + MAPS)
@pytest.mark.parametrize("operation", ["insert", "contains", "getitem", "get", "delete", "pop", "setdefault", "update"])
def test_key_equality_errors(cls, operation):
    k1, k2 = RaisingKey(1), RaisingKey(1)
    m = cls()
    m[k1] = 1
    k1.armed = True
    with pytest.raises(ValueError, match="comparison failed"):
        if operation == "insert":
            m[k2] = 2
        elif operation == "contains":
            k2 in m
        elif operation == "getitem":
            m[k2]
        elif operation == "get":
            m.get(k2)
        elif operation == "delete":
            del m[k2]
        elif operation == "pop":
            m.pop(k2)
        elif operation == "setdefault":
            m.setdefault(k2, 2)
        else:
            m.update([(k2, 2)])
    assert len(m) == 1
    assert m[k1] == 1


@pytest.mark.parametrize("cls", [set] + SETS)
@pytest.mark.parametrize("operation", ["add", "contains", "remove", "discard", "update", "difference_update", "intersection_update", "symmetric_difference_update"])
def test_set_key_equality_errors(cls, operation):
    k1, k2 = RaisingKey(1), RaisingKey(1)
    s = cls([k1])
    k1.armed = k2.armed = True
    with pytest.raises(ValueError, match="comparison failed"):
        if operation == "contains":
            k2 in s
        elif operation in ("add", "remove", "discard"):
            getattr(s, operation)(k2)
        else:
            getattr(s, operation)([k2])
    assert len(s) == 1
    assert k1 in s
