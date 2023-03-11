import collections.abc
from pendulum import today


def days_ago(n):
    """Get a datetime object representing n days ago."""
    n = -1 * n
    return today("UTC").add(days=n)


def nested_update(d, u):
    d = d.copy()
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = nested_update(d.get(k, {}), v)
        else:
            d[k] = v
    return d
