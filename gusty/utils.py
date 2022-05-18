from pendulum import today


def days_ago(n):
    """Get a datetime object representing n days ago."""
    n = -1 * n
    return today("UTC").add(days=n)
