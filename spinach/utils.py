def human_duration(duration_seconds: float) -> str:
    """Convert a duration in seconds into a human friendly string."""
    if duration_seconds < 0.001:
        return '0 ms'
    if duration_seconds < 1:
        return '{} ms'.format(int(duration_seconds * 1000))
    return '{} s'.format(int(duration_seconds))
