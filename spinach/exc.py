class SpinachError(Exception):
    """Base class for other Spinach exceptions."""


class UnknownTask(SpinachError):
    """Task name is not registered with the Engine."""
