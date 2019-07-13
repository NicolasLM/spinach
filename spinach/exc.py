class SpinachError(Exception):
    """Base class for other Spinach exceptions."""


class UnknownTask(SpinachError):
    """Task name is not registered with the Engine."""


class InvalidJobSignatureError(SpinachError):
    """Job does not have proper arguments to execute the task function."""
