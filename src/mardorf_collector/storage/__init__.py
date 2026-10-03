"""Provider-neutral immutable object transport; no weather interpretation."""
from .objects import ObjectRef, ObjectError, LocalObjects, B2Objects
__all__ = ['ObjectRef', 'ObjectError', 'LocalObjects', 'B2Objects']
