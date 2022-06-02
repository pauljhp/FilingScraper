
"""Utilities class"""

from typing import Union, Optional, List, Dict, Any
from types import SimpleNamespace
import itertools

__all__ = ['has_method', 'config']


def has_method(obj: Any, method: str, default: Any):
    """
    Check if a method is implemented in a class. DONT USE this if you just want
        to check if attribute exists, use builtin hasattr() instead.
    :param cls: The class to check.
    :param method: The method to check.
    :param default: The default value to return if the method is not implemented.
    """
    
    if hasattr(obj, method):
        if callable(getattr(obj, method)):
            return getattr(obj, method)
    else:
        return default

def iter_by_chunk(iterable: Any, chunk_size: int):
    """iterate by chunk size"""
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, chunk_size))
        if not chunk:
            break
        yield chunk

class config(SimpleNamespace):
    def __init__(self, *args, **kwargs):
        super(config, self).__init__(*args, **kwargs)
    def get(self, name, returntype: Union[None, str]='dict', 
        default: Any=None, ):
        """
        :param name: The name of the attribute to get.
        :param returntype: The typpe of the returned attribute. Will force 
            returned object into the specified type.
            Takes strings 'dict' and 'bool'
        :param default: The default value to return if the attribute is not found
        """
        if hasattr(self, name):
            res = getattr(self, name)
            if returntype == 'bool':
                return True if res else False
            elif returntype == 'dict':
                return vars(res)
            elif returntype == 'list':
                return list(res)
            elif returntype == 'str':
                return str(res)
            elif returntype == 'int':
                return int(res)
            elif returntype == 'float':
                return float(res)
            else:
                raise NotImplementedError("returntype must be 'dict' or 'bool'")
        else:
            return default