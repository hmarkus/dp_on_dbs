#!/usr/bin/env python3

import os

def absolutizePath(relPath): # paths in config may be relative to root of repository
    return os.path.abspath(os.path.join(os.path.dirname(__file__), relPath))

def absolutizePaths(object): # returns new config in which all paths are absolute
    if isinstance(object, str): # base case
        return object
    else:
        assert isinstance(object, dict), object # config
        absCfg = {} # new config with absolute paths, to be returned
        for (key, value) in object.items():
            assert isinstance(key, str), key
            if key in {"path", "file"}:
                assert isinstance(value, str), value
                absCfg[key] = absolutizePath(value)
            elif isinstance(value, dict):
                absCfg[key] = absolutizePaths(value)
            elif isinstance(value, list): # possibly `list` of `str`
                absCfg[key] = [absolutizePaths(member) for member in value]
            else: # base case
                absCfg[key] = value
        return absCfg
