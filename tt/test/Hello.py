#!/usr/bin/env python
# -*- coding: utf-8 -*-

'test module'
__author__ = "make"
import sys

def test() :
    args = sys.argv
    if len(args) == 1 :
        print args[0]
    elif len(args) == 2:
        print args[0] + " " + args[1]
    else:
        print "too many arguments!"

if __name__ == '__main__':
    test()

