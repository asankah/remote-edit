"""Tests for channel."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
import unittest

DIR_OF_CURRENT_SCRIPT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(
    0, os.path.normpath(os.path.join(DIR_OF_CURRENT_SCRIPT, '..', '..')))

from threading import Thread
from editor_proxy.channel import Channel


class ChannelTest(unittest.TestCase):

  def testReadWrite(self):
    c = Channel()
    c.Put(1)
    self.assertEqual(1, c.Get())
    c.Close()

  def testReadAfterClose(self):
    c = Channel()
    c.Close()
    self.assertEqual(None, c.Get())

  def testGetAfterEOF(self):
    c = Channel()
    c.Close()
    self.assertEqual(None, c.Get())
    self.assertEqual(None, c.Get())

  def testOffThreadWrite(self):
    c = Channel()

    def WriteStuff():
      c.Put(1)

    t = Thread(target=WriteStuff)
    t.start()

    self.assertEqual(1, c.Get())
    c.Close()

  def testOffThreadWrites(self):
    c = Channel()

    def WriteStuff():
      c.Put(1)
      c.Put(2)
      c.Put(3)

    t = Thread(target=WriteStuff)
    t.start()

    self.assertEqual(1, c.Get())
    self.assertEqual(2, c.Get())
    self.assertEqual(3, c.Get())
    c.Close()

  def testOffThreadRead(self):
    c = Channel()
    r = []

    def ReadStuff():
      for x in range(3):
        r.append(c.Get())

    t = Thread(target=ReadStuff)
    t.start()

    c.Put(1)
    c.Put(2)
    c.Put(3)

    t.join()
    self.assertSequenceEqual([1, 2, 3], r)
    c.Close()

  def testNoneIsInvalid(self):
    with self.assertRaises(ValueError):
      c = Channel()
      c.Put(None)

  def testIter(self):
    c = Channel()
    c.Put(1)
    c.Close()
    for v in c:
      self.assertEqual(1, v)

  def testIterMultipleValues(self):
    c = Channel()
    for x in range(10):
      c.Put(x)
    c.Close()

    self.assertSequenceEqual(range(10), [x for x in c])


if __name__ == '__main__':
  unittest.main()
