"""Tests for chunked."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from StringIO import StringIO
import os
import sys
import unittest

DIR_OF_CURRENT_SCRIPT = os.path.dirname( os.path.abspath( __file__ ) )
sys.path.insert( 0, os.path.normpath( os.path.join(
    DIR_OF_CURRENT_SCRIPT, '..', '..' ) ) )

from editor_proxy import chunked

class ChunkedTest(unittest.TestCase):

  def testOutput(self):
    out = StringIO()
    inp = StringIO("")
    pipe = chunked.ChunkedPipe(inp, out)
    stream = pipe.CreateStream(1)
    stream.Write({'a':'b'})
    stream.Close()

    pipe.Join()
    self.assertEqual('{"i": 1, "s": 10}\n{"a": "b"}{"i": 1, "close": true}\n',
                     out.getvalue())

  def testInput(self):
    out = StringIO()
    pipe = chunked.ChunkedPipe(open('basic.in', 'r'), out)

    for stream in pipe:
      print("Found stream")
      for o in stream:
        print("Found object {}".format(o))
        stream.Write({'found': o})
        print("Done writing")
      print("Done with stream")
      stream.Close()
      print("Stream closed")

    pipe.Join()
    self.assertEqual('', out.getvalue())

if __name__ == '__main__':
  unittest.main()
