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
    self.assertEqual('{"i":1,"s":9}\n{"a":"b"}{"i":1,"close":true}\n',
                     out.getvalue())

  def testInput(self):
    out = StringIO()
    pipe = chunked.ChunkedPipe(open('basic.in', 'r'), out)

    for stream in pipe:
      for o in stream:
        stream.Write({'found': o})
      stream.Close()

    pipe.Join()
    self.assertEqual('{"i":1,"s":19}\n{"found":{"a":"b"}}'
                     '{"i":1,"s":19}\n{"found":{"a":"b"}}'
                     '{"i":1,"s":19}\n{"found":{"a":"b"}}'
                     '{"i":1,"close":true}\n', out.getvalue())

  def testInterleavedInputs(self):
    out = StringIO()
    pipe = chunked.ChunkedPipe(open('interleaved.in', 'r'), out)
    for stream in pipe:
      for o in stream:
        stream.Write(o)
      stream.Close()

    pipe.Join()
    self.assertEqual('{"i":1,"s":7}\n{"a":1}'
                     '{"i":1,"s":7}\n{"a":1}'
                     '{"i":1,"close":true}\n'
                     '{"i":2,"s":7}\n{"a":2}'
                     '{"i":2,"s":7}\n{"a":2}'
                     '{"i":2,"close":true}\n'
                     '{"i":3,"s":7}\n{"a":3}'
                     '{"i":3,"close":true}\n', out.getvalue())

if __name__ == '__main__':
  unittest.main()
