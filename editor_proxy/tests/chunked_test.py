"""Tests for chunked."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from StringIO import StringIO
import os
import sys
import unittest

DIR_OF_CURRENT_SCRIPT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(
    0, os.path.normpath(os.path.join(DIR_OF_CURRENT_SCRIPT, '..', '..')))

from editor_proxy import chunked


class ChunkedTest(unittest.TestCase):

  def testOutput(self):
    out = StringIO()
    inp = StringIO('')
    pipe = chunked.ChunkedPipe(inp, out)
    stream = pipe.CreateStream(1)
    stream.Write({'a': 'b'})
    stream.Close()

    pipe.Join()
    self.assertEqual('{"i":1,"s":9}\n{"a":"b"}{"i":1,"close":true}\n',
                     out.getvalue())

  def testInput(self):
    out = StringIO()
    pipe = chunked.ChunkedPipe(open('streams/basic.in', 'r'), out)

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
    pipe = chunked.ChunkedPipe(open('streams/interleaved.in', 'r'), out)
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

  def testChunkedFileStreamWrite(self):
    inp = StringIO('')
    out = StringIO()
    pipe = chunked.ChunkedPipe(inp, out)
    fs1 = chunked.ChunkedFileStream(pipe.CreateStream(6))
    fs2 = chunked.ChunkedFileStream(
        pipe.CreateStream(7), output_filter=lambda x: {'e': x})

    try:
      fs1.write('Hello')
      fs1.writelines(['a', 'b', 'c'])
      fs2.write('a')

    finally:
      fs1.close()
      fs2.close()
      pipe.Join()

    self.assertEqual('{"i":6,"s":15}\n{"out":"Hello"}'
                     '{"i":6,"s":11}\n{"out":"a"}'
                     '{"i":6,"s":11}\n{"out":"b"}'
                     '{"i":6,"s":11}\n{"out":"c"}'
                     '{"i":7,"s":9}\n{"e":"a"}'
                     '{"i":6,"close":true}\n'
                     '{"i":7,"close":true}\n', out.getvalue())

  def testChunkedFileStreamRead(self):
    inp = StringIO(
        '{"i":6,"s":"l"}\n{"d":"abcd"}\n'
        '{"i":6,"s":"l"}\n{"d":"efgh"}\n'
    )
    out = StringIO()

    pipe = chunked.ChunkedPipe(inp, out)

    try:
      for stream in pipe:
        try:
          fs = chunked.ChunkedFileStream(stream)
          self.assertEqual("abcd", fs.read())
          self.assertEqual("efgh", fs.read())
        finally:
          fs.close()

    finally:
      pipe.Join()

  def testChunkedFileStreamIter(self):
    inp = StringIO(
        '{"i":6,"s":"l"}\n{"d":"abcd"}\n'
        '{"i":6,"s":"l"}\n{"d":"efgh"}\n'
    )
    out = StringIO()

    pipe = chunked.ChunkedPipe(inp, out)

    try:
      for stream in pipe:
        try:
          fs = chunked.ChunkedFileStream(stream)
          buf = ''
          for s in fs:
            buf += s
          self.assertEqual("abcdefgh", buf)
        finally:
          fs.close()

    finally:
      pipe.Join()

  def testChunkedFileStreamReadLine(self):
    inp = StringIO(
        '{"i":6,"s":"l"}\n{"d":"abcd\\nefgh\\n"}\n'
        '{"i":6,"s":"l"}\n{"d":"x"}\n'
        '{"i":6,"s":"l"}\n{"d":"y"}\n'
        '{"i":6,"s":"l"}\n{"d":"z\\na"}\n'
        '{"i":6,"s":"l"}\n{"d":"\\nb"}\n'
    )
    out = StringIO()

    pipe = chunked.ChunkedPipe(inp, out)

    try:
      for stream in pipe:
        try:
          fs = chunked.ChunkedFileStream(stream)
          self.assertEqual('abcd\n', fs.readline())
          self.assertEqual('efgh\n', fs.readline())
          self.assertEqual('xyz\n', fs.readline())
          self.assertEqual('a\n', fs.readline())
          self.assertEqual('b', fs.readline())
          self.assertEqual('', fs.readline())
          self.assertEqual('', fs.readline())
        finally:
          fs.close()

    finally:
      pipe.Join()

if __name__ == '__main__':
  unittest.main()
