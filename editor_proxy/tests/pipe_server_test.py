"""Tests for pipe_server."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
import unittest
from StringIO import StringIO

DIR_OF_CURRENT_SCRIPT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(
    0, os.path.normpath(os.path.join(DIR_OF_CURRENT_SCRIPT, '..', '..')))
sys.path.insert(
    0,
    os.path.normpath(
        os.path.join(DIR_OF_CURRENT_SCRIPT, '..', '..', 'third_party', 'ycmd',
                     'third_party', 'bottle')))

from bottle import get, post, request, Response, Bottle, debug
from editor_proxy.pipe_server import PipeServer
from editor_proxy.chunked import ChunkedPipe

debug(True)  # Enables debugging in Bottle.

class PipeServerTest(unittest.TestCase):

  def testEcho(self):
    app = Bottle()

    @app.post('/echo')
    def Echo():
      s = request.body.readline()
      self.assertEqual('abc', s)
      return Response(
          body=s,
          headers=[('Content-Type', 'application/json')],
          status='200 OK')

    out = StringIO()
    pipe = PipeServer(app, ChunkedPipe(open('streams/post_echo.in', 'r'), out))
    pipe.Run()

    with open('streams/post_echo.expected', 'r') as f:
      expected = f.read()

    self.assertEqual(expected, out.getvalue())


if __name__ == '__main__':
  unittest.main()
