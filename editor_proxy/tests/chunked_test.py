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

  def testBasic(self):
    out = StringIO()
    pipe = chunked.ChunkedPipe(open('basic.in', 'r'), out)

    for stream in pipe:
      for o in stream:
        stream.Write({'found': o})

    self.assertEqual("", out.getvalue())

if __name__ == '__main__':
  unittest.main()
