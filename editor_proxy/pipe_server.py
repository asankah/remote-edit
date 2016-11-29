"""TODO(asanka): DO NOT SUBMIT without one-line documentation for pipe_server.

TODO(asanka): DO NOT SUBMIT without a detailed description of pipe_server.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import json
from StringIO import StringIO
from threading import Lock, Thread
from collections import deque

class PipeRequestHandler(Thread):
  def __init__(self, outlock, environ, app):
    self.outlock = outlock
    self.environ = environ
    self.app = app
    self.daemon = True

  def WriteFrame(self, data):
    header = json.dumps({'s': len(data), 'i': self.environ.get('pipe.index')})
    with self.outlock:
      sys.stdout.write(header + '\n')
      sys.stdout.write(data)
      sys.stdout.flush()

  def run(self):

    headers_set = []
    headers_sent = []

    def write(data):
      if not headers_sent:
        d = dict()
        d['st'], d['h'] = headers_set[:] = headers_set
        self.WriteFrame(json.dumps(d))

      d = dict()
      d['d'] = data
      self.WriteFrame(json.dumps(d))

    def start_response(status, response_headers, exc_info=None):
      if exc_info:
        try:
          if headers_sent:
            raise exc_info[0], exc_info[1], exc_info[2]
        finally:
          exc_info = None

      headers_set[:] = [status, response_headers]
      return write

    result = self.app(self.environ, start_response)
    try:
      for data in result:
        if data:
          write(data)
      if not headers_sent:
        write('')
    finally:
      if hasattr(result, 'close'):
        result.close()


class PipeServer:
  def __init__(self, app, infile, outfile):
    self.app = app
    self.request_map = {}
    self.base_environ = {
        'wsgi.version': (1, 0),
        'wsgi.multithread': True,
        'wsgi.multiprocess': False,
        'wsgi.run_once': False,
        'wsgi.url_scheme': 'http',
        'SCRIPT_NAME': '',
        'CONTENT_TYPE': 'application/json',
        'SERVER_PROTOCOL': 'HTTP/1.1',
        'SERVER_NAME': 'localhost',
        'SERVER_PORT': '65535',
        'HTTP_HOST': 'localhost'
    }
    self.input = infile
    self.output = outfile
    self.output_lock = Lock()

  def DispatchRequest(self, h, data):
    environ = dict(self.base_environ.items())
    environ['wsgi.input'] = StringIO(data)
    environ['REQUEST_METHOD'] = h.get('m', 'GET')
    environ['PATH_INFO'] = h.get('p', '/')
    environ['QUERY_STRING'] = h.get('q', '')
    environ['CONTENT_LENGTH'] = len(data)
    environ['pipe.index'] = h.get('i', -1)
    t = PipeRequestHandler(self.output_lock, environ, self.app)
    t.start()

  def Run(self):
    for line in self.input:
      h = json.loads(line)
      if ('s' not in h) or ('i' not in h):
        sys.stderr.write('Request malformed: "{}"\n'.format(line))
        return

      data = self.input.read(h.s)
      self.DispatchRequest(h, data)


