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

  def __init__(self, stream, environ, app):
    self.stream = stream
    self.environ = environ
    self.app = app

  def run(self):

    headers_set = []
    headers_sent = []

    def write(data):
      if not headers_sent:
        d = dict()
        d['st'], d['h'] = headers_set[:] = headers_set
        self.stream.Write(d)

      d = dict()
      d['d'] = data
      self.stream.Write(d)

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
      self.stream.Close()


class PipeServer:

  def __init__(self, app, pipe):
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
    self.pipe = pipe

  def DispatchRequest(self, stream):
    environ = dict(self.base_environ.items())
    environ['wsgi.input'] = StringIO(data)
    environ['REQUEST_METHOD'] = h.get('m', 'GET')
    environ['PATH_INFO'] = h.get('p', '/')
    environ['QUERY_STRING'] = h.get('q', '')
    environ['CONTENT_LENGTH'] = len(data)
    environ['pipe.index'] = h.get('i', -1)
    t = PipeRequestHandler(stream, environ, self.app)
    t.start()

  def Run(self):
    for stream in self.pipe:
      self.DispatchRequest(stream)
    self.pipe.Close()
