"""TODO(asanka): DO NOT SUBMIT without one-line documentation for pipe_server.

TODO(asanka): DO NOT SUBMIT without a detailed description of pipe_server.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import json
import logging
from StringIO import StringIO
from threading import Lock, Thread
from collections import deque
from .chunked import ChunkedFileStream


class PipeRequestHandler(Thread):

  def __init__(self, stream, environ, app):
    super(PipeRequestHandler, self).__init__()
    self.stream = stream
    self.environ = environ
    self.app = app

    self.status = None
    self.headers = None
    self.headers_sent = False
    self.buffered_body = ''
    self.buffering = False

  def SendHeadersIfNotBuffering(self):
    if self.headers_sent or self.buffering:
      return

    self.headers_sent = True
    d = dict()
    d['st'] = self.status
    d['h'] = self.headers
    self.stream.Write(d)

  def OnStartResponse(self, status, headers, exc_info):
    logging.debug('start_response(%s, %s)', repr(status), repr(headers))
    if exc_info:
      try:
        if self.headers_sent:
          raise exc_info[0], exc_info[1], exc_info[2]
      finally:
        exc_info = None

    self.status = status
    self.headers = headers

    if ('Content-Type', 'application/json') in self.headers:
      self.buffering = True

    def write(data):
      return self.OnWrite(data)

    return write

  def OnWrite(self, data):
    self.SendHeadersIfNotBuffering()

    if self.buffering:
      self.buffered_body += data
      return

    d = dict()
    d['d'] = data
    self.stream.Write(d)

  def run(self):

    try:
      head = self.stream.Read()
      logging.debug('Received headers %s', repr(head))

      assert head is not None, 'Unexpected EOF while reading request header'
      assert 'p' in head, '"p" not found in header object: {}'.format(
          repr(head))

      if 'd' in head:
        bodystream = StringIO(body.get('d', ''))
      elif 'o' in head:
        bodystream = StringIO(json.dumps(body.get('o')))
      else:
        bodystream = ChunkedFileStream(self.stream)

    except:
      self.stream.Close()
      return

    environ = dict(self.environ.items())
    environ['wsgi.errors'] = sys.stderr
    environ['wsgi.input'] = bodystream
    environ['REQUEST_METHOD'] = head.get('m', 'GET')
    environ['PATH_INFO'] = head.get('p', '/')
    environ['QUERY_STRING'] = head.get('q', '')
    environ['CONTENT_LENGTH'] = len(data)

    for (k, v) in head.get('h', []):
      key_with_underscores = k.upper().replace('-', '_')
      environ['HTTP_{}'.format(key_with_underscores)] = v

    def start_response(s, h, e=None):
      return self.OnStartResponse(s, h, e)

    result = self.app(environ, start_response)
    try:

      for data in result:
        if data:
          self.OnWrite(data)
      if not self.headers_sent:
        self.OnWrite('')

      if self.buffering:
        d = dict()
        d['st'] = self.status
        d['h'] = self.headers
        d['o'] = json.loads(self.buffered_body)
        self.stream.Write(d)

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
    t = PipeRequestHandler(stream, self.base_environ, self.app)
    t.start()

  def Shutdown(self):
    pass

  def Run(self):
    for stream in self.pipe:
      logging.debug('Starting %s', repr(stream))
      self.DispatchRequest(stream)
    self.pipe.Close()
