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


class NewStreamError(Exception):
  def __init__(self, index, body):
    self.index = index
    self.body = body

class ChunkedPipe(self, input, output):
  def __init__(self, input, output):
    self.input = input
    self.output = output
    self.streams_lock = threading.Lock()
    self.streams = {}
    self.dispatch_thread = None

  def Close(self):
    self.input.close()
    if input != output:
      self.output.close()
    self.input = None
    self.output = None

  def __iter__(self):
    return self

  def next(self):
    pass

  def CloseStream(self, stream):
    if not isinstance(stream, ChunkedStream):
      raise ValueError("Invalid stream.")
    with self.dispatch_thread.lock:
      del self.dispatch_thread.streams[stream.index]

def InputDispatchThread:
  def __init__(self, input, pipe):
    self.input = input
    self.pipe = pipe
    self.lock = threading.Lock()
    self.streams = {}

  def run(self):
    line = self.input.readline()
    if line == '':
      raise EOFError()

    v = json.loads(line)
    if not v or 's' not in v or 'i' not in v:
      raise IOError("Input malformed: [{}]".format(line))

    body = self.input.read(v.s)
    with self.lock:
      if v.i not in self.streams:
        self.streams[v.i] = ChunkedStream(self.pipe, v.i)
      stream = self.streams[v.i]
      with stream.cond:
        stream.queue.append(body)
        stream.cond.notify()

class ChunkedStream:
  def __init__(self, pipe, index, initial_body):
    self.pipe = pipe
    self.index = index
    self.cond = threading.Condition()
    self.queue = []

  def close(self):
    self.pipe._Close(self)

  def flush(self):
    pass

  def next(self):
    pass

  def read(self, size = -1):
    pass

  def readline(self, size = -1):
    pass

  def readlines(self, size = -1):
    pass

  def write(self, buf):

  def writelines(self, seq):
    for line in seq:
      self.write(line)

  def seek(self, offset, whence):
    raise NotImplementedError("Can't seek inside a chunked stream.")

  def tell(self):
    raise NotImplementedError("Can't tell offset of a chunked stream.")

  def truncate(self, size):
    raise NotImplementedError("Can't truncate a ChunkedStream.")

  def closed(self):
    return self.input is None

  def encoding(self):
    return 'utf8'

  def mode(self):
    return 'rw'

  def name(self):
    return self.input.name()

  def newlines(self):
    return None


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


