"""TODO(asanka): DO NOT SUBMIT without one-line documentation for chunked.

TODO(asanka): DO NOT SUBMIT without a detailed description of chunked.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import logging

from .channel import Channel
from threading import Thread, Lock, Event
from StringIO import StringIO

THREAD_TIMEOUT = 1.0


class NewStreamError(Exception):

  def __init__(self, index, body):
    self.index = index
    self.body = body


class ChunkedPipe:

  def __init__(self, input_file, output_file):
    assert hasattr(input_file, 'read')
    assert hasattr(input_file, 'readline')
    assert hasattr(output_file, 'write')
    assert hasattr(output_file, 'flush')

    self.active_channels = set()
    self.quit_event = Event()

    self.stream_channel = Channel(name='ChunkedPipe')
    self.output_thread = OutputDispatchThread(output_file)
    self.input_thread = InputDispatchThread(input_file, self)

    self.output_thread.start()
    self.input_thread.start()

  def Close(self):
    self.quit_event.wait()
    self.input_thread.Quit()
    self.output_thread.Quit()

  def Join(self):
    self.Close()
    self.input_thread.join(THREAD_TIMEOUT)
    self.output_thread.join(THREAD_TIMEOUT)

  def __iter__(self):
    return self.stream_channel.__iter__()

  def WriteStream(self, stream, obj):
    self.output_thread.Write(stream, obj)

  def CreateStream(self, index, out_of_band=False):
    logging.debug('Creating stream with index {}'.format(index))
    stream = ChunkedStream(self, index)
    self.input_thread.Attach(stream)
    self.output_thread.Attach(stream)

    if not out_of_band:
      assert stream.index not in self.active_channels, (
          'Stream id {} is '
          'already in use').format(stream.index)
      self.stream_channel.Put(stream)
      self.active_channels.add(stream.index)
      self.quit_event.clear()

    return stream

  def CloseStream(self, stream):
    self.output_thread.Close(stream)
    self.input_thread.Close(stream)

    if stream.index in self.active_channels:
      self.active_channels.remove(stream.index)
      if len(self.active_channels) == 0:
        self.quit_event.set()

  def _InputDrained(self):
    self.stream_channel.Close()


class ChunkedStream:

  def __init__(self, pipe, index):
    self.pipe = pipe
    self.index = int(index)
    self.channel = Channel(name='ChunkedStream')

  def __iter__(self):
    return self.channel.__iter__()

  def Close(self):
    self.channel.Close()
    self.pipe.CloseStream(self)

  def Read(self):
    return self.channel.Get()

  def Write(self, obj):
    self.pipe.WriteStream(self, obj)

  def __repr__(self):
    return 'ChunkedStream(None, {})'.format(self.index)


class OutputDispatchThread(Thread):

  class CreateNewStream:
    pass

  class DeleteExistingStream:
    pass

  def __init__(self, output_file):
    super(OutputDispatchThread, self).__init__()
    self.output_file = output_file
    self.channel = Channel(name='OutputDispatchThread')

  def Quit(self):
    self.channel.Close()

  def Attach(self, stream):
    self.Write(stream, OutputDispatchThread.CreateNewStream())

  def Close(self, stream):
    self.Write(stream, OutputDispatchThread.DeleteExistingStream())

  def Write(self, stream, body):
    v = (stream.index, body)
    self.channel.Put(v)

  def run(self):

    def Serialize(o):
      return json.dumps(o, separators=(',', ':'))

    try:
      active_channels = set()

      for (index, body) in self.channel:

        if isinstance(body, OutputDispatchThread.CreateNewStream):
          active_channels.add(index)
          continue

        if isinstance(body, OutputDispatchThread.DeleteExistingStream):
          active_channels.remove(index)
          d = dict()
          d['i'] = index
          d['close'] = True
          self.output_file.write(Serialize(d) + '\n')
          self.output_file.flush()
          continue

        if index not in active_channels:
          raise ValueError('Writing to inactive channel')

        data = Serialize(body)
        d = dict()
        d['s'] = len(data)
        d['i'] = index
        self.output_file.write(Serialize(d) + '\n')
        self.output_file.write(data)
        self.output_file.flush()

    except ValueError as e:
      raise ValueError('While writing: {}'.format(e.message))
      return

    finally:
      for v in active_channels:
        d = dict()
        d['i'] = v
        d['close'] = True
        self.output_file.write(Serialize(d) + '\n')
      self.output_file.flush()


class InputDispatchThread(Thread):

  def __init__(self, input_file, pipe):
    super(InputDispatchThread, self).__init__()
    self.input_file = input_file
    self.pipe = pipe
    self.lock = Lock()
    self.streams = {}

  def Quit(self):
    pass

  def Attach(self, stream):
    with self.lock:
      self.streams[stream.index] = stream

  def Close(self, stream):
    if not isinstance(stream, ChunkedStream):
      raise ValueError('Invalid stream.')
    with self.lock:
      if stream.index in self.streams:
        del self.streams[stream.index]

  def run(self):
    line = None
    try:
      while True:
        line = self.input_file.readline()
        if line == '':
          logging.debug('Done with input')
          return

        # Stray newlines?
        if line.strip() == '' or line == '\n':
          continue

        logging.debug('Header %s', repr(line))
        v = json.loads(line)
        if not v or 'i' not in v:
          raise IOError('Input malformed: [{}]'.format(line))

        if 'close' in v:
          body = None
        elif 's' not in v:
          raise IOError('Input malformed: [{}]'.format(line))
        elif v.get('s') == 'l':
          line = self.input_file.readline()
          body = json.loads(line)
        else:
          line = self.input_file.read(int(v.get('s')))
          body = json.loads(line)

        logging.debug('Payload from input: %s', repr(body))
        stream = None
        stream_id = int(v.get('i'))
        with self.lock:
          if stream_id in self.streams:
            stream = self.streams[stream_id]

        if stream is None:
          stream = self.pipe.CreateStream(stream_id)

        if body is None:
          stream.channel.Close()
        else:
          stream.channel.Put(body)

    except ValueError as v:
      logging.exception("Can't process input stream.")
      return

    finally:
      with self.lock:
        streams = self.streams.values()
      for stream in streams:
        stream.channel.Close()
      self.pipe._InputDrained()


class ChunkedFileStream:

  def __init__(self, stream, input_filter=None, output_filter=None):

    def d(o):
      assert 'd' in o, 'No "d" field in received object {}.'.format(repr(o))
      return o.get('d')

    self.stream = stream
    self.output_filter = output_filter
    self.input_filter = input_filter if input_filter else d
    self.buffer = ''

  def close(self):
    self.stream.Close()

  def flush(self):
    pass

  def __iter__(self):
    return self

  def next(self):
    s = self.readline()
    if s == '':
      raise StopIteration
    return s

  def read(self, size=-1):
    if len(self.buffer) == 0:
      o = self.stream.Read()
      if o is None:
        return ''

      self.buffer = self.input_filter(o)

    if size == -1 or size >= len(self.buffer):
      s = self.buffer
      self.buffer = ''
      return s
    else:
      s = self.buffer[:size]
      self.buffer = self.buffer[size:]
      return s

  def readline(self, size=-1):
    remaining_size = size
    line_so_far = ''
    while remaining_size == -1 or remaining_size > 0:
      s = self.read(remaining_size)
      if s == '':
        return line_so_far

      line_so_far += s
      if remaining_size > 0:
        remaining_size -= len(s)

      if '\n' in s:
        break

    s = line_so_far
    o = s.find('\n')
    if o == -1 or o == len(s) - 1:
      return s

    self.buffer = s[o + 1:] + self.buffer
    return s[:o + 1]

  def readlines(self, size=-1):
    return list(self)

  def write(self, buf):
    if not self.output_filter:
      d = dict()
      d['out'] = buf
    else:
      d = self.output_filter(buf)
    self.stream.Write(d)

  def writelines(self, seq):
    for line in seq:
      self.write(line)

  def seek(self, offset, whence):
    raise NotImplementedError()

  def tell(self):
    raise NotImplementedError()

  def truncate(self, size):
    raise NotImplementedError()

  def closed(self):
    return False

  def encoding(self):
    return 'utf8'

  def mode(self):
    return 'rw'

  def name(self):
    return ''

  def newlines(self):
    return None
