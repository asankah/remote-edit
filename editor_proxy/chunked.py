"""TODO(asanka): DO NOT SUBMIT without one-line documentation for chunked.

TODO(asanka): DO NOT SUBMIT without a detailed description of chunked.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json

from .channel import Channel
from threading import Thread, Lock

THREAD_TIMEOUT=1.0

class NewStreamError(Exception):
  def __init__(self, index, body):
    self.index = index
    self.body = body

class ChunkedPipe:
  def __init__(self, input_file, output_file):
    self.stream_channel = Channel()
    self.input_thread = InputDispatchThread(input_file, self, self.stream_channel)
    self.input_thread.start()
    self.output_thread = OutputDispatchThread(output_file)
    self.output_thread.start()

  def Close(self):
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

  def CreateStream(self, index):
    stream = ChunkedStream(self, index)
    self.input_thread.Attach(stream)
    return stream

  def CloseStream(self, stream):
    self.output_thread.Close(stream)
    self.input_thread.Close(stream)

class OutputDispatchThread(Thread):
  def __init__(self, output_file):
    super(OutputDispatchThread, self).__init__()
    self.output_file = output_file
    self.channel = Channel()
    self.daemon = True

  def Quit(self):
    self.channel.Close()

  def Close(self, stream):
    self.Write(stream, None)

  def Write(self, stream, body):
    v = (stream.index, body)
    self.channel.Put(v)

  def run(self):
    for (index, body) in self.channel:

      if body is None:
        d = dict()
        d['i'] = index
        d['close'] = True
        self.output_file.write(json.dumps(d) + '\n')
        self.output_file.flush()
        continue

      data = json.dumps(body)
      d = dict()
      d['s'] = len(data)
      d['i'] = index
      self.output_file.write(json.dumps(d) + '\n')
      self.output_file.write(data)
      self.output_file.flush()

class InputDispatchThread(Thread):
  def __init__(self, input_file, pipe, stream_channel):
    super(InputDispatchThread, self).__init__()
    self.input_file = input_file
    self.pipe = pipe
    self.lock = Lock()
    self.streams = {}
    self.stream_channel = stream_channel
    self.daemon = True

  def Quit(self):
    self.stream_channel.Close()

  def Attach(self, stream):
    with self.lock:
      self.streams[stream.index] = stream

  def Close(self, stream):
    if not isinstance(stream, ChunkedStream):
      raise ValueError("Invalid stream.")
    with self.lock:
      del self.streams[stream.index]

  def run(self):
    line = self.input_file.readline()
    if line == '':
      self.stream_channel.Close()
      return

    v = json.loads(line)
    if not v or 'i' not in v:
      raise IOError("Input malformed: [{}]".format(line))

    if 'close' in v:
      body = None
    elif 's' not in v:
      raise IOError("Input malformed: [{}]".format(line))
    else:
      body = json.loads(self.input_file.read(v.get('s')))

    with self.lock:
      if v.get('i') not in self.streams:
        stream = ChunkedStream(self.pipe, v.get('i'))
        self.streams[v.get('i')] = stream
        self.stream_channel.Put(stream)
      else:
        stream = self.streams[v.get('i')]

    stream.channel.Put(body)

class ChunkedStream:
  def __init__(self, pipe, index):
    self.pipe = pipe
    self.index = index
    self.channel = Channel()

  def __iter__(self):
    return self

  def next(self):
    o = self.Read()
    if o is None:
      raise StopIteration
    return o

  def Close(self):
    self.pipe.CloseStream(self)

  def Read(self):
    return self.channel.Get()

  def Write(self, obj):
    self.pipe.WriteStream(self, obj)


class ChunkedFileStream:
  def __init__(self, stream):
    self.stream = stream

  def close(self):
    self.stream.Close();

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
    d = dict()
    d['std'] = buf
    self.stream.Write(d)

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
    return False

  def encoding(self):
    return 'utf8'

  def mode(self):
    return 'rw'

  def name(self):
    return ''

  def newlines(self):
    return None

