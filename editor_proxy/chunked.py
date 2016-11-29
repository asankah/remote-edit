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
    self.output_thread = OutputDispatchThread(output_file)
    self.input_thread = InputDispatchThread(input_file, self)
    self.output_thread.start()
    self.input_thread.start()

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
    self.output_thread.Attach(stream)
    self.stream_channel.Put(stream)
    return stream

  def CloseStream(self, stream):
    self.output_thread.Close(stream)
    self.input_thread.Close(stream)

  def _InputDrained(self):
    self.stream_channel.Close()

class OutputDispatchThread(Thread):
  class CreateStream:
    pass
  class DeleteStream:
    pass

  def __init__(self, output_file):
    super(OutputDispatchThread, self).__init__()
    self.output_file = output_file
    self.channel = Channel()
    self.daemon = True

  def Quit(self):
    self.channel.Close()

  def Attach(self, stream):
    self.Write(stream, OutputDispatchThread.CreateStream())

  def Close(self, stream):
    self.Write(stream, OutputDispatchThread.DeleteStream())

  def Write(self, stream, body):
    v = (stream.index, body)
    self.channel.Put(v)

  def run(self):
    def Serialize(o):
      return json.dumps(o, separators=(',',':'))

    try:
      active_channels = set()

      for (index, body) in self.channel:

        if isinstance(body, OutputDispatchThread.CreateStream):
          active_channels.add(index)
          continue

        if isinstance(body, OutputDispatchThread.DeleteStream):
          active_channels.remove(index)
          d = dict()
          d['i'] = index
          d['close'] = True
          self.output_file.write(Serialize(d) + '\n')
          self.output_file.flush()
          continue

        if index not in active_channels:
          raise ValueError("Writing to inactive channel")

        data = Serialize(body)
        d = dict()
        d['s'] = len(data)
        d['i'] = index
        self.output_file.write(Serialize(d) + '\n')
        self.output_file.write(data)
        self.output_file.flush()

    except ValueError:
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
    self.daemon = True

  def Quit(self):
    pass

  def Attach(self, stream):
    with self.lock:
      self.streams[stream.index] = stream

  def Close(self, stream):
    if not isinstance(stream, ChunkedStream):
      raise ValueError("Invalid stream.")
    with self.lock:
      if stream.index in self.streams:
        del self.streams[stream.index]

  def run(self):
    line = None
    try:
      while True:
        line = self.input_file.readline()
        if line == '':
          return

        if line.strip() == '':
          continue

        v = json.loads(line)
        if not v or 'i' not in v:
          raise IOError("Input malformed: [{}]".format(line))

        if 'close' in v:
          body = None
        elif 's' not in v:
          raise IOError("Input malformed: [{}]".format(line))
        else:
          body = json.loads(self.input_file.read(v.get('s')))

        stream = None
        with self.lock:
          if v.get('i') in self.streams:
            stream = self.streams[int(v.get('i'))]

        if stream is None:
          stream = self.pipe.CreateStream(v.get('i'))

        if body is None:
          stream.channel.Close()
        else:
          stream.channel.Put(body)

    except ValueError as v:
      raise ValueError(v.message + " line:[{}]".format(line))

    finally:
      with self.lock:
        streams = self.streams.values()
      for stream in streams:
        stream.channel.Close()
      self.pipe._InputDrained()


class ChunkedStream:
  def __init__(self, pipe, index):
    self.pipe = pipe
    self.index = int(index)
    self.channel = Channel()

  def __iter__(self):
    return self.channel.__iter__()

  def Close(self):
    self.channel.Close()
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

