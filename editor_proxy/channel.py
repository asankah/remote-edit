"""TODO(asanka): DO NOT SUBMIT without one-line documentation for channel.

TODO(asanka): DO NOT SUBMIT without a detailed description of channel.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from threading import Condition
from collections import deque

import logging


class Channel:

  def __init__(self, name=None):
    self.cond = Condition()
    self.queue = deque()
    self.done = False
    self.name = name

  def __iter__(self):
    return self

  def next(self):
    o = self.Get()
    if o is None:
      raise StopIteration
    logging.debug("%s returning %s", repr(self), repr(o))
    return o

  def Put(self, o):
    if o is None:
      raise ValueError("'None' is not a value datum")

    logging.debug("%s adding %s", repr(self), repr(o))
    with self.cond:
      self.queue.append(o)
      self.cond.notify()

  def Get(self):
    with self.cond:
      while len(self.queue) == 0 and not self.done:
        self.cond.wait()

      o = self.queue.popleft()
      if o is None:
        self.done = True
    return o

  def Close(self):
    with self.cond:
      self.queue.append(None)
      self.cond.notify()

  def __repr__(self):
    return "Channel({})".format(repr(self.name))
