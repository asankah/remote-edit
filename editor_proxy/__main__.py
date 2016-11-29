"""TODO(asanka): DO NOT SUBMIT without one-line documentation for __main__.

TODO(asanka): DO NOT SUBMIT without a detailed description of __main__.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
import argparse

DIR_OF_CURRENT_SCRIPT = os.path.dirname(os.path.abspath(__file__))

sys.path.insert(0, DIR_OF_CURRENT_SCRIPT)
sys.path.insert(
    0,
    os.path.normpath(
        os.path.join(DIR_OF_CURRENT_SCRIPT, '..', 'third_party', 'ycmd')))

from ycmd.server_utils import SetUpPythonPath
SetUpPythonPath()

import logging
import atexit
import signal
import json

from ycmd import extra_conf_store, user_options_store, utils
from ycmd.utils import ToBytes, ReadFile, OpenForStdHandle
from ycmd.server_utils import CompatibleWithCurrentCore
from .pipe_server import PipeServer
from .chunked import ChunkedPipe, ChunkedFileStream
from bottle import debug


def SetupLogging(log_level):
  numeric_level = getattr(logging, log_level.upper(), None)
  if not isinstance(numeric_level, int):
    raise ValueError('Invalid log level: %s' % log_level)

  # Has to be called before any call to logging.getLogger()
  logging.basicConfig(
      format='%(asctime)s - %(levelname)s - %(message)s', level=numeric_level)
  logging.info("Starting log")

  if log_level == 'debug':
    debug(True)


def ParseArguments():
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--log',
      type=str,
      default='info',
      help='Log level. One of [debug|info|warning|error|critical]')
  parser.add_argument(
      '--options_file',
      type=str,
      default=None,
      help='File with user options, in JSON format')
  parser.add_argument(
      '--check_interval_seconds',
      type=int,
      default=600,
      help='interval in seconds to check server '
      'inactivity and keep subservers alive')
  return parser.parse_args()


def SetUpSignalHandler():

  def SignalHandler(signum, frame):
    sys.exit()

  for sig in [signal.SIGTERM, signal.SIGINT]:
    signal.signal(sig, SignalHandler)


def SetupOptions(options_file):
  options = user_options_store.DefaultOptions()
  if options_file is not None:
    user_options = json.loads(ReadFile(options_file))
    options.update(user_options)
    utils.RemoveIfExists(options_file)
  user_options_store.SetAll(options)
  return options


def YcmCoreSanityCheck():
  if 'ycm_core' in sys.modules:
    raise RuntimeError('ycm_core already imported, ycmd has a bug!')


def OpenStdPipe():
  global _pipe
  _pipe = ChunkedPipe(sys.stdin, sys.stdout)
  stdiofile = ChunkedFileStream(_pipe.CreateStream(0, out_of_band=True))
  stderrfile = ChunkedFileStream(_pipe.CreateStream(1, out_of_band=True), out_token='stderr')

  sys.stdin = stdiofile
  sys.stdout = stdiofile
  sys.stderr = stderrfile

  return _pipe


def Main():
  args = ParseArguments()

  SetupLogging(args.log)
  options = SetupOptions(args.options_file)

  YcmCoreSanityCheck()
  extra_conf_store.CallGlobalExtraConfYcmCorePreloadIfExists()

  code = CompatibleWithCurrentCore()
  if code:
    sys.exit(code)

  # These can't be top-level imports because they transitively import
  # ycm_core which we want to be imported ONLY after extra conf
  # preload has executed.
  from ycmd import handlers
  handlers.UpdateUserOptions(options)
  handlers.KeepSubserversAlive(args.check_interval_seconds)
  SetUpSignalHandler()

  atexit.register(handlers.ServerCleanup)

  pipe = OpenStdPipe()
  handlers.wsgi_server = PipeServer(handlers.app, pipe)
  handlers.wsgi_server.Run()


if __name__ == '__main__':
  Main()
