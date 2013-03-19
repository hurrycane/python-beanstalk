import os
import socket
import sys

from itertools import chain

"""
Native Python Beanstalk client

Key features:
  * connection pool
  * simple and clear implementation

"""

MAX_READ_LENGTH = 1000000
DEFAULT_PRIORITY = 2 ** 31
DEFAULT_TTR = 120

class BeanstalkError(BaseException):
  pass

class ResponseError(BeanstalkError):
  pass

class InvalidResponse(BeanstalkError):
  pass

class InternalError(BeanstalkError):
  pass

class ConnectionError(BeanstalkError):
  pass

class Connection(object):
  def __init__(self, host="localhost", port=11300, socket_timeout=None):
    self.host = host
    self.port = port
    self.socket_timeout = socket_timeout

    self._sock = None

  def connect(self):
    if self._sock:
      return
    try:
      self._sock = self._connect()
      self._fp = self._sock.makefile('rb')
    except socket.error:
      e = sys.exc_info()[1]
      raise ConnectionError(self._error_message(e))

  def _error_message(self, exception):
    """
    Format a exception message from a socket.error
    args for socket.error can either be (errno, "message")
    or just "message"
    """

    if len(exception.args) == 1:
      return "Error connecting to %s:%s. %s." % \
             (self.host, self.port, exception.args[0])
    else:
      return "Error %s connecting %s:%s. %s." % \
             (exception.args[0], self.host, self.port, exception.args[1])

  def disconnect(self):
    """
    Disconnects from the Beanstalk server
    """
    if self._sock is None:
      return
    try:
      self._sock.close()
    except socket.error:
      pass
    self._sock = None

  def _connect(self):
    """
    Create a TCP socket connection
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(self.socket_timeout)
    sock.connect((self.host, self.port))

    return sock

  def send_command(self, command):
    "Send an already packed command to the Redis server"
    if not self._sock:
      self.connect()

    try:
      self._sock.sendall(command)
    except socket.error:
      e = sys.exc_info()[1]
      self.disconnect()
      if len(e.args) == 1:
        _errno, errmsg = 'UNKNOWN', e.args[0]
      else:
        _errno, errmsg = e.args
      raise ConnectionError("Error %s while writing to socket. %s." %
                            (_errno, errmsg))
    except:
      self.disconnect()
      raise

  def _readline(self):
    response = self._fp.readline()
    if not response:
      raise ConnectionError("Socket closed on remote end")

    return response

  def read(self, length=None):
    if length == None:
      return self._readline()

    bytes_left = length + 2
    if length > MAX_READ_LENGTH:
      # apparently reading more than 1MB or so from a windows
      # socket can cause MemoryErrors.
      # read smaller chunks at a time to work around this
      try:
        buf = BytesIO()
        while bytes_left > 0:
          read_len = min(bytes_left, MAX_READ_LENGTH)
          buf.write(self._fp.read(read_len))
          bytes_left -= read_len
        buf.seek(0)
        return buf.read(length)
      finally:
        buf.close()
    return self._fp.read(bytes_left)[:-2]

class ParserException(BaseException):
  pass

class BeanstalkParser(object):
  def put(self, connection):
    pass

  def _reserve_with_body(self, connection, job_id, nb_bytes):
    assert job_id.isdigit()
    assert nb_bytes.isdigit()

    try:
      body = connection.read(int(nb_bytes))
      return Job(job_id, body, self)
    except:
      raise ParserException("An exception occured when reading job body")

  def reserve(self, connection):
    line = connection.read().rstrip()

    try:
      action, job_id, nb_bytes = line.split(" ")

      if action == "RESERVED":
        return self._reserve_with_body(connection, job_id, nb_bytes)

      if action == "TIMED_OUT":
        return None

      if action == "DEADLINE_SOON":
        return None
    except BaseException as exp:
      if isinstance(exp, ParserException):
        raise exp
      else:
        raise ParserException("Expected [RESERVED <id> <bytes>] or [TIMED_OUT] or [DEADLINE_SOON] but got %s" % line)
  def put(self, connection):
    line = connection.read().rstrip()

    if line.find(" ") != -1:
      action, job_id = line.split(" ")

      if action == "INSERTED":
        return job_id
      elif action == "BURIED":
        raise BeanstalkError("Server run out of memory")
    else:
      if line == "EXPECTED_CRLF":
        raise InternalError("The put jobs need to be followed by CRLF")
      elif line == "DRAINING":
        raise ResponseError("The server is shutting down")
      elif line == "JOB_TOO_BIG":
        raise ResponseError("The job body needs to be lower than 2**16")

  def watch(self, connection):
    line = connection.read().rstrip()
    print line

  def ignore(self, connection):
    line = connection.read().rstrip()
    print line

class ConnectionPool(object):
  def __init__(self, connection_class=Connection, max_connections=None, **connection_kwargs):
    self.connection_class = connection_class
    self.connection_kwargs = connection_kwargs
    self.max_connections = max_connections or 2 ** 31
    self._created_connections = 0
    self._available_connections = []
    self._in_use_connections = set()

  def get_connection(self):
    "Get a connection from the pool"
    try:
      connection = self._available_connections.pop()
    except IndexError:
      connection = self.make_connection()
    self._in_use_connections.add(connection)
    return connection

  def make_connection(self):
    "Create a new connection"
    if self._created_connections >= self.max_connections:
      raise ConnectionError("Too many connections")
    self._created_connections += 1

    new_connection = self.connection_class(**self.connection_kwargs)
    new_connection.just_created = True
    return new_connection

  def release(self, connection):
    """
    Releases the connection back to the pool
    """
    self._in_use_connections.remove(connection)
    self._available_connections.append(connection)

  def disconnect(self):
    """
    Disconnects all connections in the pool
    """
    all_conns = chain(self._available_connections, self._in_use_connections)
    for connection in all_conns:
      connection.disconnect()

class Job(object):
  def __init__(self, jid, body, beanstalk):
    self.jid = jid
    self.body = body
    self.beanstalk = beanstalk

  def delete(self):
    self.beanstalk.delete(self.jid)

  def burry(self):
    self.beanstalk.delete(self.jid)

  def __repr__(self):
    return "jid: %s, body=%s" % (self.jid, self.body)

class Beanstalk(object):
  def __init__(self, host="localhost", port=11300, connection_pool=None,
               parse_class=BeanstalkParser):

    if not pool:
      self.pool = ConnectionPool(host, port)
    else:
      self.pool = pool

    self.use_tube = None
    self.watch_tubes = []

    self.parser = BeanstalkParser()

  def _send_command(self, connection, command, parser_method):
    try:
      connection.send_command(command)
      # read response and parsing it accordingly
      return parser_method(connection)
    except ConnectionError:
      connection.disconnect()
      connection.send_command(command)
      # read response and parsing it accordingly
      return parser_method(connection)

  def _set_tubes(self, connection):
    if self.use_tube != None:
      self._send_command(connection, "use %s\r\n" % self.use_tube, self.parser.use)

    for tube in self.watch_tubes:
      self._send_command(connection, "watch %s\r\n" % tube, self.parser.watch)
    self._send_command(connection, "ignore default\r\n", self.parser.ignore)

    del connection.just_created

  def _make_request(self, command, parser_method):
    connection = self.pool.get_connection()

    try:
      if hasattr(connection, "just_created"):
        self._set_tubes(connection)
      return self._send_command(connection, command, parser_method)
    finally:
      self.pool.release(connection)

  def reserve(self, timeout=None):

    if timeout is not None:
      command = 'reserve-with-timeout %d\r\n' % timeout
    else:
      command = 'reserve\r\n'

    return self._make_request(command, self.parser.reserve)

  def use(self, name):
    self.use_tube = name
    return self._make_request("use %s\r\n" % name, self.parser.use)

  def put(self, body, priority=DEFAULT_PRIORITY, delay=0, ttr=DEFAULT_TTR):
    if not isinstance(body, str):
      raise BeanstalkError("Job body must be a str instance")

    return self._make_request("put %s %s %s %s\r\n%s\r\n" %
                              (priority, delay, ttr, len(body), body),
                              self.parser.put)

  def watch(self, name):
    self.watch_tubes.append(name)
    return self._make_request("watch %s\r\n" % name, self.parser.watch)

  def ignore(self, name):
    return self._make_request("ignore %s\r\n" % name, self.parser.ignore)

  def delete(self, job_id):
    return self._make_request("delete %s\r\n" % job_id, self.parser.delete)

  def bury(self, job_id, priority=DEFAULT_PRIORITY):
    return self._make_request("bury %s\r\n" % job_id, self.parser.bury)

if __name__ == "__main__":
  pool = ConnectionPool(host="127.0.0.1", port=11300)
  client = Beanstalk(connection_pool=pool)

  client.watch("facebook_crawl")
  job = client.reserve()
  #job.delete()
  #print client.put("mama are mere")
