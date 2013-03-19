import os
import socket
import sys

from itertools import chain

"""
Native Python Beanstalk client

Key features:
  * connection pool
  * simple and clear implementation

Each connection is tighly coupled to a tube.
You cannot change this.
"""

MAX_READ_LENGTH = 1000000

class BeanstalkError(BaseException):
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

  def reserve(self, connection):
    line = connection.read().rstrip()

    try:
      action, job_id, nb_bytes = line.split(" ")

      assert action == "RESERVED"
      assert job_id.isdigit()
      assert nb_bytes.isdigit()
    except:
      raise ParserException("Expected RESERVED <id> <bytes> got %s" % line)

    line = connection.read(int(nb_bytes))
    print line

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

    return self.connection_class(**self.connection_kwargs)

  def release(self, connection):
    "Releases the connection back to the pool"
    self._in_use_connections.remove(connection)
    self._available_connections.append(connection)

  def disconnect(self):
    "Disconnects all connections in the pool"
    all_conns = chain(self._available_connections, self._in_use_connections)
    for connection in all_conns:
      connection.disconnect()

class Job(object):
  def __init__(self, jid, beanstalk):
    self.beanstalk = beanstalk
    self.jid = jid

  def delete(self):
    pass

  def burry(self):
    pass

class Beanstalk(object):
  def __init__(self, host="localhost", port=11300, connection_pool=None,
               tube="", parse_class=BeanstalkParser):

    if not pool:
      self.pool = ConnectionPool(host, port)
    else:
      self.pool = pool

    self.parser = BeanstalkParser()

  def _make_request(self, command, parser_method):
    pool = self.pool
    connection = pool.get_connection()

    try:
      connection.send_command(command)
      # read response and parsing it accordingly
      return parser_method(connection)
    except ConnectionError:
      connection.disconnect()
      connection.send_command(command)
      # read response and parsing it accordingly
      return parser_method(connection)
    finally:
      pool.release(connection)

  def reserve(self, timeout=None):

    if timeout is not None:
      command = 'reserve-with-timeout %d\r\n' % timeout
    else:
      command = 'reserve\r\n'

    return self._make_request(command, self.parser.reserve)
  
  def use(self, name):
    return self._make_request("use %s\r\n" % name, self.parser.use)
  
  def put(self, item):
    return self._make_request("use %s\r\n" % name, self.parser.put)

  def watch(self, name):
    return self._make_request("watch %s\r\n" % name, self.parser.watch)

  def ignore(self, name):
    return self._make_request("ignore %s\r\n" % name, self.parser.ignore)

if __name__ == "__main__":
  pool = ConnectionPool(host="127.0.0.1", port=11300)
  client = Beanstalk(connection_pool=pool, tube="facebook_crawl")

  client.watch("facebook_crawl")
  client.reserve()
