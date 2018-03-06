#! /usr/bin/env python

# EZID command line client.  The output is Unicode using UTF-8
# encoding unless overriden by the -e option.  By default, ANVL
# responses (currently, that's all responses) are left in %-encoded
# form.
#
# Usage: ezid.py [options] credentials operation...
#
#   options:
#     -d          decode ANVL responses
#     -e ENCODING output character encoding
#     -o          one line per ANVL value: convert newlines to spaces
#     -t          format timestamps
#
#   credentials:
#     username:password
#     username (password will be prompted for)
#     sessionid=... (as returned by previous login)
#     - (none)
#
#   operation:
#     m[int] shoulder [element value ...]
#     c[reate] identifier [element value ...]
#     v[iew] identifier
#     u[pdate] identifier [element value ...]
#     d[elete] identifier
#     login
#     logout
#     s[tatus]
#
# In the above, if an element is "@", the subsequent value is treated
# as a filename and metadata elements are read from the named
# ANVL-formatted file.  For example, if file metadata.txt contains:
#
#   erc.who: Proust, Marcel
#   erc.what: Remembrance of Things Past
#   erc.when: 1922
#
# then an identifier with that metadata can be minted by invoking:
#
#   ezid.py username:password mint ark:/99999/fk4 @ metadata.txt
#
# Otherwise, if a value has the form "@filename", a (single) value is
# read from the named file.  For example, if file metadata.xml
# contains a DataCite XML record, then an identifier with that record
# as the value of the 'datacite' element can be minted by invoking:
#
#   ezid.py username:password mint doi:10.5072/FK2 datacite @metadata.xml
#
# In both of the above cases, the contents of the named file are
# assumed to be UTF-8 encoded.  And in both cases, the interpretation
# of @ can be defeated by doubling it.
#
# Greg Janee <gjanee@ucop.edu>
# May 2013

import codecs
import getpass
import optparse
import re
import sys
import time
import types
import urllib
import urllib2

KNOWN_SERVERS = {
  "p": "http://ezid.cdlib.org"
}

OPERATIONS = {
  # operation: number of arguments (possibly variable)
  "mint": lambda l: l%2 == 1,
  "create": lambda l: l%2 == 1,
  "view": 1,
  "update": lambda l: l%2 == 1,
  "delete": 1,
  "login": 0,
  "logout": 0,
  "status": 0
}

USAGE_TEXT = """Usage: ezid.py [options] credentials operation...

  options:
    -d          decode ANVL responses
    -e ENCODING output character encoding
    -o          one line per ANVL value: convert newlines to spaces
    -t          format timestamps

  credentials:
    username:password
    username (password will be prompted for)
    sessionid=... (as returned by previous login)
    - (none)

  operation:
    m[int] shoulder [element value ...]
    c[reate] identifier [element value ...]
    v[iew] identifier
    u[pdate] identifier [element value ...]
    d[elete] identifier
    login
    logout
    s[tatus]
"""

# Global variables that are initialized farther down.

_options = None
_server = None
_opener = None
_cookie = None

class MyHelpFormatter (optparse.IndentedHelpFormatter):
  def format_usage (self, usage):
    return USAGE_TEXT

class MyHTTPErrorProcessor (urllib2.HTTPErrorProcessor):
  def http_response (self, request, response):
    # Bizarre that Python leaves this out.
    if response.code == 201:
      return response
    else:
      return urllib2.HTTPErrorProcessor.http_response(self, request, response)
  https_response = http_response

def formatAnvlRequest (args):
  request = []
  for i in range(0, len(args), 2):
    k = args[i]
    if k == "@":
      f = codecs.open(args[i+1], encoding="UTF-8")
      request += [l.strip("\r\n") for l in f.readlines()]
      f.close()
    else:
      if k == "@@":
        k = "@"
      else:
        k = re.sub("[%:\r\n]", lambda c: "%%%02X" % ord(c.group(0)), k)
      v = args[i+1]
      if v.startswith("@@"):
        v = v[1:]
      elif v.startswith("@") and len(v) > 1:
        f = codecs.open(v[1:], encoding="UTF-8")
        v = f.read()
        f.close()
      v = re.sub("[%\r\n]", lambda c: "%%%02X" % ord(c.group(0)), v)
      request.append("%s: %s" % (k, v))
  return "\n".join(request)

def encode (id):
  return urllib.quote(id, ":/").encode("UTF-8")

def issueRequest (server, opener, path, method, data=None, returnHeaders=False,
  streamOutput=False):
  request = urllib2.Request("%s/%s" % (server.encode("UTF-8"), path))
  print(server)
  print(path)
  request.get_method = lambda: method
  if data:
    request.add_header("Content-Type", "text/plain; charset=UTF-8")
    request.add_data(data.encode("UTF-8"))
  if _cookie: request.add_header("Cookie", _cookie)
  try:
    connection = opener.open(request)
    if streamOutput:
      while True:
        sys.stdout.write(connection.read(1))
        sys.stdout.flush()
    else:
      response = connection.read()
      if returnHeaders:
        return response.decode("UTF-8"), connection.info()
      else:
        return response.decode("UTF-8")
  except urllib2.HTTPError, e:
    sys.stderr.write("%d %s\n" % (e.code, e.msg))
    if e.fp != None:
      response = e.fp.read()
      if not response.endswith("\n"): response += "\n"
      sys.stderr.write(response)
      return response
    else: 
      raise
    sys.exit(1)

def printAnvlResponse (response, sortLines=False):
  response = response.splitlines()
  if sortLines and len(response) >= 1:
    statusLine = response[0]
    response = response[1:]
    response.sort()
    response.insert(0, statusLine)
  for line in response:
    # if _options.formatTimestamps and (line.startswith("_created:") or\
    #   line.startswith("_updated:")):
    #   ls = line.split(":")
    #   line = ls[0] + ": " + time.strftime("%Y-%m-%dT%H:%M:%S",
    #     time.localtime(int(ls[1])))
    # if _options.decode:
    #   line = re.sub("%([0-9a-fA-F][0-9a-fA-F])",
    #     lambda m: chr(int(m.group(1), 16)), line)
    # if _options.oneLine: line = line.replace("\n", " ").replace("\r", " ")
    print line.encode("UTF-8")


