# Creates an ISOXML file from a uid and puts it in watch directory
# To run in production environment:
# >sudo -E LD_LIBRARY_PATH=/opt/rh/python27/root/usr/lib64 /opt/rh/python27/root/usr/bin/python uid2iso.py <uid>

import sys
from lib.curatorFunctions import doISOXML


if __name__ == '__main__':
    uid = sys.argv[1]
    print(doISOXML(uid))
