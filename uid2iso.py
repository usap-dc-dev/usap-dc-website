import sys
from lib.curatorFunctions import doISOXML


if __name__ == '__main__':
    uid = sys.argv[1]
    print(doISOXML(uid))
