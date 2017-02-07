import os
import sys
sys.path.append('/opt/rh/python27/root/usr/lib64/python2.7/site-packages')
sys.path.append('/web/usap-dc/htdocs')

# cd to root web directory so flask works correctly
os.chdir('/web/usap-dc/htdocs')

from werkzeug.debug import DebuggedApplication
from flask import Flask
from usap import app
application = DebuggedApplication(app, evalex=True)

