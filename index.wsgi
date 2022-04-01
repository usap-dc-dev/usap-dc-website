
import os
import sys
sys.path.append('/usr/local/lib64/python3.9/site-packages')
sys.path.append('/web/usap-dc/htdocs')

# cd to root web directory so flask works correctly
os.chdir('/web/usap-dc/htdocs')

from werkzeug.debug import DebuggedApplication
from flask import Flask
from usap import app
application = DebuggedApplication(app, evalex=True)
