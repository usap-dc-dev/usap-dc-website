# setting up v2 for future use!
from services.lib.flask_restplus import Api
from flask import Blueprint
from endpoints_v2.datasets import ns as datasets_ns
from endpoints_v2.projects import ns as projects_ns
from endpoints_v2.persons import ns as persons_ns
from endpoints_v2.awards import ns as awards_ns
from endpoints_v2.datafiles import ns as datafiles_ns

blueprint = Blueprint('api2', __name__, url_prefix='/api/v2.0')
api = Api(blueprint, version='v2.0', title='USAP-DC API', ordered=True, doc='/doc',
          description='A Rest API service for accessing data from USAP-DC')

api.add_namespace(awards_ns)
api.add_namespace(datasets_ns)
api.add_namespace(persons_ns)
api.add_namespace(projects_ns)
api.add_namespace(datafiles_ns)

