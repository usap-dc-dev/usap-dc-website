from flask import json
import usap
from collections import OrderedDict
from services.lib.flask_restplus import Resource, reqparse, fields, inputs, Namespace
import services.models as models
import lib.curatorFunctions as cf
import psycopg2
import psycopg2.extras


config = json.loads(open('config.json', 'r').read())

ns = Namespace('collections', description='Operations related to collections', ordered=True)

collections_arguments = reqparse.RequestParser()
collections_arguments.add_argument('dataset_uid', help='USAP-DC dataset identification number', example='600030')
collections_arguments.add_argument('project_uid', help='USAP-DC project identification number', example='p0000114')
collections_arguments.add_argument('collection_uid', help='USAP-DC collection identification number', example='c0000001')
collections_arguments.add_argument('title', help='Any part of the title of the collection', example="est")
collections_arguments.add_argument('description', help='Any part of the description of the collection', example='est')

collection_model = ns.model('Collection', OrderedDict([
    ('project_ids', fields.List(fields.String())),
    ('dataset_ids', fields.List(fields.String())),
    ('collection_id', fields.String(attribute='collection_id')),
    ('title', fields.String(attribute='collection_name')),
    ('description', fields.String(attribute='description'))
]))

base_url = "{0}{1}/".format(config['API_BASE'], ns.path)
examples = """Base URL: {0}\n
        Examples:
            {0}
            {0}?dataset_uid=600030""".format(base_url)

def connect_to_db(curator=False):
    info = config['DATABASE']
    if curator and cf.isCurator():
        user = info['USER_CURATOR']
        password = info['PASSWORD_CURATOR']
    else:
        user = info['USER']
        password = info['PASSWORD']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=user,
                            password=password)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)

@ns.route('/')
class CollectionsCollection(Resource):
    @ns.expect(collections_arguments)
    @ns.marshal_with(collection_model)
    @ns.response(200, 'Success')
    @ns.response(400, 'Validation Error')
    def get(self):
        """Returns list of all collections, filtered by the provided parameters"""
        # return "hello world"
        query_parameters = collections_arguments.parse_args()
        (con, cur) = connect_to_db()
        queryTemplate = "SELECT * from collection_view"
        sqlParameters={}
        if query_parameters['project_uid']:
            sqlParameters["%s=ANY(project_ids)"] = query_parameters['project_uid']
        if query_parameters['dataset_uid']:
            sqlParameters["%s=ANY(dataset_ids)"] = query_parameters['dataset_uid']
        if query_parameters['collection_uid']:
            sqlParameters["collection_id=%s"] = query_parameters['collection_uid']
        if query_parameters['title']:
            sqlParameters["collection_name"] = query_parameters['title']
        if query_parameters['description']:
            sqlParameters['description'] = query_parameters['description']
        if len(sqlParameters)>0:
            queryTemplate += " WHERE " + " AND ".join(list(sqlParameters.keys()))
        query = cur.mogrify(queryTemplate, tuple(sqlParameters.values()))
        cur.execute(query)
        results = cur.fetchall()
        resultsList = list(map(lambda entry: dict(entry), list(results)))
        return resultsList