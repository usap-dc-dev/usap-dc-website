import flask
from flask import request, json, jsonify, Response
import usap
from datetime import datetime
from collections import OrderedDict
from api.lib.flask_restplus import Resource, reqparse, fields, marshal_with, inputs, Namespace
import api.models as models


config = json.loads(open('config.json', 'r').read())

ns = Namespace('persons', description='Operations related to people', ordered=True)

#input arguments
persons_arguments = reqparse.RequestParser()
persons_arguments.add_argument('person_uid', help='USAP-DC person id (usually in the form: LastName, FirstName)', example="Bell, Robin")
persons_arguments.add_argument('first_name', help="person's first name", example='Robin')
persons_arguments.add_argument('last_name', help="person's last name", example='Bell')
persons_arguments.add_argument('organization', help="person's institute or organization (can be partial)", example='Columbia')


#model for the datasets associated with each person
datasets_fields = models.getDatasetsShortModel(ns)
#model for the projects associated with each person
projects_fields = models.getProjectsShortModel(ns)

#model for a person
person_model = ns.model('Person', OrderedDict([
    ('person_uid', fields.String(attribute='id')),
    ('first_name', fields.String),
    ('last_name', fields.String),
    ('organization', fields.String),
    ('projects', fields.List(fields.Nested(projects_fields))),
    ('datasets', fields.List(fields.Nested(datasets_fields))),
]))


#database query used to find results
def getQuery():
    return """SELECT  *
                   FROM person p
                   LEFT JOIN (
                        SELECT ppm.person_id, 
                        json_agg(json_build_object('title', proj.title, 'proj_uid', proj.proj_uid)) AS projects
                        FROM project_person_map ppm  JOIN project proj ON ppm.proj_uid = proj.proj_uid
                        GROUP BY ppm.person_id
                    ) pp ON pp.person_id = p.id
                    LEFT JOIN (
                        SELECT dpm.person_id, 
                        json_agg(json_build_object('title', d.title, 'dataset_uid', d.id, 'doi', d.doi)) AS datasets
                        FROM dataset_person_map dpm  JOIN dataset d ON dpm.dataset_id = d.id
                        GROUP BY dpm.person_id
                    ) pd ON pd.person_id = p.id
                       WHERE TRUE"""

base_url = "{0}{1}/".format(config['API_BASE'], ns.path)
examples = """Base URL: {0}\n
    Examples:
            {0}?first_name=Frank
            {0}?last_name=Bell&organization=Columbia""".format(base_url)


@ns.route('/', doc={'description': examples})
class PersonsCollection(Resource):
    @ns.expect(persons_arguments)
    @ns.marshal_with(person_model)
    @ns.response(200, 'Success')
    @ns.response(400, 'Validation Error')
    @ns.response(404, 'No persons found.')
    def get(self):

        """Returns list of all persons, filtered by the provided parameters"""
        query_parameters = persons_arguments.parse_args()

        person_uid = query_parameters.get('person_uid')
        first_name = query_parameters.get('first_name')
        last_name = query_parameters.get('last_name')
        organization = query_parameters.get('organization')

        query = getQuery()

        to_filter = []

        if person_uid:
            query += " AND p.id = %s "
            to_filter.append(person_uid)
        if first_name:
            query += " AND p.first_name ~* %s "
            to_filter.append(first_name)   
        if last_name:
            query += " AND p.last_name ~* %s "
            to_filter.append(last_name)  
        if organization:
            query += " AND p.organization ~* %s "
            to_filter.append(organization)  

        query += ';'

        (conn, cur) = usap.connect_to_db()

        cur.execute(query, to_filter)
        results = cur.fetchall()

        for res in results:
            try:
                if res.get('datasets'):
                    for ds in res.get('datasets'):
                        ds['landing_page'] = "%sview/dataset/%s" % (config['USAP_DOMAIN'], ds['dataset_uid'])
                if res.get('projects'):
                    for p in res.get('projects'):
                        p['landing_page'] = "%sview/project/%s" % (config['USAP_DOMAIN'], p['proj_uid'])                    
            except Exception as e:
                print("ERROR")
                print(str(e))
                return [], 404

        return results

example = """Base URL: {0}\n
        Example:
            {0}Bell, Robin""".format(base_url)

@ns.route('/<person_uid>', doc={'description': example})
class PersonItem(Resource):
    @ns.marshal_with(person_model)
    @ns.response(404, 'Person not found.')
    def get(self, person_uid):
        """Returns details of a person."""
        query = getQuery()
        query += " AND p.id = %s;"

        (conn, cur) = usap.connect_to_db()
        cur.execute(query,[person_uid])
        res = cur.fetchone()
        if not res: return [],404

        try:
            if res.get('datasets'):
                for ds in res.get('datasets'):
                    ds['landing_page'] = "%sview/dataset/%s" % (config['USAP_DOMAIN'], ds['dataset_uid'])
            if res.get('projects'):
                for p in res.get('projects'):
                    p['landing_page'] = "%sview/project/%s" % (config['USAP_DOMAIN'], p['proj_uid'])                    
        except Exception as e:
            print("ERROR")
            print(str(e))
            return [], 404

        return res

