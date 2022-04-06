from flask import json
import usap
from collections import OrderedDict
from services.lib.flask_restplus import Resource, reqparse, fields, inputs, Namespace
import services.models as models


config = json.loads(open('config.json', 'r').read())

ns = Namespace('datasets', description='Operations related to datasets', ordered=True)

#input arguments
datasets_arguments = reqparse.RequestParser()
datasets_arguments.add_argument('dataset_uid', help='USAP-DC dataset identification number', example='600030')
datasets_arguments.add_argument('award', help='award number', example='0724929')
datasets_arguments.add_argument('person', help='name of anybody involved in the dataset (can be partial)', example='Nitsche')
datasets_arguments.add_argument('release_date', type=inputs.date,  help='returns datasets released on or after this date (in YYYY-MM-DD format)', example='2019-01-01')
datasets_arguments.add_argument('keywords', action='split', help='keyword assigned to dataset - can filter on multiple keywords using a comma separated list', example='penguin')
datasets_arguments.add_argument('locations', action='split', help='location of dataset - can filter on multiple locations using a comma separated list', example='Amundsen Sea')
datasets_arguments.add_argument('nsf_funding_program', help='name of NSF funding program', example='Antarctic Earth Sciences')
datasets_arguments.add_argument('science_program', help='name of science program', example='Allan Hills')
# datasets_arguments.add_argument('repository', help='repository dataset has been submitted to')
datasets_arguments.add_argument('title', help='(any part of) dataset title', example='Vostok Ice Core Chemistry')
datasets_arguments.add_argument('doi', help='dataset DOI', example='10.15784/601046')
datasets_arguments.add_argument('north', type=float, help='northern boundary of dataset', example='-80')
datasets_arguments.add_argument('south', type=float, help='southern boundary of dataset', example='-89')
datasets_arguments.add_argument('east', type=float, help='eastern boundary of dataset', example='100')
datasets_arguments.add_argument('west', type=float, help='western boundary of dataset', example='10')


#model for the projects associated with each dataset
projects_fields = models.getProjectsShortModel(ns)

#model for the dataset
dataset_model = ns.model('Dataset', OrderedDict([
    ('dataset_uid', fields.String(attribute='id')),
    ('doi', fields.String),
    ('landing_page', fields.String),
    ('title', fields.String),
    ('creator', fields.String),
    ('abstract', fields.String),
    ('release_date', fields.DateTime(dt_format='rfc822')),
    ('keywords', fields.String),
    ('locations', fields.String),
    ('persons', fields.String(attribute='persons', description="(All PIs and Co-PIs)")),
    ('projects', fields.List(fields.Nested(projects_fields))),
    ('awards', fields.String),
    ('north', fields.String),
    ('south', fields.String),
    ('east', fields.String),
    ('west', fields.String),
    ('science_programs', fields.String),
    ('nsf_funding_programs', fields.String),
    ('replaces', fields.String),
    ('replaced_by', fields.String),
    ('language_id', fields.String),
    ('version', fields.String),
    ('date_created', fields.DateTime(dt_format='rfc822')),
    ('date_modified', fields.DateTime(dt_format='rfc822')),
]))


#database query used to find results
def getQuery():
    return """SELECT *
                   FROM dataset d 
                   JOIN dataset_view dv ON dv.uid = d.id
                   WHERE TRUE"""

base_url = "{0}{1}/".format(config['API_BASE'], ns.path)
examples = """Base URL: {0}\n
        Examples:
            {0}?person=Frank
            {0}?keywords=penguin,cryosphere
            {0}?north=-80&south=-88&east=100&west=10""".format(base_url)


@ns.route('/', doc={'description': examples})
class DatasetsCollection(Resource):
    @ns.expect(datasets_arguments)
    @ns.marshal_with(dataset_model)
    @ns.response(200, 'Success')
    @ns.response(400, 'Validation Error')
    @ns.response(404, 'No datasets found.')
    def get(self):

        """Returns list of all datasets, filtered by the provided parameters"""
        query_parameters = datasets_arguments.parse_args()

        dataset_uid = query_parameters.get('dataset_uid')
        release_date = query_parameters.get('release_date')
        award = query_parameters.get('award')
        person = query_parameters.get('person')
        keywords = query_parameters.get('keywords') or query_parameters.get('keyword')
        locations = query_parameters.get('locations') or query_parameters.get('location')
        nsf_funding_program = query_parameters.get('nsf_funding_program')
        science_program = query_parameters.get('science_program')
        repository = query_parameters.get('repository')
        title = query_parameters.get('title')
        doi = query_parameters.get('doi')
        north = query_parameters.get('north')
        south = query_parameters.get('south')
        east = query_parameters.get('east')
        west = query_parameters.get('west')

        query = getQuery()
        to_filter = []

        if dataset_uid:
            query += " AND d.id = %s "
            to_filter.append(dataset_uid)
        if release_date:
            query += " AND TO_DATE(d.release_date, 'YYYY-MM-DD') >= %s"
            to_filter.append(release_date)
        if award:
            query += " AND awards ~ %s"
            to_filter.append(award)
        if person:
            query += " AND persons ~* %s"
            to_filter.append(person)
        if keywords:
            for kw in keywords:
                query += " AND keywords ~* %s"
                to_filter.append(kw)
        if locations:
            for loc in locations:
                query += " AND locations ~* %s"
                to_filter.append(loc)
        if nsf_funding_program:
            query += " AND nsf_funding_programs ~* %s"
            to_filter.append(nsf_funding_program)
        if science_program:
            query += " AND science_programs ~* %s"
            to_filter.append(science_program)
        if repository:
            query += " AND repositories ~* %s"
            to_filter.append(repository)   
        if title:
            query += " AND d.title ~* %s"
            to_filter.append(title)   
        if doi:
            query += " AND doi = %s"
            to_filter.append(doi)  
        if north:
            query += " AND north <= %s"
            to_filter.append(north)          
        if south:
            query += " AND south >= %s"
            to_filter.append(south)    
        if east:
            query += " AND east <= %s"
            to_filter.append(east)          
        if west:
            query += " AND west >= %s"
            to_filter.append(west)   

        query += ';'

        (conn, cur) = usap.connect_to_db()
        cur.execute(query, to_filter)
        results = cur.fetchall()

        if len(results) == 0:
            return [], 404

        for res in results:
            res['landing_page'] = "%sview/dataset/%s" % (config['USAP_DOMAIN'], res['id'])

        for res in results:
            try:
                if res.get('projects'):
                    res['projects'] = json.loads(res['projects'])
                    for p in res.get('projects'):
                        p['landing_page'] = "%sview/project/%s" % (config['USAP_DOMAIN'], p['proj_uid'])   
                if res.get('release_date'):
                    try:
                        res['release_date'] = datetime.strptime(res['release_date'], '%Y-%m-%d')
                    except:
                        res['release_date'] = None
            except Exception as e:
                print("ERROR")
                print(str(e))
                return [], 404

        return results

example = """Base URL: {0}\n
        Example:
            {0}600030""".format(base_url)

@ns.route('/<dataset_uid>', doc={'description': example})
class DatasetItem(Resource):
    @ns.marshal_with(dataset_model)
    @ns.response(404, 'Dataset not found.')
    def get(self, dataset_uid):
        """Returns details of a dataset."""
        query = getQuery()
        query += " AND d.id = %s;"

        (conn, cur) = usap.connect_to_db()
        cur.execute(query,[dataset_uid])
        res = cur.fetchone()
        if not res: return [],404
        res['landing_page'] = "%sview/dataset/%s" % (config['USAP_DOMAIN'], res['id'])

        try:
            if res.get('projects'):
                res['projects'] = json.loads(res['projects'])
                for p in res.get('projects'):
                    p['landing_page'] = "%sview/project/%s" % (config['USAP_DOMAIN'], p['proj_uid']) 
            if res.get('release_date'):
                try:
                    res['release_date'] = datetime.strptime(res['release_date'], '%Y-%m-%d')
                except:
                    res['release_date'] = None
        except Exception as e:
            print("ERROR")
            print(str(e))
            return [], 404

        return res
