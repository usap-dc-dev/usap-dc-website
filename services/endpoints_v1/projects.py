import flask
from flask import request, json, jsonify, Response
import usap
from datetime import datetime
from collections import OrderedDict
from services.lib.flask_restplus import Resource, reqparse, fields, marshal_with, inputs, Namespace
import services.models as models


config = json.loads(open('config.json', 'r').read())

ns = Namespace('projects', description='Operations related to projects', ordered=True)

#input arguments
projects_arguments = reqparse.RequestParser()
projects_arguments.add_argument('proj_uid', help='USAP-DC project identification number', example='p0000114')
projects_arguments.add_argument('cruise_id', help='cruise id for any ship expeditions associated with the project', example='NBP0103')
projects_arguments.add_argument('start_date', type=inputs.date,  help='returns projects with start dates on or after this date (in YYYY-MM-DD format)' , example='2010-05-01')
projects_arguments.add_argument('end_date', type=inputs.date,  help='returns awards with expiry dates on or before this date (in YYYY-MM-DD format)', example='2015-09-10')
projects_arguments.add_argument('award', help='award number', example='0724929')
projects_arguments.add_argument('person', help='name of anybody involved in the dataset (can be partial)', example='Nitsche')
projects_arguments.add_argument('keywords', action='split', help='keyword assigned to project - can filter on multiple keywords using a comma separated list', example='penguin')
projects_arguments.add_argument('locations', action='split', help='location of project - can filter on multiple locations using a comma separated list', example='Amundsen Sea')
projects_arguments.add_argument('nsf_funding_program', help='name of NSF funding program', example='Antarctic Earth Sciences')
projects_arguments.add_argument('science_program', help='name of science program', example='Allan Hills')
# projects_arguments.add_argument('repository', help='repository datasets associated with the project have been submitted to')
projects_arguments.add_argument('title', help='(any part of) project title', example='Marine Record of Cryosphere')
projects_arguments.add_argument('dif_id', help='DIF ID for the project', example='USAP-1644245_1')
projects_arguments.add_argument('north', type=float, help='northern boundary of dataset', example='-80')
projects_arguments.add_argument('south', type=float, help='southern boundary of dataset', example='-89')
projects_arguments.add_argument('east', type=float, help='eastern boundary of dataset', example='100')
projects_arguments.add_argument('west', type=float, help='western boundary of dataset', example='10')


#model for the datasets associated with each project
datasets_fields = models.getDatasetsShortModel(ns)

#model for the deployments associated with each project
deployments_fields = models.getDeploymentsShortModel(ns)

#model for the project
project_model = ns.model('Project', OrderedDict([
    ('proj_uid', fields.String(attribute='uid')),
    ('landing_page', fields.String),
    ('title', fields.String),
    ('description', fields.String),
    ('start_date', fields.DateTime(dt_format='rfc822')),
    ('end_date', fields.DateTime(dt_format='rfc822')),
    ('keywords', fields.String),
    ('locations', fields.String),
    ('persons', fields.String(attribute='persons', description="(All PIs and Co-PIs)")),
    ('datasets', fields.List(fields.Nested(datasets_fields))),
    # ('datasets', fields.String),
    ('awards', fields.String),
    ('dif_ids', fields.String),
    ('deployments', fields.List(fields.Nested(deployments_fields))),
    ('north', fields.String),
    ('south', fields.String),
    ('east', fields.String),
    ('west', fields.String),
    ('science_programs', fields.String),
    ('nsf_funding_programs', fields.String),
    ('date_created', fields.DateTime(dt_format='rfc822')),
    ('date_modified', fields.DateTime(dt_format='rfc822')),
]))


#database query used to find results
def getQuery():
    return """SELECT DISTINCT *
           FROM project p 
           JOIN project_view pv ON pv.uid = p.proj_uid
           LEFT JOIN (
                SELECT pdm.proj_uid, STRING_AGG(pdm.dif_id, '; ') dif_ids
                FROM project_dif_map pdm 
                GROUP BY pdm.proj_uid
            ) dif ON dif.proj_uid = p.proj_uid
           LEFT JOIN (
                SELECT pd.proj_uid, 
                json_agg(json_build_object('deployment_id', pd.deployment_id, 'deployment_type', pd.deployment_type))::text AS deployments
                FROM project_deployment pd
                GROUP BY pd.proj_uid
            ) depl ON depl.proj_uid = p.proj_uid
           WHERE TRUE"""

base_url = "{0}{1}/".format(config['API_BASE'], ns.path)
examples = """Base URL: {0}\n
    Examples:
        {0}?person=Frank
        {0}?locations=Ross Sea,South Shetland Islands
        {0}?north=-80&south=-88&east=100&west=10""".format(base_url)


@ns.route('/', doc={'description': examples})
class ProjectsCollection(Resource):
    @ns.expect(projects_arguments)
    @ns.marshal_with(project_model)
    @ns.response(200, 'Success')
    @ns.response(400, 'Validation Error')
    @ns.response(404, 'No project found.')
    def get(self):

        """Returns list of all projects, filtered by the provided parameters"""
        query_parameters = projects_arguments.parse_args()

        proj_uid = query_parameters.get('proj_uid')
        cruise_id = query_parameters.get('cruise_id')
        start_date = query_parameters.get('start_date')
        end_date = query_parameters.get('end_date')
        award = query_parameters.get('award')
        person = query_parameters.get('person')
        keywords = query_parameters.get('keywords')
        locations = query_parameters.get('locations')
        nsf_funding_program = query_parameters.get('nsf_funding_program')
        science_program = query_parameters.get('science_program')
        repository = query_parameters.get('repository')
        title = query_parameters.get('title')
        north = query_parameters.get('north')
        south = query_parameters.get('south')
        east = query_parameters.get('east')
        west = query_parameters.get('west')
        dif_id = query_parameters.get('dif_id')

        query = getQuery()
        to_filter = []

        if proj_uid:
            query += " AND p.proj_uid = %s "
            to_filter.append(proj_uid)
        if cruise_id:
            query += " AND deployments ~* %s"
            to_filter.append(cruise_id)
        if start_date:
            query += " AND p.start_date >= %s"
            to_filter.append(start_date)
        if end_date:
            query += " AND p.end_date <= %s"
            to_filter.append(end_date)
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
            query += " AND p.title ~* %s"
            to_filter.append(title)   
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
        if dif_id:
            query += " AND dif_ids ~* %s"
            to_filter.append(dif_id)  

        query += ';'

        (conn, cur) = usap.connect_to_db()
        cur.execute(query, to_filter)
        results = cur.fetchall()

        if len(results) == 0:
            return [], 404

        for res in results:
            res['landing_page'] = "%sview/project/%s" % (config['USAP_DOMAIN'], res['uid'])

        for res in results:
            try:
                if res.get('datasets'):
                    res['datasets'] = json.loads(res['datasets'])
                    for ds in res.get('datasets'):
                        ds['landing_page'] = ds['url']
                    print(ds['landing_page'])
                if res.get('deployments'):
                    res['deployments'] = json.loads(res['deployments'])
            except Exception as e:
                print("ERROR")
                print(str(e))
                return [], 404

        return results

example = """Base URL: {0}\n
        Example:
            {0}p0000114""".format(base_url)


@ns.route('/<proj_uid>', doc={'description': example})
class ProjectItem(Resource):
    @ns.marshal_with(project_model)
    @ns.response(404, 'Project not found.')
    def get(self, proj_uid):
        """Returns details of a project."""
        query = getQuery()
        query += " AND p.proj_uid = %s;"

        (conn, cur) = usap.connect_to_db()
        cur.execute(query,[proj_uid])
        res = cur.fetchone()
        if not res: return [],404
        res['landing_page'] = "%sview/project/%s" % (config['USAP_DOMAIN'], res['uid'])

        try:
            if res.get('datasets'):
                res['datasets'] = json.loads(res['datasets'])
                for ds in res.get('datasets'):
                    ds['landing_page'] = ds['url']
            if res.get('deployments'):
                res['deployments'] = json.loads(res['deployments'])
        except Exception as e:
            print("ERROR")
            print(str(e))
            return [], 404

        return res

