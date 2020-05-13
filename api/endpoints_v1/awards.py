import flask
from flask import request, json, jsonify, Response
import usap
from datetime import datetime
from collections import OrderedDict
from api.lib.flask_restplus import Resource, reqparse, fields, marshal_with, inputs, Namespace
import api.models as models


config = json.loads(open('config.json', 'r').read())

ns = Namespace('awards', description='Operations related to awards', ordered=True)

#input arguments
awards_arguments = reqparse.RequestParser()
awards_arguments.add_argument('award_uid', help='award number', example='0724929')
awards_arguments.add_argument('title', help="(any part of) award title", example="Antarctic Ice Cores")
awards_arguments.add_argument('pi', help="PI's name (can be partial)", example="Carbotte, Suzanne")
awards_arguments.add_argument('copi', help="co-PI's name (can be partial)", example="Tinto")
awards_arguments.add_argument('dir_code', help="directorate code", example="GEO")
awards_arguments.add_argument('div_code', help="division code", example="PLR")
awards_arguments.add_argument('start_date',type=inputs.date,  help='returns awards with start dates on or after this date (in YYYY-MM-DD format)', example="2018-05-01"),
awards_arguments.add_argument('expiry_date',type=inputs.date,  help='returns awards with expiry dates on or before this date (in YYYY-MM-DD format)', example="2019-09-10"),
awards_arguments.add_argument('nsf_funding_program', help='name of NSF funding program', example="Antarctic Earth Sciences")

# #model for the datasets associated with each award
datasets_fields = models.getDatasetsShortModel(ns)
#model for the projects associated with each award
projects_fields = models.getProjectsShortModel(ns)

#model for a award
award_model = ns.model('Award', OrderedDict([
    ('award_uid', fields.String(attribute='award')),
    ('title', fields.String),
    ('description', fields.String(attribute='sum')),
    ('pi', fields.String(attribute='name')),
    ('copi', fields.String),
    ('start_date', fields.DateTime(dt_format='rfc822', attribute='start')),
    ('expiry_date', fields.DateTime(dt_format='rfc822', attribute='expiry')),
    ('dir_code', fields.String(attribute='dir')),
    ('div_code', fields.String(attribute='div')),
    ('iscr', fields.Boolean),
    ('isipy', fields.Boolean),
    ('nsf_funding_programs', fields.String(attribute='programs')),
    ('projects', fields.List(fields.Nested(projects_fields))),
    ('datasets', fields.List(fields.Nested(datasets_fields))),
]))



#database query used to find results
def getQuery():
    return """SELECT  *
               FROM award a
               LEFT JOIN (
                    SELECT apm.award_id,
                    string_agg(apm.program_id, '; '::text) AS programs
                    FROM
                    award_program_map apm
                    GROUP BY apm.award_id
               ) aprog ON aprog.award_id = a.award
               LEFT JOIN (
                    SELECT pam.award_id, 
                    json_agg(json_build_object('title', proj.title, 'proj_uid', proj.proj_uid)) AS projects
                    FROM project_award_map pam  JOIN project proj ON pam.proj_uid = proj.proj_uid
                    GROUP BY pam.award_id
                ) ap ON ap.award_id = a.award
                LEFT JOIN (
                    SELECT dam.award_id, 
                    json_agg(json_build_object('title', d.title, 'dataset_uid', d.id, 'doi', d.doi)) AS datasets
                    FROM dataset_award_map dam  JOIN dataset d ON dam.dataset_id = d.id
                    GROUP BY dam.award_id
                ) ad ON ad.award_id = a.award
                       WHERE TRUE"""

base_url = "{0}{1}/".format(config['API_BASE'], ns.path)
examples = """Base URL: {0}\n
    Examples:
        {0}?pi=Carbotte, Suzanne
        {0}?dir_code=GEO&div_code=PLR
        {0}?start_date=2018-05-01&end_date=2018-09-10""".format(base_url)


@ns.route('/', doc={'description': examples})
class AwardsCollection(Resource):
    @ns.expect(awards_arguments)
    @ns.marshal_with(award_model)
    @ns.response(200, 'Success')
    @ns.response(400, 'Validation Error')
    @ns.response(404, 'No awards found.')
    @ns.doc({})
    def get(self):
        """Returns list of all awards, filtered by the provided parameters"""
        query_parameters = awards_arguments.parse_args()

        award_uid = query_parameters.get('award_uid')
        name = query_parameters.get('pi')
        copi = query_parameters.get('copi')
        dir_code = query_parameters.get('dir_code')
        div = query_parameters.get('div_code')
        start = query_parameters.get('start_date')
        expiry = query_parameters.get('expiry_date')
        title = query_parameters.get('title')
        nsf_funding_program = query_parameters.get('nsf_funding_program')

        query = getQuery()

        to_filter = []

        if award_uid:
            query += " AND a.award = %s "
            to_filter.append(award_uid)
        if name:
            query += " AND a.name ~* %s "
            to_filter.append(name)   
        if copi:
            query += " AND a.copi ~* %s "
            to_filter.append(copi)  
        if dir_code:
            query += " AND a.dir ~* %s "
            to_filter.append(dir_code)  
        if div:
            query += " AND a.div ~* %s "
            to_filter.append(div)  
        if start:
            query += " AND a.start >= %s"
            to_filter.append(start)
        if expiry:
            query += " AND a.expiry <= %s"
            to_filter.append(expiry) 
        if title:
            query += " AND a.title ~* %s"
            to_filter.append(title)          
        if nsf_funding_program:
            query += " AND programs ~* %s"
            to_filter.append(nsf_funding_program)

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
            {0}0724929""".format(base_url)


@ns.route('/<award_uid>', doc={'description': example})
class AwardItem(Resource):
    @ns.marshal_with(award_model)
    @ns.response(404, 'Award not found.')
    def get(self, award_uid):
        """Returns details of an award."""
        query = getQuery()
        query += " AND a.award = %s;"

        (conn, cur) = usap.connect_to_db()
        cur.execute(query,[award_uid])
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

