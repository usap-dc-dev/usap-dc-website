import flask
from flask import request, json, jsonify, Response
import usap
from datetime import datetime
from collections import OrderedDict
from flask_restplus import Resource, reqparse, fields, marshal_with, inputs, Namespace
import api.models as models


config = json.loads(open('config.json', 'r').read())

ns = Namespace('awards', description='Operations related to awards', ordered=True)

#input arguments
awards_arguments = reqparse.RequestParser()
awards_arguments.add_argument('award_uid', help='Award ID')
awards_arguments.add_argument('title', help="award title")
awards_arguments.add_argument('pi_id', help="PI's name_uid (usually in the form: LastName, FirstName)")
awards_arguments.add_argument('copi', help="co-PI's name")
awards_arguments.add_argument('dir_code', help="directorate code")
awards_arguments.add_argument('div_code', help="division code")
awards_arguments.add_argument('start_date',type=inputs.date,  help='start date of award in YYYY-MM-DD format'),
awards_arguments.add_argument('expiry_date',type=inputs.date,  help='expiry date of award in YYYY-MM-DD format'),
awards_arguments.add_argument('nsf_funding_program', help='name of NSF funding program')

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
    ('nsf_funding_programs', fields.String(attribute='program_id')),
    ('projects', fields.List(fields.Nested(projects_fields))),
    ('datasets', fields.List(fields.Nested(datasets_fields))),
]))


#database query used to find results
def getQuery():
    return """SELECT  *
               FROM award a
               LEFT JOIN award_program_map apm ON apm.award_id = a.award
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


@ns.route('/')
class AwardssCollection(Resource):
    @ns.expect(awards_arguments)
    @ns.marshal_with(award_model)
    @ns.response(200, 'Success')
    @ns.response(400, 'Validation Error')
    @ns.response(404, 'No awards found.')
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
            query += " AND apm.program_id ~* %s"
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


@ns.route('/<award_uid>')
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

