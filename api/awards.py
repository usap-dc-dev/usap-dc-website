import flask
from flask import request, json, jsonify, Response
import usap
from dicttoxml import dicttoxml


config = json.loads(open('config.json', 'r').read())
awards_page = flask.Blueprint("awards_page", __name__)


@awards_page.route('/api/v1/awards/<award_uid>', methods=['GET'])
@awards_page.route('/api/v1/awards', methods=['GET'])
def awards(award_uid=None):
    query_parameters = request.args

    ret_format = query_parameters.get('format')

    if not award_uid:
        award_uid = query_parameters.get('award_uid')
    name = query_parameters.get('name')
    copi = query_parameters.get('copi')
    dir_code = query_parameters.get('dir')
    div = query_parameters.get('div')
    start = query_parameters.get('start')
    expiry = query_parameters.get('expiry')
    title = query_parameters.get('title')
    nsf_funding_program = query_parameters.get('nsf_funding_program')

    query = """SELECT a.award AS award_uid, a.dir, a.div, a.title, a.iscr, a.isipy, a.copi, a.start, a.expiry, a.sum, a.name, 
               apm.program_id AS nsf_funding_program, ap.projects, ad.datasets 
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
    where = ""
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

    if ret_format == 'xml':
        xml = dicttoxml(results)
        return Response(xml, mimetype='text/xml')

    return jsonify(results)