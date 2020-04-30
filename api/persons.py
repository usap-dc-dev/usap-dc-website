import flask
from flask import request, json, jsonify, Response
import usap
from dicttoxml import dicttoxml


config = json.loads(open('config.json', 'r').read())
persons_page = flask.Blueprint("persons_page", __name__)


@persons_page.route('/api/v1/persons/<person_uid>', methods=['GET'])
@persons_page.route('/api/v1/persons', methods=['GET'])
def persons(person_uid=None):
    query_parameters = request.args

    ret_format = query_parameters.get('format')

    if not person_uid:
        person_uid = query_parameters.get('person_uid')
    first_name = query_parameters.get('first_name')
    last_name = query_parameters.get('last_name')
    organization = query_parameters.get('organization')

    query = """SELECT p.id, p.first_name, p.middle_name, p.last_name, p.organization, pp.projects, pd.datasets 
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
    where = ""
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

    if ret_format == 'xml':
        xml = dicttoxml(results)
        return Response(xml, mimetype='text/xml')

    return jsonify(results)