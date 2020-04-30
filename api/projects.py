import flask
from flask import request, json, jsonify, Response
import usap
from dicttoxml import dicttoxml


config = json.loads(open('config.json', 'r').read())
projects_page = flask.Blueprint("projects_page", __name__)


@projects_page.route('/api/v1/projects/<proj_uid>', methods=['GET'])
@projects_page.route('/api/v1/projects', methods=['GET'])
def projects(proj_uid=None):
    query_parameters = request.args

    ret_format = query_parameters.get('format')
    if not proj_uid:
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

    query = """SELECT DISTINCT awards, datasets, p.date_created, date_modified, p.description, east, end_date, keywords, locations, north,
               nsf_funding_programs, persons, p.proj_uid, science_programs, south, start_date, p.title, west, dif.dif_ids, depl.deployments
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
    where = ""
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
        for kw in keywords.split(';'):
            query += " AND keywords ~* %s"
            to_filter.append(kw)
    if locations:
        for loc in locations.split(';'):
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

    for res in results:
        res['landing_page'] = "%sview/project/%s" % (config['USAP_DOMAIN'], res['proj_uid'])

    for res in results:
        try:
            if res.get('datasets'):
                res['datasets'] = json.loads(res['datasets'])
            if res.get('deployments'):
                res['deployments'] = json.loads(res['deployments'])
        except Exception as e:
            print("ERROR")
            print(str(e))

    if ret_format == 'xml':
        xml = dicttoxml(results)
        return Response(xml, mimetype='text/xml')

    return jsonify(results)