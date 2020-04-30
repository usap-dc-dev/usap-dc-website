import flask
from flask import request, json, jsonify, Response
import usap
from dicttoxml import dicttoxml


config = json.loads(open('config.json', 'r').read())
datasets_page = flask.Blueprint("datasets_page", __name__)


@datasets_page.route('/api/v1/datasets/<dataset_uid>', methods=['GET'])
@datasets_page.route('/api/v1/datasets', methods=['GET'])
def datasets(dataset_uid=None):
    query_parameters = request.args

    ret_format = query_parameters.get('format')

    if not dataset_uid:
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

    query = """SELECT abstract, awards, creator, d.date_created, date_modified, doi, east, uid AS dataset_uid, keywords, language_id, locations,
               north, nsf_funding_programs, persons, projects, release_date, replaced_by, replaces, science_programs, south, d.title, version, west
               FROM dataset d 
               JOIN dataset_view dv ON dv.uid = d.id
               WHERE TRUE"""
    where = ""
    to_filter = []

    if dataset_uid:
        query += " AND d.id = %s "
        to_filter.append(dataset_uid)
    if release_date:
        query += " AND d.release_date >= %s"
        to_filter.append(release_date)
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

    for res in results:
        res['landing_page'] = "%sview/dataset/%s" % (config['USAP_DOMAIN'], res['dataset_uid'])

    for res in results:
        try:
            if res.get('projects'):
                res['projects'] = json.loads(res['projects'])
        except Exception as e:
            print("ERROR")
            print(str(e))

    if ret_format == 'xml':
        xml = dicttoxml(results)
        return Response(xml, mimetype='text/xml')

    return jsonify(results)