#!/usr/bin/python3

import psycopg2
import psycopg2.extras
import json
import requests
import time

config = json.loads(open('/web/usap-dc/htdocs/config.json', 'r').read())
config.update(json.loads(open('/web/usap-dc/htdocs/inc/report_config.json', 'r').read()))

api = 'https://api.crossref.org/works?filter=award.number:'
bib_api = 'https://api.crossref.org/works/%s/transform/text/x-bibliography'

log_file = '/web/usap-dc/htdocs/inc/crossref_harvest.log'
sql_file = '/web/usap-dc/htdocs/inc/crossref_sql.txt'
ref_uid_file = '/web/usap-dc/htdocs/inc/ref_uid'


def connect_to_db():
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER_CURATOR'],
                            password=info['PASSWORD_CURATOR'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


def crossref2ref_text(item):
    # probably not needed - all publications appear to have a DOI that works with the API
    ref_text = ""
    if item.get('author'):
        for author in item['author']:
            first_names = author['given'].split(' ')
            initials = ""
            for name in first_names:
                initials += "%s." % name[0]

            ref_text += "%s, %s" % (author['family'], initials)

    year = ""
    if item.get('published_online'):
        year = item['published_online']['date-parts'][0][0]
    elif item.get('published_print'):
        year = item['published_print']['date-parts'][0][0]
    elif item.get('created'):
        year = item['created']['date-parts'][0][0]

    if year != "":
        ref_text += "(%s). " % year

    if item.get('title'):
        ref_text += " %s." % item['title'][0]

    if item.get('container-title'):
        ref_text += " %s" % item['container-title'][0]

    if item.get('volume'):
        ref_text += ", %s" % item['volume']

    if item.get('issue'):
        ref_text += "(%s)" % item['issue']

    if item.get('page'):
        ref_text += ", %s" % item['page']

    ref_text += "."

    print(("*****REF_TEXT GENERATED FROM CROSSREF:\n%s") % ref_text)

    return ref_text


def isNsfFunder(funders, award, doi):
    nsf_dois = ['10.13039/100000001', '10.13039/100000087', '10.13039/100000162', '10.13039/100007352', '10.13039/100006447']
    nsf_names = ['National Science Foundation', 'NSF', 'Polar', 'Antarctic', 'Ice Sheets', 'WAIS', 'LTER', 'Southern Ocean']

    award_dash = award[0:2] + "-" + award[2:]
    for funder in funders:
        if funder.get('DOI') and funder['DOI'] in nsf_dois and funder.get('award') and (award in funder['award'] or award_dash in funder['award']): 
            return True
        if funder.get('name') and any(n in funder['name'] for n in nsf_names) and funder.get('award') and (award in funder['award'] or award_dash in funder['award']):
            return True
    return False


def get_crossref_pubs(new_only=True):
    (conn, cur) = connect_to_db()
    query = "SELECT * FROM project_award_map"
    cur.execute(query)
    res = cur.fetchall()
    output = ""
    sql = ""
    new_refs = {}
    new_proj_refs = set()
    if new_only:
        with open(log_file, "r") as file:
            last_harvest_datetime = float(file.read())

    # for each entry in the project_award map, get the award_id, and look up publications in crossref
    for p in res:
        proj_uid = p['proj_uid']
        award_id = p['award_id']

        if award_id == "None":
            continue
        try:
            r = requests.get(api + award_id).json()
        except Exception as e:
            print(e)
            continue

        items = r['message'].get('items')
        if not items:
            continue
        # for each publication, see if it has a DOI
        for item in items:
            # try and check whether the reference has NSF as the funder - this should weed out most non-polar refs
            is_nsf = isNsfFunder(item.get('funder'), award_id, item.get('DOI'))
            # comment out SQL for non-nsf refs so that Curator can decide whether to add it to DB
            if is_nsf: 
                comment =  '' 
            else: 
                comment = '--'
            # if new_only parameter is set, only harvest publications that were indexed since the last time we ran the harvest
            if new_only:
                index_datetime = item['indexed']['timestamp']/1000
                if index_datetime < last_harvest_datetime:
                    continue

            ref_doi = item.get('DOI')
            if ref_doi and ref_doi != '':
                # use the DOI to get the cite-as text from the x-bibliography API
                bib_url = bib_api % ref_doi
                r_bib = requests.get(bib_url)
                if r_bib.status_code == 200 and r_bib.content:
                    # split off the DOI in the cite-as string, as we don't need in our ref_text
                    ref_text = r_bib.content.decode().rsplit(' doi', 1)[0]
                    ref_text = ref_text.rsplit(' https://doi', 1)[0]
                else:
                    # if x-bibliography API doesn't return anything, generate ref_text from what we have in crossref
                    ref_text = crossref2ref_text(item)
            else:
                # if no DOI, generate ref_text from what we have in crossref
                ref_text = crossref2ref_text(item)

            ref_text = ref_text.replace('\n', '').replace('\r', '')

            # check if publication has already been added during this run of this script
            if ref_doi in new_refs:
                ref_uid = new_refs[ref_doi]
            else:
                # check if publicaton already exists in reference table, using DOI or ref_text
                query = "SELECT * FROM reference WHERE doi = '%s' OR ref_text = '%s'" % (ref_doi, ref_text)
                cur.execute(query)
                res2 = cur.fetchall()

                if len(res2) == 0:
                    # If not already in reference table, add to table
                    ref_uid = generate_ref_uid()
                    sql += comment + "INSERT INTO reference (ref_uid, ref_text, doi, from_crossref) VALUES ('%s', '%s', '%s', 't');\n" % (ref_uid, ref_text, ref_doi)
                else:
                    ref_uid = res2[0]['ref_uid']

                new_refs[ref_doi] = ref_uid

            # Add to project_ref_map, if not already there
            # Check DB and also if already added during this run of the script
            proj_ref = "%s-%s" % (proj_uid, ref_uid)

            query = "SELECT COUNT(*) FROM project_ref_map WHERE proj_uid='%s' AND ref_uid='%s'" % (proj_uid, ref_uid)
            cur.execute(query)
            res2 = cur.fetchone()
            if res2['count'] == 0 and proj_ref not in new_proj_refs:
                new_proj_refs.add(proj_ref)
                sql += comment + "INSERT INTO project_ref_map (proj_uid, ref_uid) VALUES ('%s', '%s');\n" % (proj_uid, ref_uid)

                # include project details to help curator decide whether to include
                query = "SELECT * FROM project_view WHERE uid = '%s'" % proj_uid
                cur.execute(query)
                proj = cur.fetchone()
                sql += "-- Project Title: %s\n-- Project People: %s\n" % (proj['title'].replace('\n', '').replace('\r', ''), proj['persons'])

                # include commented link to API URL so curator can check if not sure whether to include
                sql += "-- %s%s\n\n" % (api, award_id)

                # generate list for Weekly Report
                proj_url = config['PROJECT_LANDING_PAGE'] % proj_uid
                if is_nsf:
                    output += """<li><a href="%s">%s</a> %s</li>""" % (proj_url, proj_uid, ref_text)
                else:
                    output += """<li><i>(<a href="%s">%s</a> %s)</i></li>""" % (proj_url, proj_uid, ref_text)

    if sql != '':    
        # write sql to a file to be ingested by curator
        sql += "\nCOMMIT;\n"
        with open(sql_file, 'a') as file:
            # file.write(sql.encode(encoding="ascii", errors="replace"))
            file.write(sql)

    # write timestamp to log
    with open(log_file, 'w') as file:
        file.write(str(time.time()))

    return output


def generate_ref_uid():
    old_uid = int(open(ref_uid_file, 'r').readline().strip())
    new_uid = old_uid + 1
    ref_uid = 'ref_%0*d' % (7, new_uid)

    with open(ref_uid_file, 'w') as refFile:
        refFile.write(str(new_uid))

    return ref_uid


if __name__ == '__main__':
    get_crossref_pubs(new_only=True)
