#!/root/usr/bin/python3
# Run as a cron task to generate quarterly reports for project managers
# run from main usap-dc directory with python bin/pmReport

import datetime
from dateutil.relativedelta import relativedelta
import time
import psycopg2
import psycopg2.extras
import json
import os
import sys
import shutil
from gmail_functions import send_gmail_message
import io
import requests


config = json.loads(open('/web/usap-dc/htdocs/config.json', 'r').read())
config.update(json.loads(open('/web/usap-dc/htdocs/inc/report_config.json', 'r').read()))
TMP_DIR = 'tmp'


def connect_to_db():
    # info = config['PROD_DATABASE'] # when running on dev server, so we can access prouction DB
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER'],
                            password=info['PASSWORD'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


def querySubmissionTable(cur, submission_type, status):
    query = "SELECT * FROM submission WHERE submission_type = '%s' AND status = '%s';" % (submission_type, status)
    cur.execute(query)
    res = cur.fetchall()
    msg = ""
    for d in res:
        url = config['CURATOR_PAGE'] % d['uid']
        msg += """<li><a href="%s">%s</a> %s</li>""" % (url, d['uid'], d['submitted_date'].strftime('%Y-%m-%d'))
    msg += """</ul>"""
    return msg


def sendEmail(message_text, subject, file=None):
    print(subject)
    sender = config['USAP-DC_GMAIL_ACCT']
    recipients = config['RECIPIENTS']
    success, error = send_gmail_message(sender, recipients, subject, message_text, file)
    if error:
        print(error)
        sys.exit()


def getCMRUrl(dif_id):
    # get the CMR page for a dif record
    if not dif_id:
        return ''

    # use the CMR API to get the concept-id
    try:
        api_url = config['CMR_API'] + dif_id
        r = requests.get(api_url).json()
        concept_id = r['feed']['entry'][0]['id']
        # generate the GCMD page URL
        cmr_url = config['CMR_URL'] + concept_id + '.html'
        return cmr_url
    except:
        return ''


if __name__ == '__main__':
    # get current date
    today = datetime.date.today()

    today = today - relativedelta(days=10)
    six_months_ago = "2021-03-10" #today - relativedelta(months=6)
    (conn, cur) = connect_to_db()

    # make tmp dir for csv files
    os.mkdir(TMP_DIR)

    query = "SELECT * FROM program WHERE id ~* 'Antarctic' OR id ='Post Doc/Travel';"
    cur.execute(query)
    res = cur.fetchall()

    for r in res:
        program = r['id']
        # if program != "Antarctic Organisms and Ecosystems": continue
        send_report = False

        title = "USAP-DC SEMI-ANNUAL REPORT FOR %s: %s TO %s" % (program.upper(), six_months_ago, today)

        msg = """<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
                <title>%s</title>""" % title

        msg += """<html><head>
                        <style>
                            table, th, td {
                                border: 1px solid black;
                                border-collapse: collapse;
                            }
                        </style>
                </head><body><h1>%s</h1>""" % title

        msg += "<h3><i>Attatched spreadsheet contains all active awards for all programs.</i></h3>"

        # new projects/data added and updated to DB
        query = """SELECT project.proj_uid, project.title AS proj_title, project.date_created, JSON_AGG(a.*) awards,d.num_datasets, JSON(d.datasets::text) datasets
                    FROM project
                    JOIN project_award_map pam ON pam.proj_uid = project.proj_uid
                    JOIN (
                    	SELECT * FROM award
                    	JOIN award_program_map apm ON apm.award_id = award.award
                    	JOIN program ON program.id = apm.program_id
                    	) a ON a.award = pam.award_id
                    JOIN (
                        SELECT proj_uid, JSON_AGG(pd.*) AS datasets , COUNT(pdm.dataset_id) AS num_datasets
                        FROM project_dataset_map pdm 
                        JOIN project_dataset pd ON pd.dataset_id = pdm.dataset_id
                        GROUP BY proj_uid
                    ) d ON d.proj_uid=project.proj_uid
                    WHERE project.date_created >= '%s'
                    AND a.program_id='%s'
                    GROUP BY project.proj_uid,d.num_datasets,d.datasets::text
                    ORDER BY project.date_created
                    ;""" % (six_months_ago, program)
        cur.execute(query)
        res2 = cur.fetchall()
        if len(res2) > 0: send_report = True

        msg += """<h2>New Projects Added to USAP-DC in Last Six Months:</h2>"""
        for p in res2:
            url = config['PROJECT_LANDING_PAGE'] % p['proj_uid']
            awards = ""
            datasets = ""
            for a in p['awards']:
                awards += """<br>&emsp;<b>%s</b> (PEC: %s, PI: %s)""" % (a['award'], a['pec'], a['name'])
            for d in p['datasets']:
                datasets += """<br>&emsp;<a href='%s'>%s</a> (%s)""" % (d['url'], d['title'], d['repository'])
            msg += """<b>Project Title:</b> %s<br>
                      <b>Award(s):</b> %s<br>
                      <b>Date Created:</b> %s<br>
                      <b>Number of Datasets:</b> %s<br>
                      <b>Dataset Links:</b> %s<br>
                      <b>Project Landing Page:</b> %s <br><br>""" \
                      % (p['proj_title'], awards, p['date_created'], p['num_datasets'], datasets, url)


        # new datasets submitted to USAP-DC        
        query = """SELECT dataset.id, dataset.title AS ds_title, dataset.date_created, json_agg(a.*) AS awards 
                    FROM dataset
                    JOIN dataset_award_map dam ON dam.dataset_id = dataset.id
                    JOIN (
                    	SELECT * FROM award
                    	JOIN award_program_map apm ON apm.award_id = award.award
                    	JOIN program ON program.id = apm.program_id
                    	) a ON a.award = dam.award_id
                    WHERE dataset.date_created > '%s'
                    AND a.program_id='%s'
                    GROUP by dataset.id, dataset.title, dataset.date_created
                    ORDER BY dataset.date_created;""" % (six_months_ago, program)
        cur.execute(query)
        res2 = cur.fetchall()
        if len(res2) > 0: send_report = True

        msg += """<h2>New Datasets Added to USAP-DC in Last Six Months:</h2>"""
        for d in res2:
            url = config['DATASET_LANDING_PAGE'] % d['id']
            awards = ""
            for a in d['awards']:
                awards += """<br>&emsp;<b>%s</b> (PEC: %s, PI: %s)""" % (a['award'], a['pec'], a['name'])
            msg += """<b>Dataset Title:</b> %s<br>
                      <b>Award(s):</b> %s<br>
                      <b>Date Created:</b> %s<br>
                      <b>Dataset Landing Page:</b> %s <br><br>""" \
                      % (d['ds_title'], awards, d['date_created'], url)


        # new dataset links to project pages
        query = """SELECT project.proj_uid,project.title AS proj_title, ppm.person_id AS proj_pi, project.date_created, project.date_modified, 
                    JSON_AGG(a.*) awards, JSON(d.datasets::text) datasets, JSON(pubs.pubs::text) pubs
                    FROM project
                    JOIN project_person_map ppm ON ppm.proj_uid = project.proj_uid
                    JOIN project_award_map pam ON pam.proj_uid = project.proj_uid
                    JOIN (
                    	SELECT * FROM award
                    	JOIN award_program_map apm ON apm.award_id = award.award
                    	JOIN program ON program.id = apm.program_id
                    	) a ON a.award = pam.award_id
                    JOIN (
                        SELECT proj_uid, JSON_AGG(pd.*) AS datasets 
                        FROM project_dataset_map pdm 
                        JOIN project_dataset pd ON pd.dataset_id = pdm.dataset_id
                        GROUP BY proj_uid
                    ) d ON d.proj_uid=project.proj_uid
                    JOIN (
                        SELECT proj_uid, JSON_AGG(r.*) AS pubs 
                        FROM project_ref_map prm 
                        JOIN reference r ON r.ref_uid = prm.ref_uid
                        GROUP BY proj_uid
                    ) pubs ON pubs.proj_uid=project.proj_uid  
                    WHERE project.date_modified >= '%s' AND date_modified != date_created
                    AND ppm."role" IN ('Investigator and contact', 'Investigator')
                    AND a.program_id='%s'
                    GROUP BY project.proj_uid, d.datasets::text, pubs.pubs::text, ppm.person_id
                    ORDER BY project.proj_uid;""" % (six_months_ago, program)
        cur.execute(query)
        res2 = cur.fetchall()
        if len(res2) > 0: send_report = True

        msg += """<h2>Projects Updated During Last Six Months:</h2>"""
        for p in res2:
            url = config['PROJECT_LANDING_PAGE'] % p['proj_uid']
            awards = ""
            datasets = ""
            pubs = ""
            for a in p['awards']:
                awards += """<br>&emsp;<b>%s</b> (PEC: %s, PI: %s) - %s""" % (a['award'], a['pec'], a['name'], a['title'])
            for d in p['datasets']:
                datasets += """<br>&emsp;<a href='%s'>%s</a> (%s)""" % (d['url'], d['title'], d['repository'])
            for r in p['pubs']:
                pubs += """<br>&emsp;<i>- %s</i>""" % (r['ref_text'])

            # use this output once we can show new datasets and new references
            # msg += """<b>Project Title:</b> %s<br>
            #           <b>Award(s):</b> %s<br>
            #           <b>Dataset Links:</b> %s<br>
            #           <b>Publications:</b> %s<br><br>
            #           """ % (p['proj_title'], awards, datasets, pubs)        

            msg += """<b>Project Title:</b> %s<br>
                      <b>PI:</b> %s<br>
                      <b>Project Landing Page:</b> %s<br><br>
                      """ % (p['proj_title'], p['proj_pi'] , url)  


        # datasets modified
        # query = """SELECT dataset.title AS ds_title, dataset.date_created, dataset.date_modified, award.*, dam.*, program.pec 
        #             FROM dataset
        #             JOIN dataset_award_map dam ON dam.dataset_id = dataset.id
        #             JOIN award ON award.award = dam.award_id
        #             JOIN award_program_map apm ON apm.award_id = award.award
        #             JOIN program ON program.id = apm.program_id
        #             WHERE dataset.date_modified > '%s' AND date_modified != date_created
        #             AND apm.program_id='%s';""" % (six_months_ago, program)
        # cur.execute(query)
        # res2 = cur.fetchall()
        # if len(res2) > 0: send_report = True

        # msg += """<h2>Datasets Updated in USAP-DC in Last Quarter:</h2>"""
        # for d in res2:
        #     url = config['DATASET_LANDING_PAGE'] % d['dataset_id']
        #     msg += """<b>Dataset Title:</b> %s<br>
        #               <b>Award ID:</b> %s<br>
        #               <b>PEC:</b> %s<br>
        #               <b>PI:</b> %s<br>
        #               <b>Award Title:</b> %s<br>
        #               <b>Date Updated:</b> %s<br>
        #               <b>Dataset Landing Page:</b> %s <br><br>""" \
        #               % (d['ds_title'], d['award_id'], d['pec'], d['name'], unicode(d['title'], 'utf-8'), d['date_modified'], url)      


        # all active awards
        query = """SELECT award.*, program.pec, program.id as program_id, p.proj_uid, d.num_datasets, nla.non_lead_awards, pdm.dif_id 
                FROM award
                    JOIN award_program_map apm ON apm.award_id = award.award
                    JOIN program ON program.id = apm.program_id
                    LEFT JOIN project_award_map pam ON  pam.award_id = award.award
                    LEFT JOIN project p ON p.proj_uid= pam.proj_uid
                    LEFT JOIN (
                                SELECT proj_uid, COUNT(dataset_id) AS num_datasets 
                                FROM project_dataset_map pdm GROUP BY proj_uid
                    ) d ON d.proj_uid=p.proj_uid
                    LEFT JOIN (
                  		  SELECT a2.award as lead_award, JSON_AGG(a1.award) as non_lead_awards 
                  		  FROM award a1
                  		  JOIN award a2 ON a2.award= a1.lead_award_id
                  		  WHERE a1.is_lead_award = 'Non-Lead'
                  		  GROUP BY a2.award
                  	) nla ON nla.lead_award = award.award
                    LEFT JOIN project_dif_map pdm ON pdm.proj_uid = p.proj_uid
                    WHERE (program.id ~* 'Antarctic' OR program.id ='Post Doc/Travel')
                    AND start <= '%s' AND expiry >= '%s'
                    AND award.is_lead_award IN ('Standard', 'Lead')
                    ORDER BY award;""" % (today, today)

        cur.execute(query)
        res2 = cur.fetchall()
        if len(res2) > 0: send_report = True

        # make csv file
        filename = "Active_Awards_%s_to_%s.tsv" % (six_months_ago, today) 
        filepath = os.path.join('tmp', filename.replace('/','_'))
        tsv_file = io.open(filepath, 'w', encoding="utf-8")
        tsv_file.write("Award ID\tProgram\tPEC\tPI\tAward Title\tAward Start\tAward Expiry\tNon-Lead Awards\tNumber of Datasets\tProject Landing Page\tAMD Record\n")

        msg += """<h2>Summary of All Active Awards:</h2>"""
        msg += """<table><thead><tr>
                    <th>Award ID</th>
                    <th>PEC</th>
                    <th>PI</th>
                    <th>Award Title</th>
                    <th>Award Start</th>
                    <th>Award Expiry</th>
                    <th>Non-Lead Awards</th>
                    <th>Number of Datasets</th>
                    <th>Project Landing Page</th>
                    <th>AMD Record</th>
                </th></thead>"""
        for a in res2:
            if a['proj_uid']:
                url = config['PROJECT_LANDING_PAGE'] % a['proj_uid']
            else:
                url = ''
            if a['non_lead_awards']:
                nla = ', '.join(a['non_lead_awards'])
            else:
                nla = ''
            amd_link = getCMRUrl(a['dif_id'])
            tsv_file.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t %s\t%s\n" % (
                a['award'], a['program_id'], a['pec'], a['name'], a['title'], a['start'], 
                a['expiry'], nla, a['num_datasets'], url, amd_link))
            if a['program_id'] == program:   
                msg += """<tr><td>%s</td>
                            <td>%s</td>
                            <td>%s</td>
                            <td>%s</td>
                            <td>%s</td>
                            <td>%s</td>
                            <td>%s</td>
                            <td>%s</td>
                            <td>%s</td>
                            <td>%s</td>
                        </tr>""" \
                        % (a['award'], a['pec'], a['name'], a['title'], a['start'], 
                        a['expiry'], nla, a['num_datasets'], url, amd_link)
        msg += "</table>"
        tsv_file.close()

        msg += """</body></html>"""

        if send_report: sendEmail(msg, title, file=filepath)
        # need to slow process down so email function doesn't get overwhelmed
        time.sleep(1)

    # remove tmp dir
    shutil.rmtree(TMP_DIR)