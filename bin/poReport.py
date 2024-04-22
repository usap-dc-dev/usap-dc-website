from datetime import date
from itertools import repeat
import json
import psycopg2
import psycopg2.extras
from gmail_functions import send_gmail_message
import functools

config = json.loads(open('/web/usap-dc/htdocs/config.json', 'r').read())
config.update(json.loads(open('/web/usap-dc/htdocs/inc/poreport_config.json', 'r').read()))
baseURL = config['USAP_DOMAIN']
dataset_baseURL = baseURL + "view/dataset/"
project_baseURL = baseURL + "view/project/"
lastReportDate = config['LAST_REPORT']

indents = 0

class Indenter:
    indent_char='\t'
    indents = 0

    def __init__(self):
        self.indents = 0
        self.indent_char='\t'
    
    def indent(self):
        self.indents += 1
    
    def outdent(self):
        self.indents -= 1
    
    def getPrefix(self):
        return "".join(list(repeat(self.indent_char, indents)))

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

def today():
    td = date.today()
    return "%s-%s-%s" % (td.year, td.month, td.day)

def getProjectsByPOEmail(poEmail, conn, cur):
    if not conn:
        conn, cur = connect_to_db()
    awards_query = "SELECT DISTINCT award FROM award WHERE po_email=%s AND (project_needed=true OR award IN (SELECT DISTINCT award_id FROM project_award_map)) AND expiry>=%s"
    count_projects_query = "SELECT COUNT(DISTINCT proj_uid) AS proj_count FROM project_award_map WHERE award_id IN (SELECT award FROM award WHERE po_email=%s AND (project_needed=true OR award IN (SELECT DISTINCT award_id FROM project_award_map)) AND expiry>=%s)"
    projects_query = "SELECT DISTINCT proj_uid FROM project_award_map WHERE award_id=%s"
    datasets_query = "SELECT DISTINCT dataset_id FROM project_dataset_map WHERE proj_uid=%s"
    count_datasets_query = "SELECT COUNT(DISTINCT dataset_id) AS ds_count FROM project_dataset_map WHERE proj_uid IN (SELECT DISTINCT proj_uid FROM project_award_map WHERE award_id IN (SELECT award FROM award WHERE po_email=%s  AND (project_needed=true OR award IN (SELECT DISTINCT award_id FROM project_award_map)) AND expiry>=%s))"
    awards = {}
    cur.execute(awards_query, (poEmail, today()))
    awards_rows = cur.fetchall()
    for arow in awards_rows:
        cur.execute(projects_query, (arow['award'],))
        projs_rows = cur.fetchall()
        projs = {}
        for prow in projs_rows:
            cur.execute(datasets_query, (prow['proj_uid'],))
            datasets_rows = cur.fetchall()
            datasets = []
            for drow in datasets_rows:
                datasets.append(drow['dataset_id'])
            projs[prow['proj_uid']] = datasets
        awards[arow['award']] = projs
    cur.execute(count_projects_query, (poEmail, today()))
    numProjs = cur.fetchone()['proj_count']
    cur.fetchall()
    cur.execute(count_datasets_query, (poEmail, today()))
    numDatasets = cur.fetchone()['ds_count']
    cur.fetchall()
    count_awd_with_prj_query = "SELECT COUNT(DISTINCT award_id) AS ct FROM project_award_map WHERE award_id IN (SELECT DISTINCT award FROM award WHERE po_email=%s AND expiry>=%s)"
    cur.execute(count_awd_with_prj_query, (poEmail, today()))
    numAwdWithProj = cur.fetchall()[0]['ct']
    return awards, numProjs, numDatasets, numAwdWithProj

def getProjectName(projNum, conn, cur):
    if not conn:
        conn, cur = connect_to_db()
    query = "SELECT title FROM project WHERE proj_uid=%s"
    cur.execute(query, (projNum,))
    return cur.fetchall()[0]['title']

def getNewProjects(poEmail, conn, cur):
    if not conn:
        (conn, cur) = connect_to_db()
    query = """SELECT project.proj_uid, project.title AS proj_title, project.date_created, JSON_AGG(a.*) awards,d.num_datasets, JSON(d.datasets::text) datasets
                    FROM project
                    JOIN project_award_map pam ON pam.proj_uid = project.proj_uid
                    JOIN (
                    	SELECT * FROM award
                    	JOIN award_program_map apm ON apm.award_id = award.award
                    	JOIN program ON program.id = apm.program_id
                    	) a ON a.award = pam.award_id
                    LEFT JOIN (
                        SELECT proj_uid, JSON_AGG(pd.*) AS datasets , COUNT(pdm.dataset_id) AS num_datasets
                        FROM project_dataset_map pdm 
                        JOIN project_dataset pd ON pd.dataset_id = pdm.dataset_id
                        GROUP BY proj_uid
                    ) d ON d.proj_uid=project.proj_uid
                    WHERE project.date_created >= %s
                    AND a.po_email=%s
                    GROUP BY project.proj_uid,d.num_datasets,d.datasets::text
                    ORDER BY project.date_created
                    ;"""
    cur.execute(query, (lastReportDate, poEmail))
    rows = cur.fetchall()
    newProjects = []
    for row in rows:
        proj = {}
        proj["title"] = row["proj_title"]
        proj["awards"] = row["awards"]
        proj["date_created"] = row["date_created"]
        proj["datasets"] = row["datasets"]
        proj["uid"] = row["proj_uid"]
        newProjects.append(proj)
    return newProjects

def getNewDatasets(poEmail, conn, cur):
    if not conn:
        (conn, cur) = connect_to_db()
    query = """SELECT dataset.id, dataset.title AS ds_title, dataset.date_created, json_agg(a.*) AS awards 
                    FROM dataset
                    JOIN dataset_award_map dam ON dam.dataset_id = dataset.id
                    JOIN (
                    	SELECT * FROM award
                    	JOIN award_program_map apm ON apm.award_id = award.award
                    	JOIN program ON program.id = apm.program_id
                    	) a ON a.award = dam.award_id
                    WHERE dataset.date_created >= %s
                    AND a.po_email=%s
                    GROUP by dataset.id, dataset.title, dataset.date_created
                    ORDER BY dataset.date_created;"""
    cur.execute(query, (lastReportDate, poEmail))
    rows = cur.fetchall()
    datasets = []
    for row in rows:
        ds = {}
        ds["title"] = row["ds_title"]
        ds["awards"] = row["awards"]
        ds["date_created"] = row["date_created"]
        ds["id"] = row["id"]
        datasets.append(ds)
    return datasets


def draftEmail(poEmail, conn, cur):
    if not conn:
        conn, cur = connect_to_db()
    query = "SELECT DISTINCT po_name FROM award WHERE po_email=%s"
    cur.execute(query, (poEmail,))
    rows = cur.fetchall()
    name = rows[0]['po_name'] if len(rows)>0 else "Unknown Program Officer"
    awards, numProjects, numDatasets, numAwardsWithProjects = getProjectsByPOEmail(poEmail, conn, cur)
    newProjectsText = ""
    newDatasetsText = ""
    if len(awards) > 0:
        newProjects = getNewProjects(poEmail, conn, cur)
        if len(newProjects) > 0:
            newProjectsText = functools.reduce(lambda prevText, project:\
                                               """{0}<br><b>Project Title:</b> {1}<br>
                                               <b>Award(s):</b> {2}<br>
                                               <b>Date Created:</b> {3}<br>
                                               <b>Number of Datasets:</b> {4}<br>
                                               <b>Dataset Links:</b> {5}<br>
                                               <b>Project Landing Page:</b> {6} <br>"""\
                                               .format(prevText, project["title"],\
                                                       "None" if 0 == len(project["awards"])\
                                                        else functools.reduce(\
                                                            lambda prevAwardsText, curAward : "%s<br>&emsp;<b>%s</b> (PEC: %s, PI: %s)" %\
                                                                (prevAwardsText, curAward["award"], curAward["pec"], curAward["name"]),\
                                                                project["awards"], ""),\
                                                                    project["date_created"], len(project["datasets"]) if project["datasets"] is not None else 0,\
                                                                        functools.reduce(lambda prevDsText, curDs : "{0}<br>&emsp;<a href=\"{1}\">{2}</a> ({3})"\
                                                                                          .format(prevDsText, curDs["url"], curDs["title"], curDs["repository"]),\
                                                                                          project["datasets"], "<ul style=\"list-style-type: none;margin-top:-10px\">")\
                                                                            if project["datasets"] is not None and len(project["datasets"]) > 0 else "", project_baseURL + project["uid"]),\
                                               newProjects, "<h2>Projects added since %s</h2>" % (lastReportDate,))
        newDatasets = getNewDatasets(poEmail, conn, cur)
        if len(newDatasets) > 0:
            newDatasetsText = functools.reduce(lambda prevText, dataset:\
                                              """{0}<b>Dataset Title:</b> {1}<br>
                                              <b>Award(s):</b> {2}<br>
                                              <b>Dataset Landing Page:</b> {3}<br><br>"""\
                                                .format(prevText, dataset["title"],\
                                                        functools.reduce(lambda prevAwdTxt, award:\
                                                                         "{0}<br>&emsp;<b>{1}</b> (PEC: {2}, PI: {3})"\
                                                                            .format(prevAwdTxt, award["award"], award["pec"], award["name"]), dataset["awards"], ""),\
                                                        dataset_baseURL + dataset["id"])\
                                                , newDatasets, "<h2>Datasets added since %s</h2>" % (lastReportDate,))
        awards_indenter = Indenter()
        empty_awards_indenter = Indenter()
        award_text = "<table width=\"100%\" style=\"max-width:800px\"><tr><th>Award Number</th><th>Project Count</th><th>Project</th><th>Dataset</th></tr>"
        empty_awards_text = "<p>Active awards with no projects:</p>\n<ul>"
        awards_indenter.indent()
        empty_awards_indenter.indent()
        for award in awards:
            projects = awards[award]
            num_projects = len(projects)
            if 0 == num_projects:
                empty_awards_text += "\n{0}<li>{1}</li>".format(empty_awards_indenter.getPrefix(), award)
            else:
                awd_rows = 0
                for project in projects:
                    awd_rows += max(1, len(projects[project]))
                award_text += "\n{0}<tr><td rowspan={1}>{2}</td><td rowspan={1}>{3}</td>".format(awards_indenter.getPrefix(), awd_rows, award, num_projects)
                awards_indenter.indent()
                for k in range(len(projects)):
                    project = list(projects.keys())[k]
                    num_datasets = len(projects[project])
                    prj_rows = max(1, num_datasets)
                    award_text += "\n{0}{5}<td rowspan={1}><a href=\"{2}{3}\">{4}</a></td>".format(awards_indenter.getPrefix(), prj_rows, project_baseURL, project, getProjectName(project, conn, cur), "" if 0 == k else "<tr>")
                    awards_indenter.indent()
                    if 0 == num_datasets:
                        award_text += "\n%s<td>No datasets</td></tr>" % awards_indenter.getPrefix()
                    else:
                        for i in range(num_datasets):
                            award_text += "\n{0}{1}<td><a href=\"{2}{3}\">{3}</a></td></tr>".format(awards_indenter.getPrefix(), ("" if 0==i else "<tr>"), dataset_baseURL, projects[project][i])
                    awards_indenter.outdent()
                awards_indenter.outdent()
        awards_indenter.outdent()
        empty_awards_indenter.outdent()
        empty_awards_text += "\n%s</ul>" % empty_awards_indenter.getPrefix()
        award_text += "\n%s</table>" % awards_indenter.getPrefix()
        if 0 == numAwardsWithProjects:
            award_text = ""
        if len(awards) == numAwardsWithProjects:
            empty_awards_text = ""

    style = """
    table, th, td {
        border: 1px solid black;
        border-collapse: collapse;
        text-align: center;
    }
    """
    msg_text = """<html><head></head><body><p>Dear %s,</p>
    <p>This is a summary of the active awards, projects, and datasets at USAP-DC that list you as the responsible PO.</p>
    <p>There are no such awards.</p>
    <p>Sincerely,<br>The USAP-DC Team</p></body></html>""" % (name,)
    if len(awards) > 0:
        activeAwardsTxt = "You have a total of %d active award%s" % (len(awards), "" if 1 == len(awards) else "s")
        awardsWithProjectsTxt = (", %d of which ha%s at least one project. " % (numAwardsWithProjects, "s" if 1 == numAwardsWithProjects else "ve")) if len(awards) > 0 else ". "
        projectDatasetTxt = "" if 0 == numProjects else (("The only project page has %d dataset%s." % (numDatasets, "" if 1 == numDatasets else "s")) if 1 == numProjects else ("The combined %d project pages have a collective %d dataset%s." % (numProjects, numDatasets, "" if 1 == numDatasets else "s")))
        msg_text = """<html><head><style>%s</style></head><body><p>Dear %s,</p>
        \t<p>This is a summary of the active awards, projects, and datasets at USAP-DC that list you as the responsible PO.</p>
        \n\t<p>%s%s%s</p>
        %s
        %s
        <h2>Summary of All Active Awards</h2>
        %s
        %s
        <p>Sincerely,<br>
        The USAP-DC Team</p></body></html>""" % (style, name, activeAwardsTxt, awardsWithProjectsTxt, projectDatasetTxt, newProjectsText, newDatasetsText, award_text, empty_awards_text)
    return msg_text

conn, cur = connect_to_db()

emails_query = "SELECT DISTINCT po_email, po_name FROM award WHERE length(po_email) > length('@nsf.gov');"
cur.execute(emails_query)
emails_resp = cur.fetchall()
for row in emails_resp:
    po_email = row['po_email']
    po_name = row['po_name']
    text = draftEmail(po_email, conn, cur)
    subject = "From USAP-DC: Program Officer Report for {0} ({1}) as of {2}".format(po_name, po_email, today())
    sender = config['USAP-DC_GMAIL_ACCT']
    recipients = config['RECIPIENTS']
    success, error = send_gmail_message(sender, recipients, subject, text, None)
    if error:
        print(error)