#!/usr/bin/python3

import psycopg2
import psycopg2.extras
import json
import requests
import datetime
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
#import email.utils as utils
from gmail_functions import send_gmail_message
import csv

nsf_file = "/web/usap-dc/htdocs/inc/nsf_table_2022_08.tsv" # This is a tsv export (copy and paste from the spreadsheet) of Part of PD-3PO-v1.3.2-USAPDC.xlsm 07-Mar-21 11-50-11.xlsx
config = json.loads(open('/web/usap-dc/htdocs/config.json', 'r').read())
config.update(json.loads(open('/web/usap-dc/htdocs/inc/report_config.json', 'r').read()))

api = "https://api.nsf.gov/services/v1/awards.json?"
ant_program_dict = {'ANTARCTIC GLACIOLOGY':'Antarctic Glaciology',
                          'ANTARCTIC INTEGRATED SYS SCI':'Antarctic Integrated System Science',
                          'ANTARCTIC ORGANISMS & ECOSYST':'Antarctic Organisms and Ecosystems',
                          'ANTARCTIC EARTH SCIENCES':'Antarctic Earth Sciences',
                          'ANTARCTIC OCEAN & ATMOSPH SCI':'Antarctic Ocean and Atmospheric Sciences',
                          'Antarctic Astrophys&Geosp Sci':'Antarctic Astrophysics and Geospace Sciences', 
                          'ANTARCTIC INSTRUM & SUPPORT':'Antarctic Instrumentation and Support',
                          'ARCTIC NATURAL SCIENCES':'Arctic Natural Sciences',
                          'ANT Glaciology':'Antarctic Glaciology',
                          'ANT Integrated System Science':'Antarctic Integrated System Science',
                          'ANT Organisms & Ecosystems':'Antarctic Organisms and Ecosystems',
                          'ANT Earth Sciences':'Antarctic Earth Sciences',
                          'ANT Ocean & Atmos Sciences':'Antarctic Ocean and Atmospheric Sciences',
                          'ANT Astrophys & Geospace Sci':'Antarctic Astrophysics and Geospace Sciences', 
                          'ANT Instrum & Facilities':'Antarctic Instrumentation and Facilities',
                          'Instrumentation & Facilities':'Antarctic Instrumentation and Facilities',
                          'ANS-Arctic Natural Sciences':'Arctic Natural Sciences',
                          'Antarctic Science and Technolo':'Antarctic Science and Technology',
                          'Polar Special Initiatives': 'Polar Special Initiatives',
                          'ANT Coordination & Information': 'Antarctic Coordination and Information',
                          'POST DOC/TRAVEL': 'Post Doc/Travel',
                          'Polar Cyberinfrastructure': 'Polar Cyberinfrastructure'}

# read in tsv version of NSF spreadsheet
with open(nsf_file) as csvfile:
    reader = csv.DictReader(csvfile, delimiter='\t')
    nsf_dict = {row['prop_id']:row for row in reader}


def connect_to_db():
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER_CURATOR'],
                            password=info['PASSWORD_CURATOR'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


def sendEmail_old(message, subject):
    sender = config['USAP-DC_GMAIL_ACCT']
    recipients = config['RECIPIENTS']

    msg = MIMEMultipart('alternative')
    #msg['message-id'] = utils.make_msgid()
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)

    content = MIMEText(message, 'html', 'utf-8')
    msg.attach(content)

    success, error = send_gmail_message(sender, recipients, msg['Subject'], msg.as_string(), None, None)     

def sendEmail(message_text, subject, file=None):
    print(subject)
    sender = config['USAP-DC_GMAIL_ACCT']
    recipients = config['RECIPIENTS']
    success, error = send_gmail_message(sender, recipients, subject, message_text, file)
    if error:
        print(error)
        sys.exit()

def getAwardsFromNSF(start_date):
    fields = [
        'id',
        'startDate',
        'expDate',
        'coPDPI',
        'piFirstName',
        'piMiddeInitial',
        'piLastName',
        'piEmail',
        'title',
        'fundProgramName',
        'awardeeName',
        'awardeeAddress',
        'awardeeCity',
        'awardeeStateCode',
        'awardeeZipCode',
        'abstractText',
        'cfdaNumber',
        'poName',
        'poEmail'
    ]   

    awards = []

    # -- the api returns only max 25 records, for more record we have to loop over the page parameter until nothing is returned anyumore
    not_done = True
    offset = 1
    num_records = 0

    while not_done:
        parameter = {'offset': offset,
                    'fundProgramName': 'ANT, "POST DOC/TRAVEL", "Polar Cyberinfrastructure"',
                    'dateStart': start_date,
                    'printFields':','.join(fields)} 

        r = requests.get(api, params=parameter)
        if r.status_code == 200:
            data = r.json()
            num_records += len(data['response']['award'])
            awards += data['response']['award']
            offset += 25
            if len(data['response']['award']) < 25:
                not_done = False

    print("Total Awards: %s" % num_records)
    return awards


def escapeQuotes(string):
    if not string:
        return None
    return string.replace("'", "''")


def isLead(award_id):
    iln_dict = {'I':'Standard', 'L':'Lead', 'N':'Non-Lead'}
    if nsf_dict.get(award_id):
        iln = nsf_dict[award_id]['ILN']
        lead = iln_dict.get(iln, 'Standard')
        lead_id = nsf_dict[award_id]['lead']
        return lead, lead_id
    return 'Standard', ''


def update_award(awards):
    '''
    check if award is in database,
    if not, insert award
    '''
    (conn, cur) = connect_to_db()
    out_text = ''
    new_awards = 0
    updated_awards = 0

    for item in awards:

        pi = escapeQuotes(item['piLastName'] + ', ' + item['piFirstName'])
        if item.get('coPDPI'):
            copi = '; '.join(item['coPDPI'])
            copi = ' '.join(copi.split())
            copi = escapeQuotes(copi)
        else:
            copi = None

        # check if dataset_id already exist in table
        sql_line = "Select * " \
                   "From award " \
                   "WHERE award = '{0}';".format(item['id'])
        cur.execute(sql_line)
        data = cur.fetchall()
        # if no data returned use insert otherwise update
        if not data:
            new_awards += 1

            collab = 'Collaborative Research' in item['title']
            lead, lead_id = isLead(item['id'])

            try:
                sql_line = """INSERT INTO award 
                        (award, dir, div, title, iscr, isipy, copi, start,
                        expiry, sum, name, email, org, orgaddress, orgcity,
                        orgstate, orgzip, po_name, po_email,
                        is_lead_award, lead_award_id, project_needed, letter_welcome, letter_year1, letter_final_year) 
                        VALUES (%s, 'GEO', 'OPP', %s, %s, 'False',
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, True, False, False, False);"""                   
         
                cur.execute(sql_line, (item['id'], escapeQuotes(item.get('title','')), 
                                collab, copi, item.get('startDate',''), item.get('expDate',''),
                                escapeQuotes(item.get('abstractText','')).encode('ascii','ignore'), pi, item.get('piEmail',''),
                                escapeQuotes(item.get('awardeeName','')), item.get('awardeeAddress',''),
                                item.get('awardeeCity',''),item.get('awardeeState',''),item.get('awardeeZip',''),
                                escapeQuotes(item.get('poName', '')), item.get('poEmail',''), lead, lead_id))
                out_text += """<b>Award added.</b>
                               <br><b>ID:</b> %s<br>
                               <b>Title:</b> %s<br>
                               <b>PI:</b> %s<br>
                               <b>Start Date:</b> %s<br>
                               <b>Program:</b> %s<br><br>""" \
                               % (item['id'], item['title'], pi, item['startDate'], item['fundProgramName'])
            except Exception as e:
                text = "Database Error. %s<br>" % sys.exc_info()[1]
                print(text)
                sendEmail(text,'Unsuccessul Awards Harvest')
                sys.exit(1)

        else:
            a = data[0]
            start_date = datetime.datetime.strptime(item['startDate'], '%m/%d/%Y').date()
            expiry_date= datetime.datetime.strptime(item['expDate'], '%m/%d/%Y').date()
            lead, lead_id = isLead(item['id'])
            update = False
            update_text = ""
            if a['start'] != start_date:
                update_text += "<b>Start Date: </b> OLD: <i>%s</i> NEW: <i>%s</i><br>" % (a['start'], start_date)
                update = True
            
            if a['expiry'] != expiry_date:
                update_text += "<b>Expiry Date: </b> OLD: <i>%s</i> NEW: <i>%s</i><br>" % (a['expiry'], expiry_date)
                update = True
            
            if escapeQuotes(a['name']) != pi:
                update_text += "<b>PI: </b> OLD: <i>%s</i> NEW: <i>%s</i><br>" % (a['name'], pi)
                update = True

            if a['email'] != item['piEmail']:
                update_text += "<b>PI Email: </b> OLD: <i>%s</i> NEW: <i>%s</i><br>" % (a['email'], item['piEmail'])
                update = True

            if escapeQuotes(a['copi']) != copi:
                update_text += "<b>COPI: </b> OLD: <i>%s</i> NEW: <i>%s</i><br>" % (a['copi'], copi)
                update = True
   
            if a['po_name'] != item.get('poName',''):
                update_text += "<b>PO Name: </b> OLD: <i>%s</i> NEW: <i>%s</i><br>" % (a['po_name'], item.get('poName',''))
                update = True
        
            if a['po_email'] != item.get('poEmail',''):
                update_text += "<b>PO Email: </b> OLD: <i>%s</i> NEW: <i>%s</i><br>" % (a['po_email'], item.get('poEmail',''))
                update = True

            if a['is_lead_award'] != lead:
                update_text += "<b>Is Lead Award: </b> OLD: <i>%s</i> NEW: <i>%s</i><br>" % (a['is_lead_award'], lead)
                update = True

            if a['lead_award_id'] != lead_id:
                update_text += "<b>Lead Proposal ID: </b> OLD: <i>%s</i> NEW: <i>%s</i><br>" % (a['lead_award_id'], lead_id)
                update = True

            if update:
                updated_awards += 1
                try:
                    sql_line = """UPDATE award SET (start, expiry, name, email, copi, po_name, po_email, is_lead_award, lead_award_id) 
                            = (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            WHERE award = %s;"""
                    cur.execute(sql_line, (start_date, expiry_date, pi, item['piEmail'], copi, escapeQuotes(item.get('poName','')), item.get('poEmail',''), 
                             lead, lead_id, item['id']))

                    out_text += "<b>Award %s Updated</b><br>" % a['award']
                    out_text += update_text + "<br>"
                except Exception as e:
                    text = "Database Error. %s<br>" % sys.exc_info()[1]
                    print(text)
                    sendEmail(text,'Unsuccessul Awards Harvest')
                    sys.exit(1)

    # Make the changes to the database persistent
    conn.commit()
    cur.close()
    conn.close() 
    # print(out_text) 
    print("New awards added to DB: %s" % new_awards)  
    out_text +=  "New awards added to DB: %s<br>" % new_awards
    print("Awards updated in DB: %s" % updated_awards)  
    out_text +=  "Awards updated in DB: %s" % updated_awards

    return out_text


def update_award_program(award_list, out_text):
    ''' run through the award list and check if all entries
        for award_program_map exist
    '''
    (conn, cur) = connect_to_db()
        
    for item in award_list:
        prg = item['fundProgramName']
        # check if award_id and program_id combo already exist in table
        if prg in ant_program_dict:
            prg2 = ant_program_dict[prg]
            sql_line = "Select award_id, program_id " \
                        "From award_program_map " \
                        "WHERE award_id = '{0}' AND program_id = '{1}' ;"\
                        .format(item['id'], prg2)
            cur.execute(sql_line)
            data = cur.fetchall()
            # if no data returned use insert otherwise update
            if not data:
                sql_line = "INSERT INTO award_program_map(award_id, program_id)"\
                            "VALUES ('{0}', '{1}'); "\
                            .format(item['id'], prg2)
                try:
                    cur.execute(sql_line)
                except:
                    text = "Database Error. %s<br>" % sys.exc_info()[1]
                    print(text)
                    sendEmail(text,'Unsuccessul Awards Harvest')
                    sys.exit(1)
        else:
            out_text += '%s is not in dict<br>' % prg


        
    # Make the changes to the database persistent
    conn.commit()
    cur.close()
    conn.close()               
    return out_text


if __name__ == '__main__':
    # Get awards from NSF API for last 5 years
    start_date = (datetime.datetime.now() - datetime.timedelta(days=5*365)).strftime('%m/%d/%Y')
    awards = getAwardsFromNSF(start_date)
    
    # Update award table
    out_text = update_award(awards)

    # Update award_program_map
    out_text = update_award_program(awards, out_text)

    sendEmail(out_text, 'NSF Awards Harvest Completed')
