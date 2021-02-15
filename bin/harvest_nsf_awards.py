#!/opt/rh/python27/root/usr/bin/python

import psycopg2
import psycopg2.extras
import json
import requests
import datetime
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

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
                          'ANT Coordination & Information':'Antarctic Coordination and Information'}

def connect_to_db():
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER_CURATOR'],
                            password=info['PASSWORD_CURATOR'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


def sendEmail(message, subject):
    sender = config['USAP-DC_GMAIL_ACCT']
    recipients = config['RECIPIENTS']

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)

    content = MIMEText(message, 'html', 'utf-8')
    msg.attach(content)

    smtp_details = config['SMTP']
    s = smtplib.SMTP(smtp_details["SERVER"], smtp_details['PORT'].encode('utf-8'))
    # identify ourselves to smtp client
    s.ehlo()
    # secure our email with tls encryption
    s.starttls()
    # re-identify ourselves as an encrypted connection
    s.ehlo()
    s.login(smtp_details["USER"], smtp_details["PASSWORD"])
    s.sendmail(sender, recipients, msg.as_string())
    s.quit()  


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
        'cfdaNumber'
    ]   

    awards = []

    # -- the api returns only max 25 records, for more record we have to loop over the page parameter until nothing is returned anyumore
    not_done = True
    offset = 1
    num_records = 0

    while not_done:
        parameter = {'offset': offset,
                    'fundProgramName': 'ANT',
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
    return string.replace("'","''")


def update_award(awards):
    '''
    check if award is in database,
    if not, insert award
    '''
    (conn, cur) = connect_to_db()
    out_text = ''
    new_awards = 0

    for item in awards:
        # check if dataset_id already exist in table
        sql_line = "Select award " \
                   "From award " \
                   "WHERE award = '{0}';".format(item['id'])
        cur.execute(sql_line)
        data = cur.fetchall()
        # if no data returned use insert otherwise update
        if not data:
            new_awards += 1
            pi = escapeQuotes(item['piLastName'] + ', ' + item['piFirstName'])
            if item.get('coPDPI'):
                copi = '; '.join(item['coPDPI'])
                copi = ' '.join(copi.split())
                copi = escapeQuotes(copi)
            else:
                copi = None
            collab = 'Collaborative Research' in item['title']
            try:
                sql_line = "INSERT INTO award " \
                        "(award, dir, div, title, iscr, isipy, copi, start, "\
                        "expiry, sum, name, email, org, orgaddress, orgcity, "\
                        "orgstate, orgzip) "\
                        "VALUES ('{0}','GEO','OPP','{1}','{2}','False',"\
                        "'{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}');" \
                        .format(item['id'], escapeQuotes(item.get('title','')), 
                                collab, copi, item.get('startDate',''), item.get('expDate',''),
                                escapeQuotes(item.get('abstractText','')).encode('utf-8'), pi, item.get('piEmail',''),
                                escapeQuotes(item.get('awardeeName','')), item.get('awardeeAddress',''),
                                item.get('awardeeCity',''),item.get('awardeeState',''),item.get('awardeeZip',''))
         
                cur.execute(sql_line)
                out_text += """<b>Award added.</b>
                               <br><b>ID:</b> %s<br>
                               <b>Title:</b> %s<br>
                               <b>PI:</b> %s<br>
                               <b>Start Date:</b> %s<br>
                               <b>Program:</b> %s<br><br>""" \
                               % (item['id'], item['title'], pi, item['startDate'], item['fundProgramName'])
            except Exception as e:
                text += "Database Error. %s<br>" % sys.exc_info()[1][0]
                print(text)
                sendEmail(text,'Unsuccessul Awards Harvest')
                sys.exit(1)

        else:
            pass

    # Make the changes to the database persistent
    conn.commit()
    cur.close()
    conn.close() 
    # print(out_text) 
    print("New awards added to DB: %s" % new_awards)  
    out_text +=  "New awards added to DB: %s<br>" % new_awards

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
                    text += "Database Error. %s<br>" % sys.exc_info()[1][0]
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
    # Get awards from NSF API for last 4 years
    start_date = (datetime.datetime.now() - datetime.timedelta(days=4*365)).strftime('%m/%d/%Y')
    awards = getAwardsFromNSF(start_date)
    
    # Update award table
    out_text = update_award(awards)

    # Update award_program_map
    out_text = update_award_program(awards, out_text)

    sendEmail(out_text, 'NSF Awards Harvest Completed')
