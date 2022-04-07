import os
from apiclient.discovery import build
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from google.auth.transport.requests import Request as gRequest
import base64
import pickle
import mimetypes


GMAIL_PICKLE = "/web/usap-dc/htdocs/inc/token.pickle"


def connect_to_gmail():
    creds = None
    
    if os.path.exists(GMAIL_PICKLE):
        with open(GMAIL_PICKLE, 'rb') as token:
            creds = pickle.load(token)
    else:
        # if the pickle doesn't exist, need to run the bin/gmail_quickstart.py on local system to
        # log in and create token.pickle. Then copy it to inc/token.pickle
        return None, "Unable to authorise connection to account"

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(gRequest())
        else:
            return None, "Gmail credentials are not valid"

    service = build('gmail', 'v1', credentials=creds)
    return service, None


def create_gmail_message(sender, recipients, subject, message_text, file=None):
    """Create a message for an email.

    Args:
        sender: Email address of the sender.
        to: Email address of the receiver.
        subject: The subject of the email message.
        message_text: The text of the email message.
        file: Path to file to be sent as attachment

    Returns:
        An object containing a base64url encoded email object.
    """
    message = MIMEMultipart('mixed')
    message['To'] = ', '.join(recipients)
    message['From'] = sender
    message['Subject'] = subject
    content = MIMEText(message_text, 'html', 'utf-8')
    message.attach(content)

    if file:
        content_type, encoding = mimetypes.guess_type(file)
        if content_type is None or encoding is not None:
            content_type = 'application/octet-stream'
        main_type, sub_type = content_type.split('/', 1)
        if main_type == 'text':
            fp = open(file, 'rb')
            msg = MIMEText(fp.read().decode(), _subtype=sub_type)
            fp.close()
        elif main_type == 'image':
            fp = open(file, 'rb')
            msg = MIMEImage(fp.read(), _subtype=sub_type)
            fp.close()
        elif main_type == 'audio':
            fp = open(file, 'rb')
            msg = MIMEAudio(fp.read(), _subtype=sub_type)
            fp.close()
        else:
            fp = open(file, 'rb')
            msg = MIMEBase(main_type, sub_type)
            msg.set_payload(fp.read())
            fp.close()
        filename = os.path.basename(file)
        msg.add_header('Content-Disposition', 'attachment', filename=filename)
        message.attach(msg)
    try:
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
    except Exception as e:
        raw = base64.urlsafe_b64encode(message.as_bytes())

    return {'raw': raw}


def send(service, user_id, message):
    """Send an email message.

    Args:
        service: Authorized Gmail API service instance.
        user_id: User's email address. The special value "me"
        can be used to indicate the authenticated user.
        message: Message to be sent.

    Returns:
        success and error messages.
    """
    success = None
    error = None
    try:
        message = (service.users().messages().send(userId=user_id, body=message)
                .execute())
        # print('Message Id: %s' % message['id'])
        success = "Email sent"
        return success, error
    except Exception as error:
        print('An error occurred: %s' % error)
        err = "Error sending email: " + str(error)
        return success, err


def send_gmail_message(sender, recipients, subject, message_text, file):
    success = None
    error = None

    msg_raw = create_gmail_message(sender, recipients, subject, message_text, file)

    service, error = connect_to_gmail()
    if error:
        err = 'ERROR connecting to gmail' + str(error)
        return success, err
    else:
        return send(service, 'me', msg_raw)