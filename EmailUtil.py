import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


# Gmail settings
smtp_server_name = "smtp.gmail.com"
smtp_server_port = 587
from_email = "sahil.mahe@gmail.com"
from_email_pwd = "Welcome2020!"
to_email = "amitji@gmail.com"


# def __init__(self):
    # print ("Calling GetNSELiveData constructor")

'''
def sendMail(message):

    try:
        server = smtplib.SMTP(smtp_server_name, smtp_server_port)

        server.ehlo()
        server.starttls()
        server.login(from_email, from_email_pwd)
        server.sendmail(from_email, to_email, message)
        server.close()
        print ('successfully sent the mail')

    except Exception as e:
        print ("error e - ", str(e))
        print ("\n******Amit - failed to send mail, some exception sending email")

'''

def send_email_as_text(process_name, updated_stock_list, url):
    #update these from my_server_info
    # gmail_user = ""
    # gmail_pwd = "A"
    # FROM = "amit@stockcircuit.in"
    # TO = ""
    subject = "Process finished - ", process_name
    text = process_name, updated_stock_list
    text += "\n\nNow run the URL - ", url

    message = """From: %s\nTo: %s\nSubject: %s\n\n%s
    """ % (from_email, ", ".join(to_email), subject, text)


    try:
        server = smtplib.SMTP(smtp_server_name, smtp_server_port)
        server.ehlo()
        server.starttls()
        server.login(from_email, from_email_pwd)
        server.sendmail(from_email, to_email, message)
        server.close()
        print ('successfully sent the mail')


    except  Exception as e2:
        print ("error e2 - ", str(e2))
        print ("\n******Amit - failed to send mail, some exception sending email")



def send_email_as_html(subject, body, url):
    try:
        message = MIMEMultipart('alternative')
        message['Subject'] = subject
        message['From'] = from_email
        message['To'] = to_email
        part1 = MIMEText(subject, 'plain')
        part2 = MIMEText(body, 'html')
        message.attach(part1)
        message.attach(part2)
        # print('\n Message - ', message.as_string())
        # message = message.encode('ascii', 'ignore').decode('ascii')
      
        server = smtplib.SMTP(smtp_server_name, smtp_server_port)
        server.ehlo()
        server.starttls()
        server.login(from_email, from_email_pwd)
        server.sendmail(from_email, to_email, message.as_string())
        # server.sendmail(from_email, to_email, message.encode('utf-8'))
        server.close()
        print ('successfully sent the mail')


    except  Exception as e2:
        print ("error e2 - ", str(e2))
        print ("\n******Amit - failed to send mail, some exception sending email")        
'''
def send_email_with_body(title, body):
    import smtplib
    gmail_user = "a"
    gmail_pwd = ""
    FROM = "amit@stockcircuit.in"
    TO = ""

    message = """From: %s\nTo: %s\nSubject: %s\n\n%s
    """ % (FROM, ", ".join(TO), title, body)


    try:
        # server = smtplib.SMTP("smtp.gmail.com", 465)
        server = smtplib.SMTP("mail.stockcircuit.in", 587)
        server.ehlo()
        server.starttls()
        server.login(gmail_user, gmail_pwd)
        server.sendmail(FROM, TO, message)
        server.close()
        print ('successfully sent the mail')
    except  Exception as e2:
        print ("error e2 - ", str(e2))
        print ("\n******Amit - failed to send mail, some exception sending email")
'''
