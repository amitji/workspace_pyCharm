import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Gmail settings


# Gmail settings
smtp_server_name = "smtp.gmail.com"
smtp_server_port = 587
from_email = "sahil.mahe@gmail.com"
from_email_pwd = "Welcome2020!"
to_email = "amitji@gmail.com"





def __init__(self):
    print ("Calling GetNSELiveData constructor")

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


def send_email_OLD( subject,updated_stock_list, url):
    # me == my email address
    # you == recipient's email address
    #update these from my_server_info 
    me = ""
    me_pwd = ""
    you = ""

    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = me
    msg['To'] = you

    #print type(updated_stock_list)

    part1 = MIMEText(updated_stock_list, 'html')
    #part2 = MIMEText("url = "+url, 'html')
    msg.attach(part1)
    #msg.attach(part2)

    # SUBJECT = "Process finished - ", process_name
    # TEXT = "Updated List is  - ", updated_stock_list
    # TEXT += "Now run the URL - " , url

    # Prepare actual message
    # message = """From: %s\nTo: %s\nSubject: %s\n\n%s
    # """ % (FROM, ", ".join(TO), SUBJECT, TEXT)
    try:
        #server = smtplib.SMTP("smtp.gmail.com", 465)
        server = smtplib.SMTP("mail.stockcircuit.in", 587)

        server.ehlo()
        server.starttls()
        server.login(me, me_pwd)
        #server.sendmail(me, you, msg)
        #print "msg as string", msg.as_string()
        server.sendmail(me, you, msg.as_string())
        server.close()
        print ('successfully sent the mail')
    except  (Exception, e2):
        print ("error e2 - ", str(e2))
        print ("\n******Amit - failed to send mail, some exception sending email")


def send_email_as_text(process_name, updated_stock_list, url):
    import smtplib
    #update these from my_server_info
    gmail_user = ""
    gmail_pwd = "A"
    FROM = "amit@stockcircuit.in"
    TO = ""
    SUBJECT = "Process finished - ", process_name
    TEXT = process_name, updated_stock_list
    TEXT += "\n\nNow run the URL - ", url

    message = """From: %s\nTo: %s\nSubject: %s\n\n%s
    """ % (from_email, ", ".join(to_email), SUBJECT, TEXT)


    try:
        sendMail(message)
        # server = smtplib.SMTP("mail.stockcircuit.in", 587)
        # server.ehlo()
        # server.starttls()
        # server.login(gmail_user, gmail_pwd)
        # server.sendmail(FROM, TO, message)
        # server.close()
        # print ('successfully sent the mail')
    except  Exception as e2:
        print ("error e2 - ", str(e2))
        print ("\n******Amit - failed to send mail, some exception sending email")

def send_email_with_body(title, body):
    import smtplib
    gmail_user = "a"
    gmail_pwd = ""
    FROM = "amit@stockcircuit.in"
    TO = ""
#    SUBJECT = "Process finished - ", title
#    TEXT = process_name, updated_stock_list
#    TEXT += "\n\nNow run the URL - ", url

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

