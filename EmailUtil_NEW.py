import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def __init__(self):
    print "Calling GetNSELiveData constructor"

def send_email( subject,updated_stock_list, url):
    # me == my email address
    # you == recipient's email address 
    me = "amit@stockcircuit.in"
    me_pwd = ""
    you = "amitji@gmail.com"

    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = me
    msg['To'] = you

    # html = """\
    # <html>
    #   <head></head>
    #   <body>"""
    # html += updated_stock_list
    # html +="""\
    #   </body>
    # </html>
    # """
    #part1 = MIMEText(updated_stock_list, 'html')
    part1 = MIMEText("<table></table>", 'html')
    msg.attach(part1)

    # SUBJECT = "Process finished - ", process_name
    # TEXT = "Updated List is  - ", updated_stock_list
    # TEXT += "Now run the URL - " , url

    # Prepare actual message
    # message = """From: %s\nTo: %s\nSubject: %s\n\n%s
    # """ % (FROM, ", ".join(TO), SUBJECT, TEXT)
    try:
        #server = smtplib.SMTP("smtp.gmail.com", 587)
        server = smtplib.SMTP("mail.stockcircuit.in", 587)

        server.ehlo()
        server.starttls()
        server.login(me, me_pwd)
        server.sendmail(me, you, msg.as_string())
        server.close()
        print 'successfully sent the mail'
    except  Exception, e2:
        print "error e2 - ", str(e2)
        print "\n******Amit - failed to send mail, some exception sending email"

