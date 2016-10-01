def __init__(self):
    print "Calling GetNSELiveData constructor"

def send_email( process_name,updated_stock_list, url):
    import smtplib

    gmail_user = "amit@stockcircuit.in"
    gmail_pwd = "amit1973"
    FROM = "amit@stockcircuit.in"
    TO = "amitji@gmail.com"
    SUBJECT = "Process finished - ", process_name
    TEXT = "Updated List is  - ", updated_stock_list
    TEXT += "Now run the URL - " , url

    # Prepare actual message
    message = """From: %s\nTo: %s\nSubject: %s\n\n%s
    """ % (FROM, ", ".join(TO), SUBJECT, TEXT)
    try:
        #server = smtplib.SMTP("smtp.gmail.com", 465)
        server = smtplib.SMTP("mail.stockcircuit.in", 25)

        server.ehlo()
        server.starttls()
        server.login(gmail_user, gmail_pwd)
        server.sendmail(FROM, TO, message)
        server.close()
        print 'successfully sent the mail'
    except  Exception, e2:
        print "error e2 - ", str(e2)
        print "\n******Amit - failed to send mail, some exception sending email"

