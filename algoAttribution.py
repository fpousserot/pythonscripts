import redis
from cassandra.cluster import Cluster
import sys
from mailer import Mailer
from mailer import Message
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
cluster = Cluster(contact_points=['192.168.34.234','192.168.34.231','192.168.34.232','192.168.34.233'], protocol_version=3)
red=redis.Redis(host='192.168.34.235', db=0)
session = cluster.connect()
session.set_keyspace('adlog')
session.execute('USE adlog')

def algoAttributionDetails(year,month,day):
	me = "amit.singh2@timesinternet.in"
	#to = "amit.singh2@timesinternet.in"
	to = ["asheesh.mahor@timesinternet.in","saurabh.chandolia@timesinternet.in","amit.singh2@timesinternet.in"]
	msg = MIMEMultipart('alternative')
	msg['Subject'] = "Apsalar and Mat Data Of Year= {0} , Month={1}, day={2}".format(year,month,day)
	msg['From'] = me
	msg['To'] = ", ".join(to)
	row_counter = 0
	apsalarSuccessCount = 0
	matSuccessfulCount = 0
	othersuccessfulcount = 0
	apsalarCount = 0
	matCount = 0
	othercount = 0	
	apsalarFailedCount = 0
	matFailedCount = 0
	otherfailedcount = 0
	for hour in range(0,24):
		query = "select * from adtracker where year={0} and month={1} and day={2} and hour={3}".format(year,month,day,hour)
		rows=session.execute(query)
		for user_row in rows:
			row_counter=row_counter+1
			requestMap = user_row.reqmap
			jobStatus = user_row.jsts
			isAttributed = user_row.isattributed
			imprid = user_row.impressionid
			other = True
			for data in requestMap:
				if 'os' in data:
					osDetails = requestMap[data]
					if 'UnKnown, More-Info: Apsalar-Postback' in osDetails:
						other = False
						apsalarCount = apsalarCount + 1
						if jobStatus == 3:
							apsalarSuccessCount = apsalarSuccessCount + 1
						elif jobStatus!=3:
							apsalarFailedCount = apsalarFailedCount + 1
							key = ("JSTS:{0}").format(jobStatus)
							val = red.get(key)
							#print "Apsalar-",user_row
							#print "ImpressionId-",imprid,"\nReason For Failure-",val
					elif 'UnKnown, More-Info: HasOffers Mobile AppTracking v1.0' in osDetails:
						other = False
						matCount = matCount + 1
						if jobStatus == 3:
							matSuccessfulCount = matSuccessfulCount + 1
						elif jobStatus!=3:
							matFailedCount = matFailedCount + 1
							key = ("JSTS:{0}").format(jobStatus)
							val = red.get(key)
							#print"MAT", user_row
							#print "ImpressionId-",imprid,"\nReason For Failure-",val
			if other == True:	
				othercount = othercount + 1
				if jobStatus == 3:
				        othersuccessfulcount = othersuccessfulcount + 1
				elif jobStatus!=3:
					otherfailedcount = otherfailedcount +1
				        key = ("JSTS:{0}").format(jobStatus)
				        val = red.get(key)
	print "Apsalar Data - ",apsalarCount,apsalarSuccessCount,apsalarFailedCount
	print "MAT Data - ",matCount,matSuccessfulCount,matFailedCount
	print "Rest Data - ",othercount,othersuccessfulcount,otherfailedcount
	print "Total Rows in Table -",row_counter
	apsFailPercent = float(apsalarFailedCount)/float(apsalarCount)*100
	matFailPercent = float(matFailedCount)/float(matCount)*100
	restFailPercent = float(otherfailedcount)/float(othercount)*100
	text = "Matrics      Total Count      SuccessFullCount    FailedCount     Difference%"
	text = text + "\nApsalar Data - {0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(apsalarCount,apsalarSuccessCount,apsalarFailedCount,apsFailPercent)
	text = text + "\nMat Data      - {0}\t\t  {1}\t\t\t  {2}\t\t\t{3}".format(matCount,matSuccessfulCount,matFailedCount,matFailPercent)
	text = text + "\nRest Data     - {0}\t\t  {1}\t\t\t  {2}\t\t\t{3}".format(othercount,othersuccessfulcount,otherfailedcount,restFailPercent)
	part1 = MIMEText(text, 'plain')
	msg.attach(part1)
	s = smtplib.SMTP('192.168.24.21')
	s.sendmail(me,to, msg.as_string())


algoAttributionDetails(sys.argv[1],sys.argv[2],sys.argv[3])


