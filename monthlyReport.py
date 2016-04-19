import csv
from cassandra.cluster import Cluster
cluster = Cluster(contact_points=['192.168.34.231'], protocol_version=3)
session = cluster.connect()
session.set_keyspace('wls')
session.execute('USE wls')
with open("/home/amit_scripts_python/Month_Report_Jan.csv", 'a') as csvfile:
	sitewriter = csv.writer(csvfile, delimiter=',',quotechar=',', quoting=csv.QUOTE_MINIMAL)
	sitewriter.writerow(["Date","ClientId","GeoDimensionId","Siteid","Lineitemid","Click","Impression","InorganicClick","OrganicClick","Spend"])
	for x in range(1,32):
		strs = "select * from advsitewisegeodailyreportv2 where date='2016-03-{0}'".format(x)
		rows=session.execute(strs)
		for a in rows:
			sitewriter.writerow([a.date,a.clientid,a.geodimensionid,a.siteid,a.lineitemid,a.click,a.impr,a.inorgclick,a.orgclick,a.spend])

