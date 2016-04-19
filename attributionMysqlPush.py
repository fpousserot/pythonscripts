import redis
import json
from cassandra.cluster import Cluster
import sys
import MySQLdb
import datetime
cluster = Cluster(contact_points=['192.168.34.234','192.168.34.231','192.168.34.232','192.168.34.233'], protocol_version=3)
database = MySQLdb.connect(host="192.169.33.145",user ="expresso145",passwd="expresso@145",db="til_expresso_db")
cur = database.cursor()
red = redis.Redis(host='192.168.34.235', db=0)
session = cluster.connect()
session.set_keyspace('adlog')
session.execute('USE adlog')
sql = "create table if not exists conversion_count (clientid varchar(10),clientname varchar(75),date varchar(20),successful int(30), failed int(30), primary key(clientid,date))"
cur.execute(sql)
def algoAttributionDetails(year,month,day):
        for hour in range(0,24):
		query = "select * from adtracker where year={0} and month={1} and day={2} and hour={3}".format(year,month,day,hour)
                rows = session.execute(query)
		#print rows
		#print query
                for user_row in rows:
			jobStatus = user_row.jsts
			#print jobStatus
			#print user_row.clientid
                        if jobStatus==1:
                                #print "Success",user_row.clientid,user_row.createtime
                                convertedDate = datetime.datetime.fromtimestamp(user_row.createtime/1000).strftime('%Y-%m-%d 00:00:00')
                                insertionquerysuccessful = """INSERT INTO  conversion_count(clientid,clientname,date,successful,failed) VALUES({0},\"{1}\",\"{2}\",{3},{4}) ON DUPLICATE KEY UPDATE successful = (successful +1)"""
                                #print datainsertion
				key = "CLNTS:{0}".format(user_row.clientid)
				#print key
				if red.exists(key)==1:
					val = json.loads(red.get(key))
					clientname = val['cname']
					datainsertion = insertionquerysuccessful.format(user_row.clientid,clientname,convertedDate,1,0)
					#splitval = val.split(':')
					#clientname = splitval[2].split('}')
					#print clientname[0]
	                                cur.execute(datainsertion)
	                                database.commit()
                        elif jobStatus!=1:
                                #print "Failure",user_row.clientid,user_row.createtime
				key = "CLNTS:{0}".format(user_row.clientid)
				#print key
				if red.exists(key)==1:
					val = json.loads(red.get(key))
					clientname = val['cname']
					convertedDate = datetime.datetime.fromtimestamp(user_row.createtime/1000).strftime('%Y-%m-%d 00:00:00')
	                                insertionqueryfailed = """INSERT INTO  conversion_count(clientid,clientname,date,successful,failed) VALUES({0},\"{1}\",\"{2}\",{3},{4}) ON DUPLICATE KEY UPDATE failed = (failed +1) """.format(user_row.clientid,clientname,convertedDate,0,1)
	                                cur.execute(insertionqueryfailed)
	                                database.commit()
	


algoAttributionDetails(sys.argv[1],sys.argv[2],sys.argv[3])
