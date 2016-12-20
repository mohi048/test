import csv
import datetime
import sqlite3
import os
import re

def date_key(row):
	return datetime.datetime.strptime(row[0].strip(), "%Y-%m-%d %H:%M:%S")

def read_data_csv(filename):
	excel_data = []
	for row in csv.reader(open(filename)):
		excel_data.append(row)
	table_headers = excel_data.pop(0) ### removing headers
	excel_data.sort(key=date_key)
	#import pdb; pdb.set_trace()
	return excel_data



def read_data_csv_aggregated(filename):
	excel_data = []
	for row in csv.reader(open(filename)):
		excel_data.append(row)
	table_headers = excel_data.pop(0) ### removing headers
	#excel_data.sort(key=date_key)
	#import pdb; pdb.set_trace()
	return excel_data

def get_last_date_time(data_list):
	count = 0
	for items in data_list:
		my_date_time_coll = items[0].split("__")
		last_date_time = my_date_time_coll[-1]
		data_list[count][0] = last_date_time
		count += 1
	return data_list
	





def create_db_with_dict(data_keys,data_values):
	data_key = data_keys.replace('-','_') ### Remove special charecters
	print "="*20
	print data_key+"    :"+str(len(data_values))+" Records"
	conn = sqlite3.connect(data_key)
	c = conn.cursor()
	c.executescript("DROP TABLE IF EXISTS "+data_key+"; CREATE TABLE "+data_key+" (id INTEGER PRIMARY KEY AUTOINCREMENT,date_time TEXT, applianceName TEXT, tenantName TEXT, localAccCktId INTEGER, localAccCktName TEXT, remoteAccCktId INTEGER, remoteAccCktName TEXT, localSiteId INTEGER, localSiteName TEXT, remoteSiteId INTEGER, remoteSiteName TEXT, fwdClass TEXT, applianceId INTEGER, tenantId INTEGER, delay INTEGER, fwdDelayVar INTEGER, revDelayVar INTEGER, fwdLoss INTEGER, revLoss INTEGER, fwdLossRatio INTEGER, revLossRatio INTEGER, pduLossRatio INTEGER)")
	conn.commit()
	for row1 in data_values:
		row = [w.replace('/', '-') for w in row1]
		c.execute("INSERT INTO "+data_key+" (date_time, applianceName, tenantName, localAccCktId, localAccCktName, remoteAccCktId, remoteAccCktName, localSiteId, localSiteName, remoteSiteId, remoteSiteName, fwdClass, applianceId, tenantId, delay, fwdDelayVar, revDelayVar, fwdLoss, revLoss, fwdLossRatio, revLossRatio, pduLossRatio) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", row)
		conn.commit()
	conn.close()




def get_new_aggr(db_name,time_in_min):
	conn = sqlite3.connect(db_name)
	c = conn.cursor()
	last_id = 0	
	c.execute("select max(id),min(date_time) from "+db_name+";")
	total_datas = c.fetchall()
	total_max_id = total_datas[0][0]
	while last_id != total_max_id:
		c.execute("select min(id), max(id) from "+db_name+" where date_time between (select min(date_time) from "+db_name+" where id > "+str(last_id)+") and (select datetime((select min(date_time) from "+db_name+" where id > "+str(last_id)+"), +'"+str(time_in_min)+" minutes'));")
		id_datas = c.fetchall()
		print id_datas
		last_id = id_datas[0][1]
		#total_max_id = id_datas[0][1]
		print "Ratio     :: "+str(id_datas[0][0])+":"+str(id_datas[0][1])
		#import pdb; pdb.set_trace()
		c.execute("select date_time from "+db_name+" where id >= "+str(id_datas[0][0])+" and id <= "+str(id_datas[0][1])+";")
		timestamp_list = []
		for row in c:
			timestamp_list.append(row)
		c.execute("select avg(delay),avg(fwdDelayVar),avg(revDelayVar),avg(fwdLoss),avg(revLoss),avg(fwdLossRatio),avg(revLossRatio),avg(pduLossRatio) from "+db_name+" where id >= "+str(id_datas[0][0])+" and id <= "+str(id_datas[0][1])+";")
		avg_datas = c.fetchall()
		write_to_csv_data = []
		all_dates = []
		timestamp_list1 = [i[0] for i in timestamp_list]
		coll_dates = '__'.join(str(x) for x in timestamp_list1)
		tm = list(avg_datas[0])
		tm.insert(0,coll_dates)
		write_datas_to_csv('avg_analytics.csv',tm)
		if total_max_id == last_id:
			break
	conn.close()
	cleanup(db_name)




def get_unique_vcpe_local_site(data_list):
	app = []
	#import pdb; pdb.set_trace()
	for a in data_list:
		mm = a[1]
		if mm not in app:
			app.append(mm)
	app = [v for v in app if "controller" not in v ]
	return app

def get_unique_vcpe_remote_site(data_list):
	app = []
	#import pdb; pdb.set_trace()
	for a in data_list:
		mm = a[2]
		if mm not in app:
			app.append(mm)
	app = [v for v in app if "controller" not in v ]
	return app


def get_data_per_cpe_wrt_remote_site(cpe_name,data_list):
	data_by_cpe = []
	for items in data_list:
		#import pdb; pdb.set_trace()
		if cpe_name in items[2]:
			data_by_cpe.append(items)
	# if len(data_by_cpe)%3 != 0:
	# 	print "exception on records"
	return data_by_cpe


def get_data_per_cpe_wrt_local_site(cpe_name,data_list):
	data_by_cpe = []
	for items in data_list:
		#import pdb; pdb.set_trace()
		if cpe_name in items[1]:
			data_by_cpe.append(items)
	# if len(data_by_cpe)%3 != 0:
	# 	print "exception on records"
	return data_by_cpe



def write_headers_to_csv(filename):
	headers = ["date","AVG-delay","AVG-fwdDelayVar","AVG-RevDelayVar","AVG-fwdLoss","AVG-revLoss","AVG-fwdLossRatio","AVG-revLossRatio","AVG-pduLossRatio"]
	mp = open(filename,'a')
	wr1 = csv.writer(mp, dialect='excel')
	wr1.writerow(headers)
	mp.close()



def write_datas_to_csv(filename,datas):
	print datas
	#print dates
	mp = open(filename,'a')
	wr = csv.writer(mp, dialect='excel')
	wr.writerow(datas)
	mp.close()




def get_closest_timestamps(times,times_list):
	closet_times = datetime.datetime.strptime(times.strip(), "%Y-%m-%d %H:%M:%S")
	closest_record = sorted(dates_list, key=lambda M: abs(closet_times - M))[0]
	print closest_record
	return closest_record



def search_in_dict(cpe_type,record):
	for k,v in cpe_type.iteritems:
		datas_list = cpe_type[k]




tm = read_data_csv('analytics-sla-timeseries-metrics.csv')
#print tm
# tc = get_unique_vcpe(tm)
# print tc


vcpe_local_site = get_unique_vcpe_local_site(tm)
vcpe_remote_site = get_unique_vcpe_remote_site(tm)

print vcpe_local_site
print vcpe_remote_site



# vcpe_local_site_dict = {}
# vcpe_remote_site_dict = {}

vcpe_data_dict_local_site = {}
vcpe_data_dict_remote_site = {}

for items in vcpe_local_site:
	li = get_data_per_cpe_wrt_remote_site(items,tm)
	vcpe_data_dict_local_site[items] = li

for items in vcpe_remote_site:
	li = get_data_per_cpe_wrt_local_site(items,tm)
	vcpe_data_dict_remote_site[items] = li


#import pdb; pdb.set_trace()

aggr_list_data = read_data_csv_aggregated('avg_analytics.csv')
#import pdb; pdb.set_trace()

get_last_times = get_last_date_time(aggr_list_data)
print get_last_times


# cpe_data_dict = {}

# for items in tc:
#  	li = get_data_per_cpe(items,tm)
#  	cpe_data_dict[items] = li




# dates_list = []
# for val in cpe_data_dict['Trbleshoot-vcpe1']:
# 	dates_list.append(datetime.datetime.strptime(val[0].strip(), "%Y-%m-%d %H:%M:%S"))


# # closet_time = datetime.datetime(2016, 12, 19, 6, 39)
# # closest_record = sorted(dates_list, key=lambda M: abs(closet_time - M))[0]
# # print closest_record

# nr = get_closest_timestamps('2016-12-19 06:20:00',dates_list)
# #print nr
# for v in cpe_data_dict['Trbleshoot-vcpe1']:
# 	print v[0]
# 	import pdb; pdb.set_trace()
# 	if nr in v[0]:
# 		print "****"
# 	 	print v
# 	else:
# 	 	pass








