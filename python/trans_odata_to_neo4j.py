# trans_odata_to_neo4j.py
# from francois.belleau@saaq.gouv.qc.ca
# create Transformation nodes in NEO4J using a CDS VIEW exposed as OData service

from neo4j import GraphDatabase
#pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org neo4j

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

import requests
import pyodata
#pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org pyodata

import datetime
import re  

def add_transformation(tx, environment, name, subtype, label, ID, date, user, sourcename, targetname):
    tx.run("MERGE (a:Transformation {environment: $environment, name: $name, subtype: $subtype, label: $label, ID: $ID , date: $date, user: $user, sourcename: $sourcename, targetname: $targetname})",
           environment=environment, name=name, subtype=subtype, label=label, ID=ID, date=date, user=user, sourcename=sourcename, targetname=targetname)

def add_transformation_object(tx, env, name, type, subtype):
    tx.run("MERGE (a:TransObject {environment: $environment, name: $name, type: $type, subtype: $subtype})",
           environment=environment, name=name, type=type, subtype=subtype)


# convert sap odata date format to a string date format
def odata_date2string(conttimestmp):
	try:
		matches = re.match(r"^/Date\((.*)\+0000\)/$", conttimestmp)
		value = matches.group(1)
		#print(value, int(value))
		value = datetime.datetime(1970, 1, 1) + datetime.timedelta(milliseconds=int(value))
		#print(type(value))
		result = value.strftime('%Y-%m-%d %H:%M:%S')
	except:
		result = ''
	return(result)
		
# odata feed connection

environment = 'SW1'
host = 'TO_BE_DEFINED'
exec(open('./CONFIDENTIAL_'+environment+'.py').read())
		   
#http://pgissw101a.casa.intra.saaq.net:8041/sap/opu/odata/SAAQ/BW_RSTRAN_CDS/?$format=xml		   
SERVICE_URL = 'http://' + host + '/sap/opu/odata/SAAQ/BW_RSTRAN_CDS'
EntityName = 'xSAAQxBW_RSTRAN'

print('OData service URL:', SERVICE_URL)

session = requests.Session()
session.auth = requests_auth

odata_feed = pyodata.Client(SERVICE_URL, session)

# number of entries
print('xSAAQxBW_RSTRAN:', odata_feed.entity_sets.xSAAQxBW_RSTRAN.get_entities().count().execute())

#exit()

# list column names
rows = odata_feed.entity_sets.xSAAQxBW_RSTRAN.get_entities().execute()
row = rows[0].__dict__['_cache']
print('colonnes: ', row)

name = row['tranid']
subtype = row['sourcetype'] + '->' +row['targettype']
label = row['txtlg']
ID = environment + ' ' + name + ' ' + subtype
date = odata_date2string(row['timestmp'])
user = row['tstpnm']

sourcename = row['sourcename']
sourcetype = row['sourcetype']
sourcesubtype = row['sourcesubtype']

targetname = row['targetname']
targettype = row['targettype']
targetsubtype = row['targetsubtype']

print('Transformation',environment, name, subtype, ID, date, user, sourcename,sourcetype,sourcesubtype,targetname,targettype,targetsubtype)
			
#exit()

#create de dictionnary of source target object
dictionnary_object={}

with driver.session() as session:

	# delete existing node collection
	print('DELETING')
	result = session.run("MATCH n = (p:Transformation) DETACH DELETE n")
	result = session.run("MATCH n = (p:TransObject) DETACH DELETE n")

	# create nodes from odata feeed
	print('LOADING TRANSFORMATION')
	for data in odata_feed.entity_sets.xSAAQxBW_RSTRAN.get_entities().execute():
		row = data.__dict__['_cache']
		#print(a)

		name = row['tranid']
		subtype = row['sourcetype'] + '->' +row['targettype']
		label = row['txtlg']
		ID = environment + ' ' + name + ' ' + subtype
		date = odata_date2string(row['timestmp'])
		user = row['tstpnm']

		sourcename = row['sourcename']
		sourcetype = row['sourcetype']
		sourcesubtype = row['sourcesubtype']

		targetname = row['targetname']
		targettype = row['targettype']
		targetsubtype = row['targetsubtype']
		
		dictionnary_object[sourcename]= {
										'name' : sourcename,
										'type' : sourcetype,
										'subtype' : sourcesubtype
										}
		dictionnary_object[targetname]= {
										'name' : targetname,
										'type' : targettype,
										'subtype' : targetsubtype
										}

		#print("add_transformation", environment, name, subtype, label, ID, date, user, sourcename, targetname)
		
		session.write_transaction(add_transformation, environment, name, subtype, label, ID, date, user, sourcename, targetname)

	#exit()

	# Creates node except for ADSO
	print('LOADING TRANSFORMATION_OBJECT')
	for item in dictionnary_object.values():
		#print(item)
		type = item['type']

		if type != 'ADSO':
			name = item['name']
			subtype = item['subtype']
			session.write_transaction(add_transformation_object, environment, name, type, subtype)
		
	# create parent relationship
	print('CREATING RELATIONS')
	result = session.run("MATCH (t:Transformation),(o:TransObject) WHERE t.sourcename = o.name CREATE (o)-[:Source]->(t)")
	result = session.run("MATCH (t:Transformation),(o:TransObject) WHERE t.targetname = o.name CREATE (t)-[:Target]->(o)")

	result = session.run("MATCH (t:Transformation),(o:ADSO) WHERE t.sourcename = o.name CREATE (o)-[:Source]->(t)")
	result = session.run("MATCH (t:Transformation),(o:ADSO) WHERE t.targetname = o.name CREATE (t)-[:Target]->(o)")
