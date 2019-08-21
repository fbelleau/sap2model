# datasource_odata_to_neo4j.py
# from francois.belleau@saaq.gouv.qc.ca
# create Datasource nodes in NEO4J using a CDS VIEW exposed as OData service

from neo4j import GraphDatabase
#pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org neo4j

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

import requests
import pyodata
#pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org pyodata

import datetime
import re  

#session.write_transaction(add_datasource, name, source, label, environment, ID, date_str, user)

def add_datasource(tx, name, source, label, environment, ID, date, user):
    tx.run("MERGE (a:Datasource {name: $name, label: $label, source: $source, environment: $environment, ID: $ID , date: $date, user: $user})",
           name=name, source=source, label=label, environment=environment, ID=ID, date=date, user=user)

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
		   
SERVICE_URL = 'http://' + host + '/sap/opu/odata/SAAQ/BW_RSDS_CDS'
EntityName = 'xSAAQxBW_RSDS'

print('OData service URL:', SERVICE_URL)

session = requests.Session()
session.auth = requests_auth

odata_feed = pyodata.Client(SERVICE_URL, session)

# number of entries
print('xSAAQxBW_RSDS:', odata_feed.entity_sets.xSAAQxBW_RSDS.get_entities().count().execute())

#exit()

# list column names
rows = odata_feed.entity_sets.xSAAQxBW_RSDS.get_entities().execute()
row = rows[0].__dict__['_cache']
print('colonnes: ', row)

name = row['datasource']
label = row['txtlg']
source = row['logsys']
ID = environment + ' ' + source + ' ' + name
date_str = odata_date2string(row['timestmp'])
user = row['tstpnm']

print('Datasource', name, source, label, environment, ID, date_str, user)

#exit()

with driver.session() as session:

	# delete existing node collection
	print('DELETING')
	result = session.run("MATCH n = (p:Datasource) DETACH DELETE n")

	# create nodes from odata feeed
	print('LOADING')

	for data in odata_feed.entity_sets.xSAAQxBW_RSDS.get_entities().execute():
		row = data.__dict__['_cache']
		#print(a)
		name = row['datasource']
		label = row['txtlg']
		source = row['logsys']
		ID = environment + ' ' + source + ' ' + name
		date_str = odata_date2string(row['timestmp'])
		user = row['tstpnm']

		#print('Datasource', name, source, label, environment, ID, date_str, user)
		
		session.write_transaction(add_datasource, name, source, label, environment, ID, date_str, user)

	#exit()

	# create parent relationship
	print('CREATING RELATION')
	result = session.run("MATCH (t:TransObject),(d:Datasource) WHERE t.name = d.name CREATE (d)-[:Source]->(t)")
	
