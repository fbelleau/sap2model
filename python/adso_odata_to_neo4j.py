# adso_odata_to_neo4j.py
# from francois.belleau@saaq.gouv.qc.ca
# create ADSO nodes in NEO4J using a CDS VIEW exposed as OData service

from neo4j import GraphDatabase
#pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org neo4j

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

import requests
import pyodata
#pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org pyodata

import datetime
import re  

def add_adso(tx, name, subtype, infoarea, label, system, environment, ID, date, user, caracteristics):
    tx.run("MERGE (a:ADSO {name: $name, label: $label, subtype: $subtype, system: $system, environment: $environment, ID: $ID , date: $date, user: $user, infoarea: $infoarea, caracteristics: $caracteristics})",
           name=name, label=label, subtype=subtype, system=system, environment=environment, ID=ID, date=date, user=user, infoarea=infoarea, caracteristics=caracteristics)

# extract system name from infoarea name
def  infoarea_system_name(system):
	if system[0:4] == '/IMO':
		system = 'CO'
	elif system[0:6] == 'ZSAAQ_':
		system = system[6:8]
	else:
		system = 'NULL'
	return(system)

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

# compute ADSO type
def AdsoType(AdsoName):
	if AdsoName[0:3] == 'ZCM':
		type = 'Corporate memory'
	elif AdsoName[0:3] == 'ZD_':
		type = 'Core layer'
	elif AdsoName[0:7] == '/IMO/CM':
		type = 'Corporate memory'
	elif AdsoName[0:7] == '/IMO/D_':
		type = 'Core layer'
	else :
		type = ''
	return(type)
		
# odata feed connection

environment = 'SW1'
host = 'TO_BE_DEFINED'
exec(open('./CONFIDENTIAL_'+environment+'.py').read())
		   
SERVICE_URL = 'http://' + host + '/sap/opu/odata/SAAQ/BW_RSOADSO_CDS'
EntityName = 'xSAAQxBW_RSOADSO'

print('OData service URL:', SERVICE_URL)

session = requests.Session()
session.auth = requests_auth

odata_feed = pyodata.Client(SERVICE_URL, session)

# number of entries
print('xSAAQxBW_RSOADSO:', odata_feed.entity_sets.xSAAQxBW_RSOADSO.get_entities().count().execute())

#exit()

# list column names
rows = odata_feed.entity_sets.xSAAQxBW_RSOADSO.get_entities().execute()
row = rows[0].__dict__['_cache']
print('colonnes: ', row)

adso = row['adsonm']
subtype = AdsoType(adso)
infoarea = row['infoarea']
name = row['description']
system = infoarea_system_name(infoarea)
ID = environment + ' ' + system + ' ' + adso
date_str = odata_date2string(row['timestmp'])
user = row['tstpnm']

caracteristics = []
for key in ['activate_data', 'write_changelog', 'cubedeltaonly', 'no_aq_deletion', 'unique_records', 'planning_mode', 'check_delta_cons', 'extended_aq_table', 'all_sids_checked', 'all_sids_materialized', 'direct_update', 'snapshot_scenario', 'dyn_tiering_per_part', 'is_reporting_obj', 'force_no_concat', 'compatibility_views', 'autorefresh']:
	#print(key)
	if row[key]:
		caracteristics.append(key)
		
print('ADSO', adso, name, subtype, infoarea, environment, system, ID, date_str, user, caracteristics)
			
#exit()

with driver.session() as session:

	# delete existing node collection
	print('DELETING')
	result = session.run("MATCH n = (p:ADSO) DETACH DELETE n")

	# create nodes from odata feeed
	print('LOADING')

#	for data in odata_feed.entity_sets.Z001_RSDAREA.get_entities().execute():
	for data in odata_feed.entity_sets.xSAAQxBW_RSOADSO.get_entities().execute():
		row = data.__dict__['_cache']
		#print(a)
		name = row['adsonm']
		subtype = AdsoType(name)
		infoarea = row['infoarea']
		label = row['description']
		system = infoarea_system_name(infoarea)
		ID = environment + ' ' + system + ' ' + name + ' ' + subtype 
		date = odata_date2string(row['timestmp'])
		user = row['tstpnm']

		#create caracteristics list to replace boolean
		caracteristics = []
		for key in ['activate_data', 'write_changelog', 'cubedeltaonly', 'no_aq_deletion', 'unique_records', 'planning_mode', 'check_delta_cons', 'extended_aq_table', 'all_sids_checked', 'all_sids_materialized', 'direct_update', 'snapshot_scenario', 'dyn_tiering_per_part', 'is_reporting_obj', 'force_no_concat', 'compatibility_views', 'autorefresh']:
			if row[key]:
				caracteristics.append(key)
				
		#print('ADSO', adso, name, subtype, infoarea, environment, label, system, ID, date, user, caracteristics)
		
		session.write_transaction(add_adso, name, subtype, infoarea, label, system, environment, ID, date, user, caracteristics)

	#exit()

	# create parent relationship
	print('CREATING RELATION')
	result = session.run("MATCH (i:Infoarea),(a:ADSO) WHERE i.name = a.infoarea CREATE (i)-[:contient]->(a)")
