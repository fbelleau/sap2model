# infoarea_odata_to_neo4j.py
# from francois.belleau@saaq.gouv.qc.ca
# create Infoarea nodes in NEO4J using a CDS VIEW exposed as OData service

from neo4j import GraphDatabase
#pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org neo4j

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

import requests
import pyodata
#pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org pyodata

import datetime
import re  

def add_infoarea(tx, name, label, parent, system, environment, ID, date, user):
    tx.run("MERGE (a:Infoarea {name: $name, label: $label, parent: $parent, system: $system, environment: $environment, ID: $ID , date: $date, user: $user })",
           name=name, label=label, parent=parent, system=system, environment=environment, ID=ID, date=date, user=user)

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

# odata feed connection

#environment = 'S41'
environment = 'SW1'
host = 'TO_BE_DEFINED'
exec(open('./CONFIDENTIAL_'+environment+'.py').read())
		   
SERVICE_URL = 'http://' + host + '/sap/opu/odata/SAAQ/BW_RSDAREA_CDS'
EntityName = 'xSAAQxBW_RSDAREA'
#SERVICE_URL = 'http://' + host + '/sap/opu/odata/sap/Z001_RSDAREA_CDS'
#EntityName = 'Z001_RSDAREA'

print('OData service URL:', SERVICE_URL)

session = requests.Session()
session.auth = requests_auth

odata_feed = pyodata.Client(SERVICE_URL, session)

# number of entries
#print('xSAAQxBW_RSDAREA:', odata_feed.entity_sets.Z001_RSDAREA.get_entities().count().execute())
print('xSAAQxBW_RSDAREA:', odata_feed.entity_sets.xSAAQxBW_RSDAREA.get_entities().count().execute())

#exit()

# list column names
rows = odata_feed.entity_sets.xSAAQxBW_RSDAREA.get_entities().execute()
#rows = odata_feed.entity_sets.Z001_RSDAREA.get_entities().execute()

a = rows[0].__dict__
print('colonnes: ', a['_cache'])

infoarea = a['_cache']['infoarea']
txtlg = a['_cache']['txtlg']
infoarea_p = a['_cache']['infoarea_p']
system = infoarea_system_name(infoarea)
ID = environment + ' ' + system + ' ' + infoarea
date_str = odata_date2string(a['_cache']['timestmp'])
user = a['_cache']['tstpnm']
print(infoarea, txtlg, infoarea_p, system, environment, ID, date_str, user, environment)

#exit()

with driver.session() as session:

	# delete existing node collection
	print('DELETING')
	result = session.run("MATCH n = (p:Infoarea) DETACH DELETE n")

	# create nodes from odata feeed
	print('LOADING')

#	for data in odata_feed.entity_sets.Z001_RSDAREA.get_entities().execute():
	for data in odata_feed.entity_sets.xSAAQxBW_RSDAREA.get_entities().execute():
		a = data.__dict__
		#print(a)
		infoarea = a['_cache']['infoarea']
		txtlg = a['_cache']['txtlg']
		infoarea_p = a['_cache']['infoarea_p']
		#print(infoarea, txtlg, infoarea_p)
		system = infoarea_system_name(infoarea)
		ID = environment + ' ' + system + ' ' + infoarea
		date_str = odata_date2string(a['_cache']['timestmp'])
		user = a['_cache']['tstpnm']
		
		#print(infoarea, objvers, txtlg, infoarea_p)
		session.write_transaction(add_infoarea, infoarea, txtlg, infoarea_p, system, environment, ID, date_str, user)

	#exit()

	# create parent relationship
	print('CREATING RELATION')
	result = session.run("MATCH (e:Infoarea),(p:Infoarea) WHERE e.name = p.parent CREATE (e)-[:Contient]->(p)")
