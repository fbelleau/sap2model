# adsot_odata_to_neo4j.py
# from francois.belleau@saaq.gouv.qc.ca
# create ADSO column name nodes in NEO4J using a CDS VIEW exposed as OData service

from neo4j import GraphDatabase
#pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org neo4j

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

import requests
import pyodata
#pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org pyodata

import datetime
import re  

def add_column(tx, environment, adso, name, type, label, ID):
    tx.run("MERGE (a:Column {name: $name, label: $label, type: $type, environment: $environment, ID: $ID, adso: $adso})",
           environment=environment, adso=adso, name=name, type=type, label=label, ID=ID)

def add_element(tx, label):
    tx.run("MERGE (a:Element {label: $label})",
           label=label)

# odata feed connection

environment = 'SW1'
host = 'TO_BE_DEFINED'
exec(open('./CONFIDENTIAL_'+environment+'.py').read())
		   
SERVICE_URL = 'http://' + host + '/sap/opu/odata/SAAQ/BW_RSOADSOT_CDS'
EntityName = 'xSAAQxBW_RSOADSOT'

print('OData service URL:', SERVICE_URL)

session = requests.Session()
session.auth = requests_auth

odata_feed = pyodata.Client(SERVICE_URL, session)

# number of entries
print('xSAAQxBW_RSOADSOT:', odata_feed.entity_sets.xSAAQxBW_RSOADSOT.get_entities().count().execute())

#exit()

# list column names
rows = odata_feed.entity_sets.xSAAQxBW_RSOADSOT.get_entities().execute()
row = rows[0].__dict__['_cache']
print('colonnes: ', row)

adso = row['adsonm']
name = row['colname'].replace('!23!2F!2F!2F','').replace('!2F','/')
type = row['ttyp']
label = row['description']
ID = environment + ' ' + adso + ' ' + type + ' ' + name
	
print('Column', environment, adso, name, type, label, ID)
			
#exit()

with driver.session() as session:

	# delete existing node collection
	print('DELETING')
	result = session.run("MATCH n = (p:Column) DETACH DELETE n")
	result = session.run("MATCH n = (p:Element) DETACH DELETE n")

	# create nodes from odata feeed
	print('LOADING ADSO COLUMN')

	dict_element = {}
	
#	for data in odata_feed.entity_sets.Z001_RSDAREA.get_entities().execute():
	for data in odata_feed.entity_sets.xSAAQxBW_RSOADSOT.get_entities().execute():
		row = data.__dict__['_cache']
		#print(a)
		adso = row['adsonm']
		name = row['colname'].replace('!23!2F!2F!2F','').replace('!2F','/')
		type = row['ttyp']
		label = row['description']
		ID = environment + ' ' + adso + ' ' + type + ' ' + name
			
		#print('Column', environment, adso, name, type, label, ID)

		session.write_transaction(add_column, environment, adso, name, type, label, ID)
		
		dict_element[label] = {'label': label}
	#exit()

	print('LOADING ELEMENT')
	for key in dict_element.keys():
		#print(key)
		session.write_transaction(add_element, key)

	# create parent relationship
	print('CREATING RELATION')
	result = session.run("MATCH (c:Column),(a:ADSO) WHERE a.name = c.adso CREATE (a)-[:Column]->(c)")
	result = session.run("MATCH (c:Column),(e:Element) WHERE c.label = e.label CREATE (e)-[:Synonym]->(c)")
	
