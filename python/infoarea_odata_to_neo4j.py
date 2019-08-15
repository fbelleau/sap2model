# infoarea_odata_to_neo4j.py
# from francois.belleau@saaq.gouv.qc.ca
# create Infoarea nodes in NEO4J using a CDS VIEW exposed as OData service

# CDS view definition of odata feed
'''
@AbapCatalog.sqlViewName: '/SAAQ/RSDAREA'
@AbapCatalog.compiler.compareFilter: true
@AbapCatalog.preserveKey: true
@AccessControl.authorizationCheck: #CHECK
@EndUserText.label: 'BW_RSDAREA'

@OData.publish: true
@VDM.viewType: #CONSUMPTION

define view /SAAQ/BW_RSDAREA as select from rsdarea 
association [0..1] to rsdareat as A on $projection.infoarea = A.infoarea and $projection.objvers = A.objvers and A.langu = 'F'
association [0..1] to rsdareat as B on $projection.infoarea_p = B.infoarea and $projection.objvers = B.objvers and B.langu = 'F'
//left outer join rsdareat on     rsdarea.infoarea = rsdareat.infoarea
//inner join rsdareat on     rsdarea.infoarea = rsdareat.infoarea
//                              and rsdarea.objvers = rsdareat.objvers
//                              and rsdareat.langu = 'F'                                                
{
//rsdarea 
key rsdarea.infoarea, 
key rsdarea.objvers, 
 objstat, 
 contrel, 
 conttimestmp, 
 owner, 
 bwappl, 
 infoarea_p, 
 infoarea_c, 
 infoarea_n, 
 tstpnm, 
 timestmp,

A.txtsh, 
A.txtlg,

B.txtsh as txtsh_p, 
B.txtlg as txtlg_p
}
where objvers = 'A'
'''

from neo4j import GraphDatabase
#pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org neo4j

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

import requests
import pyodata
#pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org pyodata

import datetime
import re  

environment = 'SW1'
host = 'TO_BE_DEFINED'
exec(open('./CONFIDENTIAL_'+environment+'.py').read())


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
		   
SERVICE_URL = 'http://' + host + '/sap/opu/odata/SAAQ/BW_RSDAREA_CDS'

print('OData service URL:', SERVICE_URL)

session = requests.Session()
session.auth = requests_auth

BW_RSDAREA_CDS = pyodata.Client(SERVICE_URL, session)

# number of entries
print('BW_RSDAREA_CDS:', BW_RSDAREA_CDS.entity_sets.xSAAQxBW_RSDAREA.get_entities().count().execute())

#exit()

# list column names
rows = BW_RSDAREA_CDS.entity_sets.xSAAQxBW_RSDAREA.get_entities().execute()
a = rows[0].__dict__
print('colonnes: ', a.get('_cache'))
#convert date
print('conttimestmp:', odata_date2string(a.get('_cache').get('conttimestmp')))

#exit()

with driver.session() as session:

	# delete existing node collection
	result = session.run("MATCH n = (p:Infoarea) "
						"DETACH DELETE n")
	#print(result)

	# create nodes from odata feeed
	for data in BW_RSDAREA_CDS.entity_sets.xSAAQxBW_RSDAREA.get_entities().execute():
		a = data.__dict__
		#print(a)
		infoarea = a.get('_cache').get('infoarea')
		txtlg = a.get('_cache').get('txtlg')
		infoarea_p = a.get('_cache').get('infoarea_p')
		#print(infoarea, txtlg, infoarea_p)
		system = infoarea_system_name(infoarea)
		ID = environment + ' ' + system + ' ' + infoarea
		date_str = odata_date2string(a.get('_cache').get('timestmp'))
		user = a.get('_cache').get('tstpnm')
		
		#print(infoarea, objvers, txtlg, infoarea_p)
		session.write_transaction(add_infoarea, infoarea, txtlg, infoarea_p, system, environment, ID, date_str, user)

	#exit()

	# create parent relationship
	result = session.run("MATCH (e:Infoarea),(p:Infoarea) "
						"WHERE e.name = p.parent "
						"CREATE (e)-[:Infoarea_contient]->(p)")
	#print(result)
