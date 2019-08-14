# odata_cdsview2tsvimport

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

'''


import pyodata #from SAP
import requests
import re
import csv

URL = 'http://pgissw101a.casa.intra.saaq.net:8041/sap/opu/odata/SAAQ/BW_RSDAREA_CDS'
exec(open("./sw1_mp.py").read())

print(URL)

session = requests.Session()
session.auth = requests_auth

odata_service = pyodata.Client(URL, session)
a = odata_service

print(odata_service.entity_sets.xSAAQxBW_RSDAREA.get_entities().count().execute())

row_tab = []

with open('INFOAREA.tsv', "w+", newline='') as output_csv:
	writer = csv.writer(output_csv, delimiter='\t')
		
	ctr = 0
	
	for entity in odata_service.entity_sets.xSAAQxBW_RSDAREA.get_entities().execute():
		ctr = ctr + 1

		#print(entity.txtsh)
		entity_dict = entity.__dict__
		#print(entity_dict['_cache'])
		data = entity_dict['_cache']

		# print header as a first line
		if ctr == 1:
			#add calculated value to header
			header = ['environnement','system']
			for key in data.keys():
				header.append(key)
				#print(key)
			print(header)
			writer.writerow(header)
		
		#export each data values on a line
		#exit()
		values = []
		
		#add calculated value
		
		#compute SAAQ system name
		#compute SAAQ systeme name
		values.append('SW1')

		system = data['infoarea']
		if system[0:6] == 'ZSAAQ_':
			system = system[6:8]
		else:
			system = 'ND'
		values.append(system)

		for value in data.values():
			values.append(value)
			#print(value)
		
		#export values line to file		
		writer.writerow(values)
