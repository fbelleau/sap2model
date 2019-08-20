# cdsview_odata_to_tsv.py
# export CDS view soure code to a TSV file named DDDDLSRC.tsv

import pyodata #from SAP
import requests
import re
import csv

exec(open("./CONFIDENTIAL_S41.py").read())
URL = 'http://' + host + '/sap/opu/odata/sap/ZCDS_DDDDLSRC_CDS'

#exec(open("./CONFIDENTIAL_SW1.py").read())
#URL = 'http://' + host + '/sap/opu/odata/SAAQ/BW_CDS_VIEW_CDS'

session = requests.Session()
session.auth = requests_auth

print(URL)

odata_service = pyodata.Client(URL, session)
print(odata_service)

print(odata_service.entity_sets.ZCDS_DDDDLSRC.get_entities().count().execute())
#print(odata_service.entity_sets.xSAAQxBW_CDS_VIEW.get_entities().count().execute())

#exit()

with open('DDDDLSRC.tsv', "w+", newline='') as output_csv:
	writer = csv.writer(output_csv, delimiter='\t')
		
	writer.writerow(['ddlname','number','source'])
	
	ctr = 0

	
	for row in odata_service.entity_sets.ZCDS_DDDDLSRC.get_entities().execute():
#	for row in odata_service.entity_sets.xSAAQxBW_CDS_VIEW.get_entities().execute():
		row_dict = row.__dict__
		source = row_dict['_cache']['source']
		ddlname = row_dict['_cache']['ddlname']
			
		lineNumber = 0
		ctr = ctr + 1
		
		for line in source.split("\n"):
			lineNumber = lineNumber + 1
			line = line.replace('\t','~').replace('\r','')
			
			row_tab = [ddlname, lineNumber, line]
			try:
				writer.writerow(row_tab)
			except:
				#UTF unknown caracter exception
				print('ERREUR:', ddlname, lineNumber, line)
				
#		if ctr == 100:
#			exit()
