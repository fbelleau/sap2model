# cdsview_keyword_to_es.py
# load ElasticSearch index with TSV data

import csv
import re

#pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org ElasticSearch
# https://elasticsearch-py.readthedocs.io/en/master/

from elasticsearch import Elasticsearch

#siren use 9220 instead of 9200 as ES REST port
es = Elasticsearch(['http://localhost:9220'])
print(es)

with open('DDDDLSRC_keyword.tsv', newline='\n') as input_tsv:
	line_tsv = csv.reader(input_tsv, delimiter='\t')
	
	for line in line_tsv:
		#print(ligne)
		if line[2] == 'ANNOTATION DELETED':
			data = {
				'sourceName': line[0],
				'lineNumber': line[1],
				'annotationKeyword': line[3],
				'annotationValue': line[4]
				}
				
			es.index(index='cds_annotation', doc_type='ANNOTATION', body=data)
#			es.index(index='cds_annotation', doc_type='ANNOTATION', body=data, id=line[0]+'-'+line[1])
			#print(data)
			
		if line[2] == 'ASSOCIATION DELETED':
			data = {
				'sourceName': line[0],
				'lineNumber': line[1],
				'type': line[3],
				'to': line[4],
				'as': line[5],
				'on': line[6]
				}
				
			es.index(index='cds_association', doc_type='ASSOCIATION', body=data)
			#print(data)
			
		if line[2] == 'AS':
			data = {
				'sourceName': line[0],
				'lineNumber': line[1],
				'from': line[3],
				'to': line[4]
				}
				
			es.index(index='cds_as', doc_type='AS', body=data)
			#print(data)
		
		if line[2] == 'SELECT_FROM':
			data = {
				'sourceName': line[0],
				'lineNumber': line[1],
				'from': line[3]
				}
				
			es.index(index='cds_select_from', doc_type='SELECT_FROM', body=data)
			#print(data)
