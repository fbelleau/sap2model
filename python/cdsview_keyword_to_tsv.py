# cdsview_keyword_to_tsv.py
# parse CDS view syntax with RE expression and create a TSV file containaing the matches

import csv
import re

with open('DDDDLSRC.tsv', newline='\n') as input_tsv:
	ligne_tsv = csv.reader(input_tsv, delimiter='\t')
	
	with open('DDDDLSRC_keyword.tsv', "w+") as output_tsv:
		writer_tsv = csv.writer(output_tsv, delimiter='\t', lineterminator='\n')

		header = ['DDDDLSRC', 'LineNumber', 'Keyword', 'Group1', 'Group2', 'Group3', 'Group4', 'Source']

		writer_tsv.writerow(header)
		#print(header)

		for ligne in ligne_tsv:
			#print(ligne)
			
			m = re.match(r"@(.*):(.*)", ligne[2])
			if m:
				resultat=[ligne[0], ligne[1], 'ANNOTATION', m.group(1).replace(' ',''), m.group(2), '', '', ligne[2]]
				#print(resultat)
				writer_tsv.writerow(resultat)
			else:
				m = re.match(r".*association (.*?) to (.*?) as (.*?) on (.*)", ligne[2])
				if m:
					resultat=[ligne[0], ligne[1], 'ASSOCIATION', m.group(1).replace(' ',''), m.group(2).replace(' ',''), m.group(3).replace(' ',''), m.group(4), ligne[2]]
					#print(resultat)
					writer_tsv.writerow(resultat)
				else:
					m = re.match(r".*as select from (.*)", ligne[2])
					if m:
						resultat=[ligne[0], ligne[1], 'SELECT_FROM', m.group(1), '', '', '', ligne[2]]
						#print(resultat)
						writer_tsv.writerow(resultat) 
					else:
						m = re.match(r".*left outer join (.*)", ligne[2])
						if m:
							resultat=[ligne[0], ligne[1], 'LEFT_JOIN', m.group(1), '', '', '', ligne[2]]
							#print(resultat)
							writer_tsv.writerow(resultat)
						else:
							m = re.match(r".*inner join (.*)", ligne[2])
							if m:
								resultat=[ligne[0], ligne[1], 'INNER_JOIN', m.group(1), '', '', '', ligne[2]]
								#print(resultat)
								writer_tsv.writerow(resultat)
							else:
								m = re.match(r"(.*) as (.*)", ligne[2])
								if m:
									resultat=[ligne[0], ligne[1], 'AS', m.group(1).replace(' ',''), m.group(2).replace(' ','').replace(',',''), '', '', ligne[2]]
									#print(resultat)
									writer_tsv.writerow(resultat)
