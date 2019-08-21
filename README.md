# sap2model
Project to create a data dictionary for SAP BW development to be explored with CYPHER graph query language

Run the python scripts yo load NEO4J in this order :

* python infoarea_odata_to_neo4j.py
* python adso_odata_to_neo4j.py
* python adsot_odata_to_neo4j.py
* python trans_odata_to_neo4j.py
* python transfield_odata_to_neo4j.py
* python datasource_odata_to_neo4j.py

Then try examples queries from the Wiki.

https://github.com/fbelleau/sap2model/wiki

To load CDS view into ElasticSearch use those scripts :

* python cdsview_odata_to_tsv.py
* python cdsview_keyword_to_tsv.py
* python cdsview_keyword_to_es.py
