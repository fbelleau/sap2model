//RSTRANFIEL.cds

@AbapCatalog.sqlViewName: '/SAAQ/RSTRANFIEL'
@AbapCatalog.compiler.compareFilter: true
@AbapCatalog.preserveKey: true
@AccessControl.authorizationCheck: #CHECK
@EndUserText.label: 'BW_RSTRANFIELD'

@OData.publish: true
@VDM.viewType: #CONSUMPTION

define view /SAAQ/BW_RSTRANFIELD as select from rstranfield {
//RSTRANFIELD 
key tranid, 
key objvers, 
key segid, 
key ruleid, 
key stepid, 
key paramtype, 
key paramnm, 
fieldnm, 
fieldtype, 
keyflag, 
ruleposit, 
aggr    
}
where objvers = 'A'
