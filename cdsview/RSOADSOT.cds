//RSOADSOT.cds
@AbapCatalog.sqlViewName: '/SAAQ/RSOADSOT'
@AbapCatalog.compiler.compareFilter: true
@AbapCatalog.preserveKey: true
@AccessControl.authorizationCheck: #CHECK
@EndUserText.label: 'BW_RSOADSOT'

@OData.publish: true
@VDM.viewType: #CONSUMPTION

define view /SAAQ/BW_RSOADSOT as select from rsoadsot 
{
//rsoadsot 
key langu, 
key adsonm, 
key objvers, 
key ttyp, 
colname, 
description, 
quick_info
}
where objvers = 'A' and rsoadsot.langu = 'F'

