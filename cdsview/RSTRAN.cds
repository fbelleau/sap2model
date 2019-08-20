//RSTRAN.cds

@AbapCatalog.sqlViewName: '/SAAQ/RSTRAN'
@AbapCatalog.compiler.compareFilter: true
@AbapCatalog.preserveKey: true
@AccessControl.authorizationCheck: #CHECK
@EndUserText.label: 'BW_RSTRAN'

@OData.publish: true
@VDM.viewType: #CONSUMPTION

define view /SAAQ/BW_RSTRAN as select from rstran 
association [0..1] to rstrant on $projection.tranid = rstrant.tranid and $projection.objvers = rstrant.objvers and rstrant.langu = 'F'
{
//rstran 
key rstran.tranid, 
key rstran.objvers, 
objstat, 
contrel, 
conttimestmp, 
owner, 
bwappl, 
activfl, 
tstpnm, 
timestmp, 
sourcetype, 
sourcesubtype, 
sourcename, 
targettype, 
targetsubtype, 
targetname, 
created_at, 
created_by,
//rstrant
rstrant.txtlg
}
where objvers = 'A'
