@AbapCatalog.sqlViewName: '/SAAQ/CDS_VIEW'
@AbapCatalog.compiler.compareFilter: true
@AbapCatalog.preserveKey: true
@AccessControl.authorizationCheck: #CHECK
@EndUserText.label: 'BW_CDS_VIEW'

@OData.publish: true
@VDM.viewType: #CONSUMPTION

define view /SAAQ/BW_CDS_VIEW as select from ddddlsrc {
//ddddlsrc 
key ddlname, 
as4local, 
as4user, 
as4date, 
as4time, 
source, 
parentname, 
actflag, 
chgflag    
}
