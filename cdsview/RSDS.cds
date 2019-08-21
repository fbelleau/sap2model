//RSDS.cds

@AbapCatalog.sqlViewName: '/SAAQ/RSDS'
@AbapCatalog.compiler.compareFilter: true
@AbapCatalog.preserveKey: true
@AccessControl.authorizationCheck: #CHECK
@EndUserText.label: 'BW_RSDS'

@OData.publish: true
@VDM.viewType: #CONSUMPTION

define view /SAAQ/BW_RSDS as select from rsds 
association [0..1] to rsdst on $projection.datasource = rsdst.datasource and $projection.objvers = rsdst.objvers and rsdst.langu = 'F'
{
//RSDS 
key rsds.datasource, 
key rsds.logsys, 
key rsds.objvers, 
objstat, 
activfl, 
type, 
primsegid, 
objectfd, 
applnm, 
basosource, 
delta, 
stockupd, 
packgupd, 
realtime, 
timdepfl, 
langudepfl, 
exstructure, 
virtcube, 
hybridaccess, 
charonly, 
tstmpoltp, 
deltaact, 
icon, 
contrel, 
conttimestmp, 
appl_callback, 
tfmethods, 
archmethod, 
duprec, 
initsimu, 
zdd_able, 
reconciliation, 
char_psa, 
char1000, 
dezichar, 
contpacket, 
contmaster, 
deltafd, 
deltatp, 
deltalow, 
deltahigh, 
deltatz, 
tstpnm, 
timestmp, 
codeid, 
transtrus_appl, 
transtrus_char, 
hashnumber, 
contsrctype, 
contsrcvers, 
psadatcls, 
psasizcat, 
convlangu, 
exttabl,
// rsdst
rsdst.txtsh,
rsdst.txtmd,
rsdst.txtlg
}
where rsds.objvers = 'A'
