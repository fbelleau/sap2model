//RSOADSO.cds
@AbapCatalog.sqlViewName: '/SAAQ/RSOADSO'
@AbapCatalog.compiler.compareFilter: true
@AbapCatalog.preserveKey: true
@AccessControl.authorizationCheck: #CHECK
@EndUserText.label: 'BW_RSOADSO'

@OData.publish: true
@VDM.viewType: #CONSUMPTION

define view /SAAQ/BW_RSOADSO as select from rsoadso 
association [0..1] to rsoadsot on $projection.adsonm = rsoadsot.adsonm and $projection.objvers = rsoadsot.objvers and rsoadsot.langu = 'F' and rsoadsot.ttyp = 'EUSR'
{
//rsoadso 
key rsoadso.adsonm, 
key rsoadso.objvers, 
contrel, 
conttimestmp, 
owner, 
bwappl, 
infoarea, 
tstpnm, 
timestmp, 
crnm, 
crtstp, 
//xml_ui, 
activate_data, 
write_changelog, 
cubedeltaonly, 
no_aq_deletion, 
unique_records, 
planning_mode, 
check_delta_cons, 
extended_aq_table, 
ncumtim, 
all_sids_checked, 
all_sids_materialized, 
hanamodelfl, 
direct_update, 
snapshot_scenario, 
dyn_tiering_per_part, 
temperature_schema, 
is_reporting_obj, 
force_no_concat, 
compatibility_views, 
autorefresh, 
uom_iobjnm, 
group_name, 
group_fieldnm,
//
rsoadsot.description
}
where objvers = 'A'
