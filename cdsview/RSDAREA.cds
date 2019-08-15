@AbapCatalog.sqlViewName: '/SAAQ/RSDAREA'
@AbapCatalog.compiler.compareFilter: true
@AbapCatalog.preserveKey: true
@AccessControl.authorizationCheck: #CHECK
@EndUserText.label: 'BW_RSDAREA'

@OData.publish: true
@VDM.viewType: #CONSUMPTION

define view /SAAQ/BW_RSDAREA as select from rsdarea 
association [0..1] to rsdareat as A on $projection.infoarea = A.infoarea and $projection.objvers = A.objvers and A.langu = 'F'
association [0..1] to rsdareat as B on $projection.infoarea_p = B.infoarea and $projection.objvers = B.objvers and B.langu = 'F'
//left outer join rsdareat on     rsdarea.infoarea = rsdareat.infoarea
//inner join rsdareat on     rsdarea.infoarea = rsdareat.infoarea
//                              and rsdarea.objvers = rsdareat.objvers
//                              and rsdareat.langu = 'F'                                                
{
//rsdarea 
key rsdarea.infoarea, 
key rsdarea.objvers, 
 objstat, 
 contrel, 
 conttimestmp, 
 owner, 
 bwappl, 
 infoarea_p, 
 infoarea_c, 
 infoarea_n, 
 tstpnm, 
 timestmp,

A.txtsh, 
A.txtlg,

B.txtsh as txtsh_p, 
B.txtlg as txtlg_p
}
where objvers = 'A'
