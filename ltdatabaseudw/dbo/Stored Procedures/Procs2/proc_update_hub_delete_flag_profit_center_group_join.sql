CREATE PROC [dbo].[proc_update_hub_delete_flag_profit_center_group_join] AS

BEGIN

SET XACT_ABORT ON
SET NOCOUNT ON

/*
As we are doing full load for profit_center_group_join which has limited records, once loading is completed in data vault table,
dv_deleted flag should be updated to 1 if corresponding IDs are missing in stage table
This temp table stores the result for left outer join for hub and stage table to get list of null records in stage table which does not have
corresponding IDs in hub table
Updated_recs will hold the records for those whoes dv_deleted flag was set to 1 earlier but same record once again coming for
source. dv_deleted flags for those records reset to 0 once again
*/

if object_id('tempdb..#Updated_recs') is not null drop table #Updated_recs
create table dbo.#Updated_recs with(distribution=round_robin, location=user_db, heap) as
select hub.h_ig_it_cfg_profit_center_group_join_id as hub_id
  FROM h_ig_it_cfg_profit_center_group_join as hub
  INNER JOIN stage_ig_it_cfg_Profit_Center_Group_Join as stage
    ON hub.[ent_id]=stage.[ent_id]
   AND hub.[profit_ctr_grp_id]=stage.[profit_ctr_grp_id]
   AND hub.[profit_center_id]=stage.[profit_center_id]
 where hub.dv_deleted=1

/* As ANSI join is not supported, have made use of implicit join to perform update*/
UPDATE h_ig_it_cfg_profit_center_group_join 
SET  h_ig_it_cfg_profit_center_group_join.[dv_deleted] = 0
where h_ig_it_cfg_profit_center_group_join_id in (select hub_id from #Updated_recs)

drop table #Updated_recs

if object_id('tempdb..#deleted_recs') is not null drop table #deleted_recs
create table dbo.#deleted_recs with(distribution=round_robin, location=user_db, heap) as
select hub.h_ig_it_cfg_profit_center_group_join_id as hub_id
  FROM h_ig_it_cfg_profit_center_group_join as hub
  LEFT OUTER JOIN stage_ig_it_cfg_Profit_Center_Group_Join as stage
    ON hub.[ent_id]=stage.[ent_id]
   AND hub.[profit_ctr_grp_id]=stage.[profit_ctr_grp_id]
   AND hub.[profit_center_id]=stage.[profit_center_id]
 where stage.stage_ig_it_cfg_Profit_Center_Group_Join_id is null

/* As ANSI join is not supported, have made use of implicit join to perform update*/
UPDATE h_ig_it_cfg_profit_center_group_join 
SET  h_ig_it_cfg_profit_center_group_join.[dv_deleted] = 1
where h_ig_it_cfg_profit_center_group_join_id in (select hub_id from #deleted_recs)

drop table #deleted_recs

END 