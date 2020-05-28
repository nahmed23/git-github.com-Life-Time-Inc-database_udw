CREATE PROC [dbo].[proc_update_hub_delete_flag_golden_record_customer] @current_dv_batch_id [bigint] AS

BEGIN

SET XACT_ABORT ON
SET NOCOUNT ON



if object_id('tempdb..#LinkUnlink_recs') is not null drop table #LinkUnlink_recs
create table dbo.#LinkUnlink_recs with(distribution=round_robin, location=user_db) as
select hub.h_mdm_golden_record_customer_id as hub_id
  FROM h_mdm_golden_record_customer as hub
  INNER JOIN stage_mdm_goldenrecordcustomerlinkage as stage
    ON hub.[entity_id]=stage.[PreviousEntityID]
   AND hub.[source_id]=stage.[SourceID]
   AND hub.[source_code]=stage.[SourceCode]
 where hub.dv_deleted=0
   and stage.PreviousEntityID <> 0
   and stage.dv_batch_id >= @current_dv_batch_id


--get the lastest combination of entity_id, source_id and source_code amoung the set of records
if object_id('tempdb..#latestrecords') is not null drop table #latestrecords
create table dbo.#latestrecords with(distribution=round_robin, location=user_db) as
select entityid,
       sourceid,
	   sourcecode,
	   max(loaddatetime) loaddatetime
  from stage_mdm_goldenrecordcustomer
 where dv_batch_id >= @current_dv_batch_id
 group by entityid, sourceid, sourcecode, loaddatetime


if object_id('tempdb..#deleteoldrecords') is not null drop table #deleteoldrecords
create table dbo.#deleteoldrecords with(distribution=round_robin, location=user_db) as
select hub.h_mdm_golden_record_customer_id as hub_id
  from h_mdm_golden_record_customer as hub
  join #latestrecords latestrecords
    ON hub.entity_id = latestrecords.EntityID
   AND hub.source_id = latestrecords.SourceID
   AND hub.source_code = latestrecords.SourceCode
 where hub.dv_deleted=0
   and hub.load_date_time < latestrecords.loaddatetime


--if object_id('tempdb..#pittableupdates') is not null drop table #pittableupdates
--create table dbo.#pittableupdates with(distribution=round_robin, location=user_db, heap) as
--select pit.p_mdm_golden_record_customer_id as pit_id,
--       stage.loaddatetime
--  FROM p_mdm_golden_record_customer as pit
--  INNER JOIN stage_mdm_goldenrecordcustomerlinkage as stage
--    ON pit.[entity_id]=stage.[PreviousEntityID]
--   AND pit.[source_id]=stage.[SourceID]
--   AND pit.[source_code]=stage.[SourceCode]
-- where pit.dv_load_date_time < stage.loaddatetime
--   and stage.dv_batch_id = @current_dv_batch_id
--   and pit.dv_load_end_date_time = '9999-12-31'


/* As ANSI join is not supported, have made use of implicit join to perform update*/
declare @user varchar(500) = suser_sname()
UPDATE h_mdm_golden_record_customer 
SET  h_mdm_golden_record_customer.[dv_deleted] = 1,
     dv_updated_date_time = convert(datetime,getdate(),120),
     dv_update_user = @user
where h_mdm_golden_record_customer_id in (select hub_id from #LinkUnlink_recs union select hub_id from #deleteoldrecords)

--UPDATE p_mdm_golden_record_customer 
--   SET p_mdm_golden_record_customer.dv_load_end_date_time = pittableupdates.loaddatetime
--  FROM #pittableupdates pittableupdates
-- WHERE p_mdm_golden_record_customer.p_mdm_golden_record_customer_id = pittableupdates.pit_id

drop table #LinkUnlink_recs
drop table #deleteoldrecords
--drop table #pittableupdates

END  
