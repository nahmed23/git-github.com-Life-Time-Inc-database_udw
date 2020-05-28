CREATE PROC [dbo].[proc_etl_mms_val_membership_type_group] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

--join up incremental and current
if object_id('tempdb..#process') is not null drop table #process
create table dbo.#process with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select row_number() over (order by s.bk_hash) rownum,
       s.bk_hash,
       s.val_membership_type_group_id,
       s.description,
       s.sort_order,
       s.inserted_date_time,
       s.updated_date_time,
       s.val_card_level_id,
       case when r.r_mms_val_membership_type_group_id is null then s.dv_load_date_time
            else @job_start_date_time end dv_load_date_time,
       convert(datetime,'Dec 31, 9999',120) dv_load_end_date_time,
       @current_dv_batch_id dv_batch_id,
       2 dv_r_load_source_id,
       s.source_hash dv_hash,
       r.r_mms_val_membership_type_group_id
  from (select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ValMembershipTypeGroupID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
               isnull(InsertedDateTime, convert(datetime,'jan 1, 1980',120)) dv_load_date_time,
               stage_mms_ValMembershipTypeGroup.ValMembershipTypeGroupID val_membership_type_group_id,
               stage_mms_ValMembershipTypeGroup.Description description,
               stage_mms_ValMembershipTypeGroup.SortOrder sort_order,
               stage_mms_ValMembershipTypeGroup.InsertedDateTime inserted_date_time,
               stage_mms_ValMembershipTypeGroup.UpdatedDateTime updated_date_time,
               stage_mms_ValMembershipTypeGroup.ValCardLevelID val_card_level_id,
               convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_mms_ValMembershipTypeGroup.ValMembershipTypeGroupID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_mms_ValMembershipTypeGroup.Description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_mms_ValMembershipTypeGroup.SortOrder as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_mms_ValMembershipTypeGroup.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_mms_ValMembershipTypeGroup.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_mms_ValMembershipTypeGroup.ValCardLevelID as varchar(500)),'z#@$k%&P'))),2) source_hash,
               dv_batch_id
          from dbo.stage_mms_ValMembershipTypeGroup
         where (ValMembershipTypeGroupID is not null)) s 
  left join r_mms_val_membership_type_group r
    on s.bk_hash = r.bk_hash
   and r.dv_load_end_date_time = convert(varchar,'dec 31, 9999',120)
 where r.r_mms_val_membership_type_group_id is null
    or (r.r_mms_val_membership_type_group_id is not null
        and s.source_hash <> r.dv_hash)

declare @start_r_id bigint, @c int, @user varchar(50)   
--set @c = isnull((select max(rownum) from #process),0)

--exec dbo.proc_util_sequence_number_get_next @table_name = 'r_mms_val_membership_type_group', @id_count = @c, @start_id = @start_r_id out

begin tran
--end date existing business keys that have a new record with a different hash coming in
set @user = suser_sname()
update dbo.r_mms_val_membership_type_group
   set dv_load_end_date_time = @job_start_date_time,
       dv_updated_date_time = getdate(),
	   dv_update_user = @user
  from #process
 where r_mms_val_membership_type_group.r_mms_val_membership_type_group_id = #process.r_mms_val_membership_type_group_id

--insert incremental changes 
insert into dbo.r_mms_val_membership_type_group (
       bk_hash,
       val_membership_type_group_id,
       description,
       sort_order,
       inserted_date_time,
       updated_date_time,
       val_card_level_id,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
	   dv_inserted_date_time,
	   dv_insert_user,
	   dv_updated_date_time,
	   dv_update_user)
select bk_hash,
       val_membership_type_group_id,
       description,
       sort_order,
       inserted_date_time,
       updated_date_time,
       val_card_level_id,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
	   getdate(),
	   suser_sname(),
	   null,
	   null
  from #process
commit tran

end
