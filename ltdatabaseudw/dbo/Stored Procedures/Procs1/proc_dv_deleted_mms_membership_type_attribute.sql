CREATE PROC [dbo].[proc_dv_deleted_mms_membership_type_attribute] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)
declare @insert_date_time datetime
declare @user varchar(500) = suser_sname()

--THIS LOGIC (#bk_hash) IS ONLY A PLACEHOLDER AS AN EXAMPLE
--It may need to be manually updated for each individual object
--More logic than a simple query may be required, but the end result should be a #bk_hash table populated with bk_hashes, deleted times, and deleted batchids
if object_id('tempdb..#bk_hash') is not null drop table #bk_hash
create table #bk_hash with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select d.deleted_bk_hash bk_hash,
       d.dv_load_date_time,
       d.dv_batch_id
  from d_mms_deleted_data d --someday this needs a d_table
 where d.dv_batch_id >= @current_dv_batch_id
   and d.table_name = 'MembershipTypeAttribute'
   and 0=1

set @insert_date_time = getdate()
update h_mms_membership_type_attribute
   set dv_deleted = 1,
       dv_updated_date_time = @insert_date_time,
       dv_update_user = @user
  from #bk_hash
 where h_mms_membership_type_attribute.bk_hash = #bk_hash.bk_hash

--Insert all updated and new l_mms_membership_type_attribute records
set @insert_date_time = getdate()
insert into l_mms_membership_type_attribute (
       bk_hash,
       membership_type_attribute_id,
       membership_type_id,
       val_membership_type_attribute_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user,
       dv_deleted)
select l_mms_membership_type_attribute.bk_hash,
       l_mms_membership_type_attribute.membership_type_attribute_id,
       l_mms_membership_type_attribute.membership_type_id,
       l_mms_membership_type_attribute.val_membership_type_attribute_id,
       #bk_hash.dv_load_date_time,
       #bk_hash.dv_batch_id,
       l_mms_membership_type_attribute.dv_r_load_source_id,
       l_mms_membership_type_attribute.dv_hash,
       @insert_date_time,
       @user,
       1 --deleted
  from p_mms_membership_type_attribute
  join l_mms_membership_type_attribute
    on p_mms_membership_type_attribute.bk_hash = l_mms_membership_type_attribute.bk_hash
   and p_mms_membership_type_attribute.l_mms_membership_type_attribute_id = l_mms_membership_type_attribute.l_mms_membership_type_attribute_id
  join #bk_hash
    on p_mms_membership_type_attribute.bk_hash = #bk_hash.bk_hash
 where p_mms_membership_type_attribute.bk_hash in (select bk_hash from #bk_hash)
   and p_mms_membership_type_attribute.dv_load_end_date_time = 'dec 31, 9999'
   and isnull(l_mms_membership_type_attribute.dv_deleted,0) != 1

--Insert all updated and new s_mms_membership_type_attribute records
set @insert_date_time = getdate()
insert into s_mms_membership_type_attribute (
       bk_hash,
       membership_type_attribute_id,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user,
       dv_deleted)
select s_mms_membership_type_attribute.bk_hash,
       s_mms_membership_type_attribute.membership_type_attribute_id,
       s_mms_membership_type_attribute.inserted_date_time,
       s_mms_membership_type_attribute.updated_date_time,
       #bk_hash.dv_load_date_time,
       #bk_hash.dv_batch_id,
       s_mms_membership_type_attribute.dv_r_load_source_id,
       s_mms_membership_type_attribute.dv_hash,
       @insert_date_time,
       @user,
       1 --deleted
  from p_mms_membership_type_attribute
  join s_mms_membership_type_attribute
    on p_mms_membership_type_attribute.bk_hash = s_mms_membership_type_attribute.bk_hash
   and p_mms_membership_type_attribute.s_mms_membership_type_attribute_id = s_mms_membership_type_attribute.s_mms_membership_type_attribute_id
  join #bk_hash
    on p_mms_membership_type_attribute.bk_hash = #bk_hash.bk_hash
 where p_mms_membership_type_attribute.bk_hash in (select bk_hash from #bk_hash)
   and p_mms_membership_type_attribute.dv_load_end_date_time = 'dec 31, 9999'
   and isnull(s_mms_membership_type_attribute.dv_deleted,0) != 1

end
