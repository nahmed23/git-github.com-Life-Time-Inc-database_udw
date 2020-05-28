CREATE PROC [dbo].[proc_d_dim_crmcloudsync_ltf_inquiry] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
exec dbo.proc_util_task_status_insert 'proc_d_dim_crmcloudsync_ltf_inquiry','proc_d_dim_crmcloudsync_ltf_inquiry start',@current_dv_batch_id

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
exec dbo.proc_util_task_status_insert 'proc_d_dim_crmcloudsync_ltf_inquiry','max dv_batch_id',@current_dv_batch_id
if object_id('tempdb..#batch_id') is not null drop table #batch_id
create table dbo.#batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @current_dv_batch_id as current_dv_batch_id
  from dbo.d_dim_crmcloudsync_ltf_inquiry

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
exec dbo.proc_util_task_status_insert 'proc_d_dim_crmcloudsync_ltf_inquiry','#p_crmcloudsync_ltf_inquiry_insert',@current_dv_batch_id
if object_id('tempdb..#p_crmcloudsync_ltf_inquiry_insert') is not null drop table #p_crmcloudsync_ltf_inquiry_insert
create table dbo.#p_crmcloudsync_ltf_inquiry_insert with(distribution=round_robin, location=user_db, heap) as
select p_crmcloudsync_ltf_inquiry.p_crmcloudsync_ltf_inquiry_id,
       p_crmcloudsync_ltf_inquiry.bk_hash,
       row_number() over (order by p_crmcloudsync_ltf_inquiry_id) row_num
  from dbo.p_crmcloudsync_ltf_inquiry
  join #batch_id
    on p_crmcloudsync_ltf_inquiry.dv_batch_id > #batch_id.max_dv_batch_id
    or p_crmcloudsync_ltf_inquiry.dv_batch_id = #batch_id.current_dv_batch_id
 where p_crmcloudsync_ltf_inquiry.dv_load_end_date_time = convert(datetime,'9999.12.31',102)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
exec dbo.proc_util_task_status_insert 'proc_d_dim_crmcloudsync_ltf_inquiry','#insert',@current_dv_batch_id
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=round_robin, location=user_db, heap) as
select #p_crmcloudsync_ltf_inquiry_insert.row_num,
       p_crmcloudsync_ltf_inquiry.bk_hash dim_crmcloudsync_ltf_inquiry_key,
       p_crmcloudsync_ltf_inquiry.activity_id activity_id,
       ISNULL(s_crmcloudsync_ltf_inquiry.ltf_inquiry_source,'') contact_source,
       case when p_crmcloudsync_ltf_inquiry.bk_hash in ('-997','-998','-999') then p_crmcloudsync_ltf_inquiry.bk_hash
            when l_crmcloudsync_ltf_inquiry.ltf_mms_clubid is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_ltf_inquiry.ltf_mms_clubid as varchar(500)),'z#@$k%&P'))),2)
        end dim_mms_club_key,
       s_crmcloudsync_ltf_inquiry.ltf_first_name first_name,
       s_crmcloudsync_ltf_inquiry.ltf_last_name last_name,
       ISNULL(s_crmcloudsync_ltf_inquiry.ltf_inquiry_type,'') lead_source,
       case when p_crmcloudsync_ltf_inquiry.bk_hash in ('-997','-998','-999') then 'N'
            when ISNULL(l_crmcloudsync_ltf_inquiry.ltf_referring_member_id,'') = '' then 'N'
            else 'Y'
        end referring_member_flag,
       s_crmcloudsync_ltf_inquiry.status_code_name state_code_name,
       p_crmcloudsync_ltf_inquiry.p_crmcloudsync_ltf_inquiry_id,
       p_crmcloudsync_ltf_inquiry.dv_batch_id,
       p_crmcloudsync_ltf_inquiry.dv_load_date_time,
       p_crmcloudsync_ltf_inquiry.dv_load_end_date_time
  from dbo.p_crmcloudsync_ltf_inquiry
  join #p_crmcloudsync_ltf_inquiry_insert
    on p_crmcloudsync_ltf_inquiry.p_crmcloudsync_ltf_inquiry_id = #p_crmcloudsync_ltf_inquiry_insert.p_crmcloudsync_ltf_inquiry_id
  join dbo.l_crmcloudsync_ltf_inquiry
    on p_crmcloudsync_ltf_inquiry.l_crmcloudsync_ltf_inquiry_id = l_crmcloudsync_ltf_inquiry.l_crmcloudsync_ltf_inquiry_id
  join dbo.s_crmcloudsync_ltf_inquiry
    on p_crmcloudsync_ltf_inquiry.s_crmcloudsync_ltf_inquiry_id = s_crmcloudsync_ltf_inquiry.s_crmcloudsync_ltf_inquiry_id

declare @start int, @end int, @task_description varchar(50)
declare @start_p_id bigint
declare @insert_count bigint
set @start = 1
set @end = (select max(row_num) from #insert)

while @start <= @end
begin

    set @insert_count = isnull((select count(*) from #insert where row_num >= @start and row_num < @start+1000000),0)
    exec dbo.proc_util_sequence_number_get_next @table_name = 'd_dim_crmcloudsync_ltf_inquiry', @id_count = @insert_count, @start_id = @start_p_id out

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
    set @task_description = 'final insert/update '+cast(@start as varchar)+' of '+cast(@end as varchar)
    exec dbo.proc_util_task_status_insert 'proc_d_dim_crmcloudsync_ltf_inquiry',@task_description,@current_dv_batch_id
    begin tran
      delete dbo.d_dim_crmcloudsync_ltf_inquiry
       where d_dim_crmcloudsync_ltf_inquiry.dim_crmcloudsync_ltf_inquiry_key in (select bk_hash from #p_crmcloudsync_ltf_inquiry_insert where row_num >= @start and row_num < @start+1000000)

      insert dbo.d_dim_crmcloudsync_ltf_inquiry(
                 d_dim_crmcloudsync_ltf_inquiry_id,
                 dim_crmcloudsync_ltf_inquiry_key,
                 activity_id,
                 contact_source,
                 dim_mms_club_key,
                 first_name,
                 last_name,
                 lead_source,
                 referring_member_flag,
                 state_code_name,
                 p_crmcloudsync_ltf_inquiry_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select @start_p_id + row_num,
             dim_crmcloudsync_ltf_inquiry_key,
             activity_id,
             contact_source,
             dim_mms_club_key,
             first_name,
             last_name,
             lead_source,
             referring_member_flag,
             state_code_name,
             p_crmcloudsync_ltf_inquiry_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             getdate(),
             suser_sname()
        from #insert
       where row_num >= @start
         and row_num < @start+1000000
    commit tran

    set @start = @start+1000000
end

--Done!
exec dbo.proc_util_task_status_insert 'proc_d_dim_crmcloudsync_ltf_inquiry','proc_d_dim_crmcloudsync_ltf_inquiry end',@current_dv_batch_id
end
