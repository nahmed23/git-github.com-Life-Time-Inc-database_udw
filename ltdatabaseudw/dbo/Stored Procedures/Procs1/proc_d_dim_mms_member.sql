CREATE PROC [dbo].[proc_d_dim_mms_member] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
exec dbo.proc_util_task_status_insert 'proc_d_dim_mms_member','proc_d_dim_mms_member start',@current_dv_batch_id

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
exec dbo.proc_util_task_status_insert 'proc_d_dim_mms_member','max dv_batch_id',@current_dv_batch_id
if object_id('tempdb..#batch_id') is not null drop table #batch_id
create table dbo.#batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @current_dv_batch_id as current_dv_batch_id
  from dbo.d_dim_mms_member

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
exec dbo.proc_util_task_status_insert 'proc_d_dim_mms_member','#p_mms_member_insert',@current_dv_batch_id
if object_id('tempdb..#p_mms_member_insert') is not null drop table #p_mms_member_insert
create table dbo.#p_mms_member_insert with(distribution=round_robin, location=user_db, heap) as
select p_mms_member.p_mms_member_id,
       p_mms_member.bk_hash,
       row_number() over (order by p_mms_member_id) row_num
  from dbo.p_mms_member
  join #batch_id
    on p_mms_member.dv_batch_id > #batch_id.max_dv_batch_id
    or p_mms_member.dv_batch_id = #batch_id.current_dv_batch_id
 where p_mms_member.dv_load_end_date_time = convert(datetime,'9999.12.31',102)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
exec dbo.proc_util_task_status_insert 'proc_d_dim_mms_member','#insert',@current_dv_batch_id
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=round_robin, location=user_db, heap) as
select #p_mms_member_insert.row_num,
       p_mms_member.bk_hash dim_mms_member_key,
       p_mms_member.member_id member_id,
       case when s_mms_member.assess_jr_member_dues_flag = 0 then 'N'
            else 'Y'
        end assess_junior_member_dues_flag,
       case when isnull(s_mms_member.first_name, '') != '' and isnull(s_mms_member.last_name, '') != ''
                 then s_mms_member.first_name + ' ' + s_mms_member.last_name
            when isnull(s_mms_member.first_name, '') = ''
                 then isnull(s_mms_member.last_name, '')
            else isnull(s_mms_member.first_name, '')
        end customer_name,
       case when isnull(s_mms_member.first_name, '') != '' and isnull(s_mms_member.last_name, '') != ''
                 then s_mms_member.last_name + ', ' + s_mms_member.first_name
            when isnull(s_mms_member.first_name, '') = ''
                 then isnull(s_mms_member.last_name, '')
            else isnull(s_mms_member.first_name, '')
        end customer_name_last_first,
       case when s_mms_member.dob is null or s_mms_member.dob < convert(datetime, '1900.01.01', 102) or s_mms_member.dob >= convert(datetime, '2100.01.01', 102)
                 then null
            else s_mms_member.dob
        end date_of_birth,
       case when p_mms_member.bk_hash in ('-997','-998','-999')
                 then p_mms_member.bk_hash
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_member.membership_id as varchar(500)),'z#@$k%&P'))),2)
        end dim_mms_membership_key,
       isnull(ltrim(rtrim(s_mms_member.email_address)), '') email_address,
       isnull(s_mms_member.first_name, '') first_name,
       case when s_mms_member.gender is null or s_mms_member.gender not in ('m', 'f')
                 then 'U'
            else upper(s_mms_member.gender)
        end gender_abbreviation,
       case when s_mms_member.join_date >= convert(datetime, '2100.01.01', 102)
                 then convert(datetime, '9999.12.31', 102)
            else s_mms_member.join_date
        end join_date,
       isnull (s_mms_member.last_name, '') last_name,
       case when s_mms_member.active_flag = 1
                 then 'Y'
            else 'N'
        end member_active_flag,
       'r_mms_val_member_type'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_member.val_member_type_id as varchar(500)),'z#@$k%&P'))),2) member_type_dim_description_key,
       l_mms_member.membership_id membership_id,
       l_mms_member.val_member_type_id ref_mms_val_member_type_id,
       p_mms_member.p_mms_member_id,
       p_mms_member.dv_batch_id,
       p_mms_member.dv_load_date_time,
       p_mms_member.dv_load_end_date_time
  from dbo.p_mms_member
  join #p_mms_member_insert
    on p_mms_member.p_mms_member_id = #p_mms_member_insert.p_mms_member_id
  join dbo.l_mms_member
    on p_mms_member.l_mms_member_id = l_mms_member.l_mms_member_id
  join dbo.s_mms_member
    on p_mms_member.s_mms_member_id = s_mms_member.s_mms_member_id

declare @start int, @end int, @task_description varchar(50)
declare @start_p_id bigint
declare @insert_count bigint
set @start = 1
set @end = (select max(row_num) from #insert)

while @start <= @end
begin

    set @insert_count = isnull((select count(*) from #insert where row_num >= @start and row_num < @start+1000000),0)
    exec dbo.proc_util_sequence_number_get_next @table_name = 'd_dim_mms_member', @id_count = @insert_count, @start_id = @start_p_id out

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
    set @task_description = 'final insert/update '+cast(@start as varchar)+' of '+cast(@end as varchar)
    exec dbo.proc_util_task_status_insert 'proc_d_dim_mms_member',@task_description,@current_dv_batch_id
    begin tran
      delete dbo.d_dim_mms_member
       where d_dim_mms_member.dim_mms_member_key in (select bk_hash from #p_mms_member_insert where row_num >= @start and row_num < @start+1000000)

      insert dbo.d_dim_mms_member(
                 d_dim_mms_member_id,
                 dim_mms_member_key,
                 member_id,
                 assess_junior_member_dues_flag,
                 customer_name,
                 customer_name_last_first,
                 date_of_birth,
                 dim_mms_membership_key,
                 email_address,
                 first_name,
                 gender_abbreviation,
                 join_date,
                 last_name,
                 member_active_flag,
                 member_type_dim_description_key,
                 membership_id,
                 ref_mms_val_member_type_id,
                 p_mms_member_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select @start_p_id + row_num,
             dim_mms_member_key,
             member_id,
             assess_junior_member_dues_flag,
             customer_name,
             customer_name_last_first,
             date_of_birth,
             dim_mms_membership_key,
             email_address,
             first_name,
             gender_abbreviation,
             join_date,
             last_name,
             member_active_flag,
             member_type_dim_description_key,
             membership_id,
             ref_mms_val_member_type_id,
             p_mms_member_id,
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
exec dbo.proc_util_task_status_insert 'proc_d_dim_mms_member','proc_d_dim_mms_member end',@current_dv_batch_id
end
