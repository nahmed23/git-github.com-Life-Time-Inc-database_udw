CREATE PROC [dbo].[proc_d_mart_fact_seg_member_expected_value] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mart_fact_seg_member_expected_value)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mart_fact_seg_member_expected_value_insert') is not null drop table #p_mart_fact_seg_member_expected_value_insert
create table dbo.#p_mart_fact_seg_member_expected_value_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mart_fact_seg_member_expected_value.p_mart_fact_seg_member_expected_value_id,
       p_mart_fact_seg_member_expected_value.bk_hash
  from dbo.p_mart_fact_seg_member_expected_value
 where p_mart_fact_seg_member_expected_value.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mart_fact_seg_member_expected_value.dv_batch_id > @max_dv_batch_id
        or p_mart_fact_seg_member_expected_value.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mart_fact_seg_member_expected_value.bk_hash,
       p_mart_fact_seg_member_expected_value.fact_seg_member_expected_value_id fact_seg_member_expected_value_id,
       case when s_mart_fact_seg_member_expected_value.active_flag = 1 then 'Y' else 'N' end active_flag,
       case when p_mart_fact_seg_member_expected_value.bk_hash in('-997', '-998', '-999') then p_mart_fact_seg_member_expected_value.bk_hash
        when l_mart_fact_seg_member_expected_value.member_id is null then '-998'
        else  convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mart_fact_seg_member_expected_value.member_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_mms_member_key,
       s_mart_fact_seg_member_expected_value.expected_value_60_months expected_value_60_months,
       l_mart_fact_seg_member_expected_value.member_id  member_id,
       s_mart_fact_seg_member_expected_value.past_spend_last_3_years past_spend_last_3_years,
       s_mart_fact_seg_member_expected_value.row_add_date row_add_date,
       case when p_mart_fact_seg_member_expected_value.bk_hash in('-997', '-998', '-999') then p_mart_fact_seg_member_expected_value.bk_hash
           when s_mart_fact_seg_member_expected_value.row_add_date is null then '-998'
        else convert(varchar, s_mart_fact_seg_member_expected_value.row_add_date, 112)    end row_add_dim_date_key,
       case when p_mart_fact_seg_member_expected_value.bk_hash in ('-997','-998','-999') then p_mart_fact_seg_member_expected_value.bk_hash
       when s_mart_fact_seg_member_expected_value.row_add_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mart_fact_seg_member_expected_value.row_add_date,114), 1, 5),':','') end row_add_dim_time_key,
       s_mart_fact_seg_member_expected_value.row_deactivation_date row_deactivation_date,
       case when p_mart_fact_seg_member_expected_value.bk_hash in('-997', '-998', '-999') then p_mart_fact_seg_member_expected_value.bk_hash
           when s_mart_fact_seg_member_expected_value.row_deactivation_date is null then '-998'
        else convert(varchar, s_mart_fact_seg_member_expected_value.row_deactivation_date, 112)    end row_deactivation_dim_date_key,
       isnull(h_mart_fact_seg_member_expected_value.dv_deleted,0) dv_deleted,
       p_mart_fact_seg_member_expected_value.p_mart_fact_seg_member_expected_value_id,
       p_mart_fact_seg_member_expected_value.dv_batch_id,
       p_mart_fact_seg_member_expected_value.dv_load_date_time,
       p_mart_fact_seg_member_expected_value.dv_load_end_date_time
  from dbo.h_mart_fact_seg_member_expected_value
  join dbo.p_mart_fact_seg_member_expected_value
    on h_mart_fact_seg_member_expected_value.bk_hash = p_mart_fact_seg_member_expected_value.bk_hash
  join #p_mart_fact_seg_member_expected_value_insert
    on p_mart_fact_seg_member_expected_value.bk_hash = #p_mart_fact_seg_member_expected_value_insert.bk_hash
   and p_mart_fact_seg_member_expected_value.p_mart_fact_seg_member_expected_value_id = #p_mart_fact_seg_member_expected_value_insert.p_mart_fact_seg_member_expected_value_id
  join dbo.l_mart_fact_seg_member_expected_value
    on p_mart_fact_seg_member_expected_value.bk_hash = l_mart_fact_seg_member_expected_value.bk_hash
   and p_mart_fact_seg_member_expected_value.l_mart_fact_seg_member_expected_value_id = l_mart_fact_seg_member_expected_value.l_mart_fact_seg_member_expected_value_id
  join dbo.s_mart_fact_seg_member_expected_value
    on p_mart_fact_seg_member_expected_value.bk_hash = s_mart_fact_seg_member_expected_value.bk_hash
   and p_mart_fact_seg_member_expected_value.s_mart_fact_seg_member_expected_value_id = s_mart_fact_seg_member_expected_value.s_mart_fact_seg_member_expected_value_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mart_fact_seg_member_expected_value
   where d_mart_fact_seg_member_expected_value.bk_hash in (select bk_hash from #p_mart_fact_seg_member_expected_value_insert)

  insert dbo.d_mart_fact_seg_member_expected_value(
             bk_hash,
             fact_seg_member_expected_value_id,
             active_flag,
             dim_mms_member_key,
             expected_value_60_months,
             member_id,
             past_spend_last_3_years,
             row_add_date,
             row_add_dim_date_key,
             row_add_dim_time_key,
             row_deactivation_date,
             row_deactivation_dim_date_key,
             deleted_flag,
             p_mart_fact_seg_member_expected_value_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_seg_member_expected_value_id,
         active_flag,
         dim_mms_member_key,
         expected_value_60_months,
         member_id,
         past_spend_last_3_years,
         row_add_date,
         row_add_dim_date_key,
         row_add_dim_time_key,
         row_deactivation_date,
         row_deactivation_dim_date_key,
         dv_deleted,
         p_mart_fact_seg_member_expected_value_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mart_fact_seg_member_expected_value)
--Done!
end
