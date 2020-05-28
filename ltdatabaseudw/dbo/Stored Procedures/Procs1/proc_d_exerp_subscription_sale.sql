CREATE PROC [dbo].[proc_d_exerp_subscription_sale] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_subscription_sale)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_subscription_sale_insert') is not null drop table #p_exerp_subscription_sale_insert
create table dbo.#p_exerp_subscription_sale_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_subscription_sale.p_exerp_subscription_sale_id,
       p_exerp_subscription_sale.bk_hash
  from dbo.p_exerp_subscription_sale
 where p_exerp_subscription_sale.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_subscription_sale.dv_batch_id > @max_dv_batch_id
        or p_exerp_subscription_sale.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_subscription_sale.bk_hash,
       p_exerp_subscription_sale.bk_hash fact_exerp_subscription_sale_key,
       p_exerp_subscription_sale.subscription_sale_id subscription_sale_id,
       s_exerp_subscription_sale.admin_fee_discount admin_fee_discount,
       s_exerp_subscription_sale.admin_fee_member admin_fee_member,
       s_exerp_subscription_sale.admin_fee_normal_price admin_fee_normal_price,
       s_exerp_subscription_sale.admin_fee_price admin_fee_price,
       s_exerp_subscription_sale.admin_fee_sponsored admin_fee_sponsored,
       s_exerp_subscription_sale.binding_days binding_days,
       case when p_exerp_subscription_sale.bk_hash in ('-997','-998','-999') then p_exerp_subscription_sale.bk_hash
         when l_exerp_subscription_sale.center_id is null then '-998'
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_subscription_sale.center_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_club_key,
       case when p_exerp_subscription_sale.bk_hash in ('-997','-998','-999') then p_exerp_subscription_sale.bk_hash
         when l_exerp_subscription_sale.product_id is null then '-998'
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_subscription_sale.product_id as varchar(4000)),'z#@$k%&P'))),2)   end dim_exerp_product_key,
       case when p_exerp_subscription_sale.bk_hash in ('-997','-998','-999') then p_exerp_subscription_sale.bk_hash
         when l_exerp_subscription_sale.subscription_id is null then '-998'
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_subscription_sale.subscription_id as varchar(4000)),'z#@$k%&P'))),2)   end dim_exerp_subscription_key,
       case when p_exerp_subscription_sale.bk_hash in('-997', '-998', '-999') then p_exerp_subscription_sale.bk_hash
           when s_exerp_subscription_sale.end_date is null then '-998'
        else convert(varchar, s_exerp_subscription_sale.end_date, 112)    end end_dim_date_key,
       s_exerp_subscription_sale.ets ets,
       s_exerp_subscription_sale.init_contract_value init_contract_value,
       s_exerp_subscription_sale.init_period_discount init_period_discount,
       s_exerp_subscription_sale.init_period_member init_period_member,
       s_exerp_subscription_sale.init_period_normal_price init_period_normal_price,
       s_exerp_subscription_sale.init_period_price init_period_price,
       s_exerp_subscription_sale.init_period_sponsored init_period_sponsored,
       s_exerp_subscription_sale.jf_discount jf_discount,
       case when p_exerp_subscription_sale.bk_hash in ('-997','-998','-999') then p_exerp_subscription_sale.bk_hash
         when l_exerp_subscription_sale.jf_sale_log_id is null then '-998'
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_subscription_sale.jf_sale_log_id as varchar(4000)),'z#@$k%&P'))),2)   end jf_fact_exerp_transaction_log_key,
       s_exerp_subscription_sale.jf_member jf_member,
       s_exerp_subscription_sale.jf_normal_price jf_normal_price,
       s_exerp_subscription_sale.jf_price jf_price,
       s_exerp_subscription_sale.jf_sponsored jf_sponsored,
       case when p_exerp_subscription_sale.bk_hash in ('-997','-998','-999') then p_exerp_subscription_sale.bk_hash
         when l_exerp_subscription_sale.previous_subscription_id is null then '-998'
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_subscription_sale.previous_subscription_id as varchar(4000)),'z#@$k%&P'))),2)   end previous_dim_exerp_subscription_key,
       s_exerp_subscription_sale.prorata_period_discount prorata_period_discount,
       s_exerp_subscription_sale.prorata_period_member prorata_period_member,
       s_exerp_subscription_sale.prorata_period_normal_price prorata_period_normal_price,
       s_exerp_subscription_sale.prorata_period_price prorata_period_price,
       s_exerp_subscription_sale.prorata_period_sponsored prorata_period_sponsored,
       case when p_exerp_subscription_sale.bk_hash in('-997', '-998', '-999') then p_exerp_subscription_sale.bk_hash
           when s_exerp_subscription_sale.sale_datetime is null then '-998'
        else convert(varchar, s_exerp_subscription_sale.sale_datetime, 112)    end sale_dim_date_key,
       case when p_exerp_subscription_sale.bk_hash in ('-997','-998','-999') then p_exerp_subscription_sale.bk_hash
           when l_exerp_subscription_sale.sale_person_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(substring(l_exerp_subscription_sale.sale_person_id,PATINDEX('%[0-9]%',
       	l_exerp_subscription_sale.sale_person_id), 500) as int) as varchar(500)),'z#@$k%&P'))),2)
        end sale_dim_employee_key,
       case when p_exerp_subscription_sale.bk_hash in ('-997','-998','-999') then p_exerp_subscription_sale.bk_hash
       when s_exerp_subscription_sale.sale_datetime is null then '-998'
       else '1' + replace(substring(convert(varchar,s_exerp_subscription_sale.sale_datetime,114), 1, 5),':','') end sale_dim_time_key,
       l_exerp_subscription_sale.sale_id sale_id,
       case when p_exerp_subscription_sale.bk_hash in('-997', '-998', '-999') then p_exerp_subscription_sale.bk_hash
           when s_exerp_subscription_sale.start_date is null then '-998'
        else convert(varchar, s_exerp_subscription_sale.start_date, 112)    end start_dim_date_key,
       case when p_exerp_subscription_sale.bk_hash in ('-997','-998','-999') then p_exerp_subscription_sale.bk_hash
         when l_exerp_subscription_sale.subscription_center is null then '-998'
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_subscription_sale.subscription_center as int) as varchar(500)),'z#@$k%&P'))),2)   end subscription_dim_club_key,
       s_exerp_subscription_sale.state subscription_sale_state,
       s_exerp_subscription_sale.type subscription_sale_type,
       isnull(h_exerp_subscription_sale.dv_deleted,0) dv_deleted,
       p_exerp_subscription_sale.p_exerp_subscription_sale_id,
       p_exerp_subscription_sale.dv_batch_id,
       p_exerp_subscription_sale.dv_load_date_time,
       p_exerp_subscription_sale.dv_load_end_date_time
  from dbo.h_exerp_subscription_sale
  join dbo.p_exerp_subscription_sale
    on h_exerp_subscription_sale.bk_hash = p_exerp_subscription_sale.bk_hash
  join #p_exerp_subscription_sale_insert
    on p_exerp_subscription_sale.bk_hash = #p_exerp_subscription_sale_insert.bk_hash
   and p_exerp_subscription_sale.p_exerp_subscription_sale_id = #p_exerp_subscription_sale_insert.p_exerp_subscription_sale_id
  join dbo.l_exerp_subscription_sale
    on p_exerp_subscription_sale.bk_hash = l_exerp_subscription_sale.bk_hash
   and p_exerp_subscription_sale.l_exerp_subscription_sale_id = l_exerp_subscription_sale.l_exerp_subscription_sale_id
  join dbo.s_exerp_subscription_sale
    on p_exerp_subscription_sale.bk_hash = s_exerp_subscription_sale.bk_hash
   and p_exerp_subscription_sale.s_exerp_subscription_sale_id = s_exerp_subscription_sale.s_exerp_subscription_sale_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_subscription_sale
   where d_exerp_subscription_sale.bk_hash in (select bk_hash from #p_exerp_subscription_sale_insert)

  insert dbo.d_exerp_subscription_sale(
             bk_hash,
             fact_exerp_subscription_sale_key,
             subscription_sale_id,
             admin_fee_discount,
             admin_fee_member,
             admin_fee_normal_price,
             admin_fee_price,
             admin_fee_sponsored,
             binding_days,
             dim_club_key,
             dim_exerp_product_key,
             dim_exerp_subscription_key,
             end_dim_date_key,
             ets,
             init_contract_value,
             init_period_discount,
             init_period_member,
             init_period_normal_price,
             init_period_price,
             init_period_sponsored,
             jf_discount,
             jf_fact_exerp_transaction_log_key,
             jf_member,
             jf_normal_price,
             jf_price,
             jf_sponsored,
             previous_dim_exerp_subscription_key,
             prorata_period_discount,
             prorata_period_member,
             prorata_period_normal_price,
             prorata_period_price,
             prorata_period_sponsored,
             sale_dim_date_key,
             sale_dim_employee_key,
             sale_dim_time_key,
             sale_id,
             start_dim_date_key,
             subscription_dim_club_key,
             subscription_sale_state,
             subscription_sale_type,
             deleted_flag,
             p_exerp_subscription_sale_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_exerp_subscription_sale_key,
         subscription_sale_id,
         admin_fee_discount,
         admin_fee_member,
         admin_fee_normal_price,
         admin_fee_price,
         admin_fee_sponsored,
         binding_days,
         dim_club_key,
         dim_exerp_product_key,
         dim_exerp_subscription_key,
         end_dim_date_key,
         ets,
         init_contract_value,
         init_period_discount,
         init_period_member,
         init_period_normal_price,
         init_period_price,
         init_period_sponsored,
         jf_discount,
         jf_fact_exerp_transaction_log_key,
         jf_member,
         jf_normal_price,
         jf_price,
         jf_sponsored,
         previous_dim_exerp_subscription_key,
         prorata_period_discount,
         prorata_period_member,
         prorata_period_normal_price,
         prorata_period_price,
         prorata_period_sponsored,
         sale_dim_date_key,
         sale_dim_employee_key,
         sale_dim_time_key,
         sale_id,
         start_dim_date_key,
         subscription_dim_club_key,
         subscription_sale_state,
         subscription_sale_type,
         dv_deleted,
         p_exerp_subscription_sale_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_subscription_sale)
--Done!
end
