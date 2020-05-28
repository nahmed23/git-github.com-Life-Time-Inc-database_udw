CREATE PROC [dbo].[proc_d_exerp_sale_log] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_sale_log)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_sale_log_insert') is not null drop table #p_exerp_sale_log_insert
create table dbo.#p_exerp_sale_log_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_sale_log.p_exerp_sale_log_id,
       p_exerp_sale_log.bk_hash
  from dbo.p_exerp_sale_log
 where p_exerp_sale_log.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_sale_log.dv_batch_id > @max_dv_batch_id
        or p_exerp_sale_log.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_sale_log.bk_hash,
       p_exerp_sale_log.bk_hash fact_exerp_transaction_log_key,
       p_exerp_sale_log.sale_log_id sale_log_id,
       case when p_exerp_sale_log.bk_hash in('-997', '-998', '-999') then p_exerp_sale_log.bk_hash
            when s_exerp_sale_log.book_datetime is null then '-998'
            else convert(varchar, s_exerp_sale_log.book_datetime, 112)
        end book_dim_date_key,
       case when p_exerp_sale_log.bk_hash in ('-997','-998','-999') then p_exerp_sale_log.bk_hash
            when s_exerp_sale_log.book_datetime is null then '-998'
            else '1' + replace(substring(convert(varchar,s_exerp_sale_log.book_datetime,114), 1, 5),':','')
        end book_dim_time_key,
       l_exerp_sale_log.cash_register_center_id cash_register_center_id,
       case when p_exerp_sale_log.bk_hash in('-997', '-998', '-999') then p_exerp_sale_log.bk_hash
        when l_exerp_sale_log.cash_register_center_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_sale_log.cash_register_center_id as int) as varchar(500)),'z#@$k%&P'))),2)  end cash_register_dim_club_key,
       l_exerp_sale_log.company_id company_id,
       case when p_exerp_sale_log.bk_hash in('-997', '-998', '-999') then p_exerp_sale_log.bk_hash
            when l_exerp_sale_log.credit_sale_log_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_sale_log.credit_sale_log_id as varchar(4000)),'z#@$k%&P'))),2)  end credit_fact_exerp_transaction_log_key,
       case when p_exerp_sale_log.bk_hash in('-997', '-998', '-999') then p_exerp_sale_log.bk_hash
            when l_exerp_sale_log.center_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_sale_log.center_id as int) as varchar(500)),'z#@$k%&P'))),2)  end dim_club_key,
       case when p_exerp_sale_log.bk_hash in('-997', '-998', '-999') then p_exerp_sale_log.bk_hash
            when l_exerp_sale_log.product_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_sale_log.product_id as varchar(4000)),'z#@$k%&P'))),2)  end dim_exerp_product_key,
       case when p_exerp_sale_log.bk_hash in('-997', '-998', '-999') then p_exerp_sale_log.bk_hash 
              when ((l_exerp_sale_log.person_id is null) OR (l_exerp_sale_log.person_id LIKE '%e%') or (l_exerp_sale_log.person_id LIKE '%OLDe%')
       	    or (len(l_exerp_sale_log.person_id) > 9) or (d_exerp_person.person_type = 'STAFF' and l_exerp_sale_log.person_id not LIKE '%e%') 
       		  or (d_exerp_person.person_type = 'STAFF') or (isnumeric(l_exerp_sale_log.person_id) = 0)) then '-998' 
       		       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_sale_log.person_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_mms_member_key,
       case when p_exerp_sale_log.bk_hash in('-997', '-998', '-999') then p_exerp_sale_log.bk_hash
            when s_exerp_sale_log.entry_datetime is null then '-998'
            else convert(varchar, s_exerp_sale_log.entry_datetime, 112)
        end entry_dim_date_key,
       case when p_exerp_sale_log.bk_hash in ('-997','-998','-999') then p_exerp_sale_log.bk_hash
            when s_exerp_sale_log.entry_datetime is null then '-998'
            else '1' + replace(substring(convert(varchar,s_exerp_sale_log.entry_datetime,114), 1, 5),':','')
        end entry_dim_time_key,
       s_exerp_sale_log.ets ets,
       l_exerp_sale_log_1.external_id external_id,
       s_exerp_sale_log.flat_rate_commission flat_rate_commission,
       s_exerp_sale_log.gl_credit_account gl_credit_account,
       s_exerp_sale_log.gl_debit_account gl_debit_account,
       case when s_exerp_sale_log.is_company = 1 then 'Y'        else 'N'  end is_company_flag,
       s_exerp_sale_log.net_amount net_amount,
       s_exerp_sale_log.period_commission period_commission,
       case when p_exerp_sale_log.bk_hash in('-997', '-998', '-999') then p_exerp_sale_log.bk_hash
            when s_exerp_sale_log.product_center is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(s_exerp_sale_log.product_center as int) as varchar(500)),'z#@$k%&P'))),2)  end product_dim_club_key,
       s_exerp_sale_log.product_normal_price product_normal_price,
       isnull(s_exerp_sale_log.product_type, '') product_type,
       s_exerp_sale_log.quantity quantity,
       s_exerp_sale_log.sale_commission sale_commission,
       case when p_exerp_sale_log.bk_hash in ('-997','-998','-999') then p_exerp_sale_log.bk_hash
       when l_exerp_sale_log.sale_person_id is null then '-998'
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(substring(l_exerp_sale_log.sale_person_id, PATINDEX('%[0-9]%',l_exerp_sale_log.sale_person_id), 500) as int) as varchar(500)),'z#@$k%&P'))),2) end sale_entered_dim_employee_key,
       case when p_exerp_sale_log.bk_hash in('-997', '-998', '-999') then p_exerp_sale_log.bk_hash
           when l_exerp_sale_log.sale_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_sale_log.sale_id as varchar(4000)),'z#@$k%&P'))),2)   end sale_fact_exerp_transaction_log_key,
       l_exerp_sale_log.sale_id sale_id,
       isnull(s_exerp_sale_log.sale_type, '') sale_type,
       s_exerp_sale_log.sale_units sale_units,
       isnull(s_exerp_sale_log.source_type, '') source_type,
       case when p_exerp_sale_log.bk_hash in('-997', '-998', '-999') then p_exerp_sale_log.bk_hash
            when l_exerp_sale_log.sponsor_sale_log_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_sale_log.sponsor_sale_log_id as varchar(4000)),'z#@$k%&P'))),2)  end sponsor_fact_exerp_transaction_log_key,
       s_exerp_sale_log.total_amount total_amount,
       s_exerp_sale_log.vat_amount vat_amount,
       isnull(h_exerp_sale_log.dv_deleted,0) dv_deleted,
       p_exerp_sale_log.p_exerp_sale_log_id,
       p_exerp_sale_log.dv_batch_id,
       p_exerp_sale_log.dv_load_date_time,
       p_exerp_sale_log.dv_load_end_date_time
  from dbo.h_exerp_sale_log
  join dbo.p_exerp_sale_log
    on h_exerp_sale_log.bk_hash = p_exerp_sale_log.bk_hash
  join #p_exerp_sale_log_insert
    on p_exerp_sale_log.bk_hash = #p_exerp_sale_log_insert.bk_hash
   and p_exerp_sale_log.p_exerp_sale_log_id = #p_exerp_sale_log_insert.p_exerp_sale_log_id
  join dbo.l_exerp_sale_log
    on p_exerp_sale_log.bk_hash = l_exerp_sale_log.bk_hash
   and p_exerp_sale_log.l_exerp_sale_log_id = l_exerp_sale_log.l_exerp_sale_log_id
  join dbo.l_exerp_sale_log_1
    on p_exerp_sale_log.bk_hash = l_exerp_sale_log_1.bk_hash
   and p_exerp_sale_log.l_exerp_sale_log_1_id = l_exerp_sale_log_1.l_exerp_sale_log_1_id
  join dbo.s_exerp_sale_log
    on p_exerp_sale_log.bk_hash = s_exerp_sale_log.bk_hash
   and p_exerp_sale_log.s_exerp_sale_log_id = s_exerp_sale_log.s_exerp_sale_log_id
 left join 	d_exerp_person		on l_exerp_sale_log.person_id = d_exerp_person.person_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_sale_log
   where d_exerp_sale_log.bk_hash in (select bk_hash from #p_exerp_sale_log_insert)

  insert dbo.d_exerp_sale_log(
             bk_hash,
             fact_exerp_transaction_log_key,
             sale_log_id,
             book_dim_date_key,
             book_dim_time_key,
             cash_register_center_id,
             cash_register_dim_club_key,
             company_id,
             credit_fact_exerp_transaction_log_key,
             dim_club_key,
             dim_exerp_product_key,
             dim_mms_member_key,
             entry_dim_date_key,
             entry_dim_time_key,
             ets,
             external_id,
             flat_rate_commission,
             gl_credit_account,
             gl_debit_account,
             is_company_flag,
             net_amount,
             period_commission,
             product_dim_club_key,
             product_normal_price,
             product_type,
             quantity,
             sale_commission,
             sale_entered_dim_employee_key,
             sale_fact_exerp_transaction_log_key,
             sale_id,
             sale_type,
             sale_units,
             source_type,
             sponsor_fact_exerp_transaction_log_key,
             total_amount,
             vat_amount,
             deleted_flag,
             p_exerp_sale_log_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_exerp_transaction_log_key,
         sale_log_id,
         book_dim_date_key,
         book_dim_time_key,
         cash_register_center_id,
         cash_register_dim_club_key,
         company_id,
         credit_fact_exerp_transaction_log_key,
         dim_club_key,
         dim_exerp_product_key,
         dim_mms_member_key,
         entry_dim_date_key,
         entry_dim_time_key,
         ets,
         external_id,
         flat_rate_commission,
         gl_credit_account,
         gl_debit_account,
         is_company_flag,
         net_amount,
         period_commission,
         product_dim_club_key,
         product_normal_price,
         product_type,
         quantity,
         sale_commission,
         sale_entered_dim_employee_key,
         sale_fact_exerp_transaction_log_key,
         sale_id,
         sale_type,
         sale_units,
         source_type,
         sponsor_fact_exerp_transaction_log_key,
         total_amount,
         vat_amount,
         dv_deleted,
         p_exerp_sale_log_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_sale_log)
--Done!
end
