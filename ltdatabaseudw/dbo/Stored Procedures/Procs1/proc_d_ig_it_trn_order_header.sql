CREATE PROC [dbo].[proc_d_ig_it_trn_order_header] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ig_it_trn_order_header)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ig_it_trn_order_header_insert') is not null drop table #p_ig_it_trn_order_header_insert
create table dbo.#p_ig_it_trn_order_header_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_it_trn_order_header.p_ig_it_trn_order_header_id,
       p_ig_it_trn_order_header.bk_hash
  from dbo.p_ig_it_trn_order_header
 where p_ig_it_trn_order_header.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ig_it_trn_order_header.dv_batch_id > @max_dv_batch_id
        or p_ig_it_trn_order_header.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_it_trn_order_header.bk_hash,
       p_ig_it_trn_order_header.order_hdr_id order_hdr_id,
       s_ig_it_trn_order_header.check_no check_number,
       case when p_ig_it_trn_order_header.bk_hash in ('-997', '-998', '-999') then p_ig_it_trn_order_header.bk_hash
    when l_ig_it_trn_order_header.bus_day_id is null then '-998'
    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_ig_it_trn_order_header.bus_day_id as int) as varchar(500)),'z#@$k%&P'))),2) end d_ig_it_trn_business_day_dates_bk_hash,
       case when p_ig_it_trn_order_header.bk_hash in ('-997', '-998', '-999') then p_ig_it_trn_order_header.bk_hash
           when l_ig_it_trn_order_header.profit_center_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_ig_it_trn_order_header.profit_center_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_cafe_profit_center_key,
       case when p_ig_it_trn_order_header.bk_hash in ('-997', '-998', '-999') then p_ig_it_trn_order_header.bk_hash 
       when s_ig_it_trn_order_header.close_dttime is null then '-998'    
       else convert(varchar, s_ig_it_trn_order_header.close_dttime, 112) end order_close_dim_date_key,
       case when p_ig_it_trn_order_header.bk_hash in ('-997', '-998', '-999') then p_ig_it_trn_order_header.bk_hash  
      when s_ig_it_trn_order_header.close_dttime is null then '-998'
         else '1' + replace(substring(convert(varchar,s_ig_it_trn_order_header.close_dttime,114), 1, 5),':','') end order_close_dim_time_key,
       case when p_ig_it_trn_order_header.bk_hash in ('-997', '-998', '-999') then p_ig_it_trn_order_header.bk_hash 
      when s_ig_it_trn_order_header.close_dttime is null then '-998'    
       else convert(varchar,eomonth(s_ig_it_trn_order_header.close_dttime), 112) end order_close_month_ending_dim_date_key,
       case when p_ig_it_trn_order_header.bk_hash in ('-997','-998', '-999') then 'N'
            when s_ig_it_trn_order_header.close_dttime is null then 'N'    
           else  'Y'  end order_closed_flag,
       case when p_ig_it_trn_order_header.bk_hash in ('-997', '-998', '-999') then p_ig_it_trn_order_header.bk_hash
      when dim_employee.employee_id = -998 or dim_employee.employee_id is null then '-998'
      when s_ig_it_trn_order_header.tran_data_tag_text is null or dim_employee.employee_id is null then '-998'
      when substring(s_ig_it_trn_order_header.tran_data_tag_text,1,3) <> 'PT=' then '-998'
      when ISNUMERIC(substring(s_ig_it_trn_order_header.tran_data_tag_text,4,len(s_ig_it_trn_order_header.tran_data_tag_text))) = 0 then '-998'
    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(dim_employee.employee_id as int) as varchar(500)),'z#@$k%&P'))),2) end order_commissionable_dim_employee_key,
       s_ig_it_trn_order_header.discount_amt order_discount_amount,
       case when p_ig_it_trn_order_header.bk_hash in ('-997', '-998', '-999') then p_ig_it_trn_order_header.bk_hash 
      when s_ig_it_trn_order_header.open_dttime is null then '-998'    
       else convert(varchar, s_ig_it_trn_order_header.open_dttime, 112) end order_open_dim_date_key,
       case when p_ig_it_trn_order_header.bk_hash in ('-997', '-998', '-999') then p_ig_it_trn_order_header.bk_hash  
      when s_ig_it_trn_order_header.open_dttime is null then '-998'
         else '1' + replace(substring(convert(varchar,s_ig_it_trn_order_header.open_dttime,114), 1, 5),':','') end order_open_dim_time_key,
       case when s_ig_it_trn_order_header.refund_flag = 1 then 'Y'    
    else  'N'  end order_refund_flag,
       s_ig_it_trn_order_header.sales_amt_gross order_sales_amount_gross,
       s_ig_it_trn_order_header.service_charge_amt order_service_charge_amount,
       s_ig_it_trn_order_header.tax_amt order_tax_amount,
       case when s_ig_it_trn_order_header.tax_removd_flag = 1 then 'Y'    
    else  'N'  end order_tax_removed_flag,
       s_ig_it_trn_order_header.tip_amt order_tip_amount,
       case when l_ig_it_trn_order_header.void_reason_id = 0 then 'N' else 'Y'  end order_void_flag,
       l_ig_it_trn_order_header.profit_center_id profit_center_id,
       isnull(h_ig_it_trn_order_header.dv_deleted,0) dv_deleted,
       p_ig_it_trn_order_header.p_ig_it_trn_order_header_id,
       p_ig_it_trn_order_header.dv_batch_id,
       p_ig_it_trn_order_header.dv_load_date_time,
       p_ig_it_trn_order_header.dv_load_end_date_time
  from dbo.h_ig_it_trn_order_header
  join dbo.p_ig_it_trn_order_header
    on h_ig_it_trn_order_header.bk_hash = p_ig_it_trn_order_header.bk_hash
  join #p_ig_it_trn_order_header_insert
    on p_ig_it_trn_order_header.bk_hash = #p_ig_it_trn_order_header_insert.bk_hash
   and p_ig_it_trn_order_header.p_ig_it_trn_order_header_id = #p_ig_it_trn_order_header_insert.p_ig_it_trn_order_header_id
  join dbo.l_ig_it_trn_order_header
    on p_ig_it_trn_order_header.bk_hash = l_ig_it_trn_order_header.bk_hash
   and p_ig_it_trn_order_header.l_ig_it_trn_order_header_id = l_ig_it_trn_order_header.l_ig_it_trn_order_header_id
  join dbo.s_ig_it_trn_order_header
    on p_ig_it_trn_order_header.bk_hash = s_ig_it_trn_order_header.bk_hash
   and p_ig_it_trn_order_header.s_ig_it_trn_order_header_id = s_ig_it_trn_order_header.s_ig_it_trn_order_header_id
 left join dim_employee
    on dim_employee.employee_id = case when substring(s_ig_it_trn_order_header.tran_data_tag_text,1,3) <> 'PT=' then -998
        when ISNUMERIC(substring(s_ig_it_trn_order_header.tran_data_tag_text,4,len(s_ig_it_trn_order_header.tran_data_tag_text))) = 0 then -998
           else cast(substring(s_ig_it_trn_order_header.tran_data_tag_text,4,len(s_ig_it_trn_order_header.tran_data_tag_text)) as int) end

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ig_it_trn_order_header
   where d_ig_it_trn_order_header.bk_hash in (select bk_hash from #p_ig_it_trn_order_header_insert)

  insert dbo.d_ig_it_trn_order_header(
             bk_hash,
             order_hdr_id,
             check_number,
             d_ig_it_trn_business_day_dates_bk_hash,
             dim_cafe_profit_center_key,
             order_close_dim_date_key,
             order_close_dim_time_key,
             order_close_month_ending_dim_date_key,
             order_closed_flag,
             order_commissionable_dim_employee_key,
             order_discount_amount,
             order_open_dim_date_key,
             order_open_dim_time_key,
             order_refund_flag,
             order_sales_amount_gross,
             order_service_charge_amount,
             order_tax_amount,
             order_tax_removed_flag,
             order_tip_amount,
             order_void_flag,
             profit_center_id,
             deleted_flag,
             p_ig_it_trn_order_header_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         order_hdr_id,
         check_number,
         d_ig_it_trn_business_day_dates_bk_hash,
         dim_cafe_profit_center_key,
         order_close_dim_date_key,
         order_close_dim_time_key,
         order_close_month_ending_dim_date_key,
         order_closed_flag,
         order_commissionable_dim_employee_key,
         order_discount_amount,
         order_open_dim_date_key,
         order_open_dim_time_key,
         order_refund_flag,
         order_sales_amount_gross,
         order_service_charge_amount,
         order_tax_amount,
         order_tax_removed_flag,
         order_tip_amount,
         order_void_flag,
         profit_center_id,
         dv_deleted,
         p_ig_it_trn_order_header_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ig_it_trn_order_header)
--Done!
end
