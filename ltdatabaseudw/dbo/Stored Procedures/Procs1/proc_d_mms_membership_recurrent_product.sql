CREATE PROC [dbo].[proc_d_mms_membership_recurrent_product] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_membership_recurrent_product)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_membership_recurrent_product_insert') is not null drop table #p_mms_membership_recurrent_product_insert
create table dbo.#p_mms_membership_recurrent_product_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_membership_recurrent_product.p_mms_membership_recurrent_product_id,
       p_mms_membership_recurrent_product.bk_hash
  from dbo.p_mms_membership_recurrent_product
 where p_mms_membership_recurrent_product.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_membership_recurrent_product.dv_batch_id > @max_dv_batch_id
        or p_mms_membership_recurrent_product.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_membership_recurrent_product.bk_hash,
       p_mms_membership_recurrent_product.bk_hash fact_mms_membership_recurrent_product_key,
       p_mms_membership_recurrent_product.membership_recurrent_product_id membership_recurrent_product_id,
       case when p_mms_membership_recurrent_product.bk_hash in ('-997', '-998', '-999') then p_mms_membership_recurrent_product.bk_hash   
            when s_mms_membership_recurrent_product.activation_date is null then '-998'  
              else convert(varchar, s_mms_membership_recurrent_product.activation_date, 112)
       end activation_dim_date_key,
       case when p_mms_membership_recurrent_product.bk_hash in ('-997', '-998', '-999') then p_mms_membership_recurrent_product.bk_hash   
            when s_mms_membership_recurrent_product.cancellation_request_date is null then '-998'
              else convert(varchar, s_mms_membership_recurrent_product.cancellation_request_date, 112)
       	   end cancellation_request_dim_date_key,
       l_mms_membership_recurrent_product.club_id club_id,
       s_mms_membership_recurrent_product.comments comments,
       case when p_mms_membership_recurrent_product.bk_hash in ('-997','-998','-999') then p_mms_membership_recurrent_product.bk_hash
       when l_mms_membership_recurrent_product.commission_employee_id is null then '-998'
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_recurrent_product.commission_employee_id as int) as varchar(500)),'z#@$k%&P'))),2)   end commission_dim_mms_employee_key,
       l_mms_membership_recurrent_product.commission_employee_id commission_employee_id,
       s_mms_membership_recurrent_product.created_date_time_zone created_date_time_zone,
       case when p_mms_membership_recurrent_product.bk_hash in('-997', '-998', '-999') then p_mms_membership_recurrent_product.bk_hash
           when s_mms_membership_recurrent_product.created_date_time is null then '-998'
        else convert(varchar, s_mms_membership_recurrent_product.created_date_time, 112)    end created_dim_date_key,
       case when p_mms_membership_recurrent_product.bk_hash in ('-997','-998','-999') then p_mms_membership_recurrent_product.bk_hash
       when s_mms_membership_recurrent_product.created_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_membership_recurrent_product.created_date_time,114), 1, 5),':','') end created_dim_time_key,
       case when  p_mms_membership_recurrent_product.bk_hash in ('-997','-998','-999') then p_mms_membership_recurrent_product.bk_hash
            when l_mms_membership_recurrent_product.club_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_recurrent_product.club_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end dim_club_key,
       case when  p_mms_membership_recurrent_product.bk_hash in ('-997','-998','-999') then p_mms_membership_recurrent_product.bk_hash
            when l_mms_membership_recurrent_product.member_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_recurrent_product.member_id as int) as varchar(500)),'z#@$k%&P'))),2)
       	end dim_mms_member_key,
       case when p_mms_membership_recurrent_product.bk_hash in ('-997','-998','-999') then p_mms_membership_recurrent_product.bk_hash
       when l_mms_membership_recurrent_product.pricing_discount_id is null then '-998'
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_recurrent_product.pricing_discount_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_mms_pricing_discount_key,
       case when  p_mms_membership_recurrent_product.bk_hash in ('-997','-998','-999') then p_mms_membership_recurrent_product.bk_hash
            when l_mms_membership_recurrent_product.product_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_recurrent_product.product_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end dim_mms_product_key,
       case when s_mms_membership_recurrent_product.display_only_flag = 1  then 'Y' else 'N' end display_only_flag,
       case
       when p_mms_membership_recurrent_product.bk_hash in ('-997', '-998', '-999') then p_mms_membership_recurrent_product.bk_hash   
            when s_mms_membership_recurrent_product.product_hold_end_date is null then '-998'   
              else convert(varchar, s_mms_membership_recurrent_product.product_hold_end_date, 112)
       end hold_end_dim_date_key,
       case
       when p_mms_membership_recurrent_product.bk_hash in ('-997', '-998', '-999') then p_mms_membership_recurrent_product.bk_hash   
            when s_mms_membership_recurrent_product.product_hold_begin_date is null then '-998'   
              else convert(varchar,s_mms_membership_recurrent_product.product_hold_begin_date, 112)
       end hold_start_dim_date_key,
       case when p_mms_membership_recurrent_product.bk_hash in('-997', '-998', '-999') then p_mms_membership_recurrent_product.bk_hash
           when s_mms_membership_recurrent_product.inserted_date_time is null then '-998'
        else convert(varchar, s_mms_membership_recurrent_product.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_mms_membership_recurrent_product.bk_hash in ('-997','-998','-999') then p_mms_membership_recurrent_product.bk_hash
       when s_mms_membership_recurrent_product.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_membership_recurrent_product.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       case
       when p_mms_membership_recurrent_product.bk_hash in ('-997', '-998', '-999') then p_mms_membership_recurrent_product.bk_hash   
            when s_mms_membership_recurrent_product.product_assessed_date_time is null then '-998'   
              else convert(varchar, s_mms_membership_recurrent_product.product_assessed_date_time, 112)
       end last_assessment_dim_date_key,
       s_mms_membership_recurrent_product.last_updated_date_time_zone last_updated_date_time_zone,
       case when p_mms_membership_recurrent_product.bk_hash in('-997', '-998', '-999') then p_mms_membership_recurrent_product.bk_hash
           when s_mms_membership_recurrent_product.last_updated_date_time is null then '-998'
        else convert(varchar, s_mms_membership_recurrent_product.last_updated_date_time, 112)    end last_updated_dim_date_key,
       case when p_mms_membership_recurrent_product.bk_hash in ('-997','-998','-999') then p_mms_membership_recurrent_product.bk_hash
       when l_mms_membership_recurrent_product.last_updated_employee_id is null then '-998'
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_recurrent_product.last_updated_employee_id as int) as varchar(500)),'z#@$k%&P'))),2)   end last_updated_dim_employee_key,
       case when p_mms_membership_recurrent_product.bk_hash in ('-997','-998','-999') then p_mms_membership_recurrent_product.bk_hash
       when s_mms_membership_recurrent_product.last_updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_membership_recurrent_product.last_updated_date_time,114), 1, 5),':','') end last_updated_dim_time_key,
       l_mms_membership_recurrent_product.last_updated_employee_id last_updated_employee_id,
       l_mms_membership_recurrent_product.membership_id membership_id,
       s_mms_membership_recurrent_product.number_of_sessions number_of_sessions,
       s_mms_membership_recurrent_product.price price,
       s_mms_membership_recurrent_product.price_per_session price_per_session,
       l_mms_membership_recurrent_product.pricing_discount_id pricing_discount_id,
       s_mms_membership_recurrent_product.promotion_code promotion_code,
       case
       when p_mms_membership_recurrent_product.bk_hash in ('-997','-998','-999') then p_mms_membership_recurrent_product.bk_hash
            when  l_mms_membership_recurrent_product.val_recurrent_product_source_id is null then '-998'
            else 'r_mms_val_recurrent_product_source_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_recurrent_product.val_recurrent_product_source_id as int) as varchar(500)),'z#@$k%&P'))),2)
       end recurrent_product_source_dim_description_key,
       s_mms_membership_recurrent_product.retail_price retail_price,
       s_mms_membership_recurrent_product.retail_price_per_session retail_price_per_session,
       case when s_mms_membership_recurrent_product.sold_not_serviced_flag = 1  then 'Y' else 'N' end sold_not_serviced_flag,
       case
       when p_mms_membership_recurrent_product.bk_hash in ('-997', '-998', '-999') then p_mms_membership_recurrent_product.bk_hash   
            when s_mms_membership_recurrent_product.termination_date is null then '-998'   
              else convert(varchar, s_mms_membership_recurrent_product.termination_date, 112)
       end termination_dim_date_key,
       case
       when p_mms_membership_recurrent_product.bk_hash in ('-997','-998','-999') then p_mms_membership_recurrent_product.bk_hash
            when  l_mms_membership_recurrent_product.val_recurrent_product_termination_reason_id is null then '-998'
            else 'r_mms_val_termination_reason_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_membership_recurrent_product.val_recurrent_product_termination_reason_id as int) as varchar(500)),'z#@$k%&P'))),2)
       end termination_reason_dim_description_key,
       case when p_mms_membership_recurrent_product.bk_hash in('-997', '-998', '-999') then p_mms_membership_recurrent_product.bk_hash
           when s_mms_membership_recurrent_product.updated_date_time is null then '-998'
        else convert(varchar, s_mms_membership_recurrent_product.updated_date_time, 112)    end updated_dim_date_key,
       case when p_mms_membership_recurrent_product.bk_hash in ('-997','-998','-999') then p_mms_membership_recurrent_product.bk_hash
       when s_mms_membership_recurrent_product.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_membership_recurrent_product.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       case when p_mms_membership_recurrent_product.bk_hash in('-997', '-998', '-999') then p_mms_membership_recurrent_product.bk_hash
           when s_mms_membership_recurrent_product.utc_created_date_time is null then '-998'
        else convert(varchar, s_mms_membership_recurrent_product.utc_created_date_time, 112)    end utc_created_dim_date_key,
       case when p_mms_membership_recurrent_product.bk_hash in ('-997','-998','-999') then p_mms_membership_recurrent_product.bk_hash
       when s_mms_membership_recurrent_product.utc_created_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_membership_recurrent_product.utc_created_date_time,114), 1, 5),':','') end utc_created_dim_time_key,
       case when p_mms_membership_recurrent_product.bk_hash in('-997', '-998', '-999') then p_mms_membership_recurrent_product.bk_hash
           when s_mms_membership_recurrent_product.utc_last_updated_date_time is null then '-998'
        else convert(varchar, s_mms_membership_recurrent_product.utc_last_updated_date_time, 112)    end utc_last_updated_dim_date_key,
       case when p_mms_membership_recurrent_product.bk_hash in ('-997','-998','-999') then p_mms_membership_recurrent_product.bk_hash
       when s_mms_membership_recurrent_product.utc_last_updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_membership_recurrent_product.utc_last_updated_date_time,114), 1, 5),':','') end utc_last_updated_dim_time_key,
       l_mms_membership_recurrent_product.val_assessment_day_id val_assessment_day_id,
       l_mms_membership_recurrent_product.val_discount_reason_id val_discount_reason_id,
       isnull(cast(l_mms_membership_recurrent_product.val_recurrent_product_source_id as int),-998) val_recurrent_product_source_id,
       isnull(l_mms_membership_recurrent_product.val_recurrent_product_termination_reason_id,-998) val_recurrent_product_termination_reason_id,
       isnull(h_mms_membership_recurrent_product.dv_deleted,0) dv_deleted,
       p_mms_membership_recurrent_product.p_mms_membership_recurrent_product_id,
       p_mms_membership_recurrent_product.dv_batch_id,
       p_mms_membership_recurrent_product.dv_load_date_time,
       p_mms_membership_recurrent_product.dv_load_end_date_time
  from dbo.h_mms_membership_recurrent_product
  join dbo.p_mms_membership_recurrent_product
    on h_mms_membership_recurrent_product.bk_hash = p_mms_membership_recurrent_product.bk_hash
  join #p_mms_membership_recurrent_product_insert
    on p_mms_membership_recurrent_product.bk_hash = #p_mms_membership_recurrent_product_insert.bk_hash
   and p_mms_membership_recurrent_product.p_mms_membership_recurrent_product_id = #p_mms_membership_recurrent_product_insert.p_mms_membership_recurrent_product_id
  join dbo.l_mms_membership_recurrent_product
    on p_mms_membership_recurrent_product.bk_hash = l_mms_membership_recurrent_product.bk_hash
   and p_mms_membership_recurrent_product.l_mms_membership_recurrent_product_id = l_mms_membership_recurrent_product.l_mms_membership_recurrent_product_id
  join dbo.s_mms_membership_recurrent_product
    on p_mms_membership_recurrent_product.bk_hash = s_mms_membership_recurrent_product.bk_hash
   and p_mms_membership_recurrent_product.s_mms_membership_recurrent_product_id = s_mms_membership_recurrent_product.s_mms_membership_recurrent_product_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_membership_recurrent_product
   where d_mms_membership_recurrent_product.bk_hash in (select bk_hash from #p_mms_membership_recurrent_product_insert)

  insert dbo.d_mms_membership_recurrent_product(
             bk_hash,
             fact_mms_membership_recurrent_product_key,
             membership_recurrent_product_id,
             activation_dim_date_key,
             cancellation_request_dim_date_key,
             club_id,
             comments,
             commission_dim_mms_employee_key,
             commission_employee_id,
             created_date_time_zone,
             created_dim_date_key,
             created_dim_time_key,
             dim_club_key,
             dim_mms_member_key,
             dim_mms_pricing_discount_key,
             dim_mms_product_key,
             display_only_flag,
             hold_end_dim_date_key,
             hold_start_dim_date_key,
             inserted_dim_date_key,
             inserted_dim_time_key,
             last_assessment_dim_date_key,
             last_updated_date_time_zone,
             last_updated_dim_date_key,
             last_updated_dim_employee_key,
             last_updated_dim_time_key,
             last_updated_employee_id,
             membership_id,
             number_of_sessions,
             price,
             price_per_session,
             pricing_discount_id,
             promotion_code,
             recurrent_product_source_dim_description_key,
             retail_price,
             retail_price_per_session,
             sold_not_serviced_flag,
             termination_dim_date_key,
             termination_reason_dim_description_key,
             updated_dim_date_key,
             updated_dim_time_key,
             utc_created_dim_date_key,
             utc_created_dim_time_key,
             utc_last_updated_dim_date_key,
             utc_last_updated_dim_time_key,
             val_assessment_day_id,
             val_discount_reason_id,
             val_recurrent_product_source_id,
             val_recurrent_product_termination_reason_id,
             deleted_flag,
             p_mms_membership_recurrent_product_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_mms_membership_recurrent_product_key,
         membership_recurrent_product_id,
         activation_dim_date_key,
         cancellation_request_dim_date_key,
         club_id,
         comments,
         commission_dim_mms_employee_key,
         commission_employee_id,
         created_date_time_zone,
         created_dim_date_key,
         created_dim_time_key,
         dim_club_key,
         dim_mms_member_key,
         dim_mms_pricing_discount_key,
         dim_mms_product_key,
         display_only_flag,
         hold_end_dim_date_key,
         hold_start_dim_date_key,
         inserted_dim_date_key,
         inserted_dim_time_key,
         last_assessment_dim_date_key,
         last_updated_date_time_zone,
         last_updated_dim_date_key,
         last_updated_dim_employee_key,
         last_updated_dim_time_key,
         last_updated_employee_id,
         membership_id,
         number_of_sessions,
         price,
         price_per_session,
         pricing_discount_id,
         promotion_code,
         recurrent_product_source_dim_description_key,
         retail_price,
         retail_price_per_session,
         sold_not_serviced_flag,
         termination_dim_date_key,
         termination_reason_dim_description_key,
         updated_dim_date_key,
         updated_dim_time_key,
         utc_created_dim_date_key,
         utc_created_dim_time_key,
         utc_last_updated_dim_date_key,
         utc_last_updated_dim_time_key,
         val_assessment_day_id,
         val_discount_reason_id,
         val_recurrent_product_source_id,
         val_recurrent_product_termination_reason_id,
         dv_deleted,
         p_mms_membership_recurrent_product_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_membership_recurrent_product)
--Done!
end
