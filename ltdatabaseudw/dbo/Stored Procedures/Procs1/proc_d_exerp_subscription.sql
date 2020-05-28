CREATE PROC [dbo].[proc_d_exerp_subscription] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_subscription)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_subscription_insert') is not null drop table #p_exerp_subscription_insert
create table dbo.#p_exerp_subscription_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_subscription.p_exerp_subscription_id,
       p_exerp_subscription.bk_hash
  from dbo.p_exerp_subscription
 where p_exerp_subscription.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_subscription.dv_batch_id > @max_dv_batch_id
        or p_exerp_subscription.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_subscription.bk_hash,
       p_exerp_subscription.bk_hash dim_exerp_subscription_key,
       p_exerp_subscription.subscription_id subscription_id,
       case when p_exerp_subscription.bk_hash in('-997', '-998', '-999') then p_exerp_subscription.bk_hash
           when s_exerp_subscription.billed_until_date is null then '-998'
        else convert(varchar, s_exerp_subscription.billed_until_date, 112)    end billed_until_dim_date_key,
       case when p_exerp_subscription.bk_hash in('-997', '-998', '-999') then p_exerp_subscription.bk_hash
           when s_exerp_subscription.binding_end_date is null then '-998'
        else convert(varchar, s_exerp_subscription.binding_end_date, 112)    end binding_end_dim_date_key,
       s_exerp_subscription.binding_price binding_price,
       case when p_exerp_subscription.bk_hash in('-997', '-998', '-999') then p_exerp_subscription.bk_hash
           when s_exerp_subscription.creation_datetime is null then '-998'
        else convert(varchar, s_exerp_subscription.creation_datetime, 112)    end creation_dim_date_key,
       case when p_exerp_subscription.bk_hash in ('-997','-998','-999') then p_exerp_subscription.bk_hash
       when s_exerp_subscription.creation_datetime is null then '-998'
       else '1' + replace(substring(convert(varchar,s_exerp_subscription.creation_datetime,114), 1, 5),':','') end creation_dim_time_key,
       case when p_exerp_subscription.bk_hash in ('-997', '-998', '-999') then p_exerp_subscription.bk_hash
             when l_exerp_subscription.center_id is null then '-998'
             else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_subscription.center_id as int) as varchar(500)),'z#@$k%&P'))),2)
         end dim_club_key,
       case when p_exerp_subscription.bk_hash in('-997', '-998', '-999') then p_exerp_subscription.bk_hash
           when l_exerp_subscription.product_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_subscription.product_id as varchar(4000)),'z#@$k%&P'))),2)   end dim_exerp_product_key,
       case when p_exerp_subscription.bk_hash in('-997', '-998', '-999') then p_exerp_subscription.bk_hash 
              when ((l_exerp_subscription.person_id is null) OR (l_exerp_subscription.person_id LIKE '%e%') or (l_exerp_subscription.person_id LIKE '%OLDe%')
       	    or (len(l_exerp_subscription.person_id) > 9) or (d_exerp_person.person_type = 'STAFF' and l_exerp_subscription.person_id not LIKE '%e%') 
       		  or (d_exerp_person.person_type = 'STAFF') or (isnumeric(l_exerp_subscription.person_id) = 0)) then '-998' 
       		       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_subscription.person_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_mms_member_key,
       case when p_exerp_subscription.bk_hash in('-997', '-998', '-999') then p_exerp_subscription.bk_hash
           when s_exerp_subscription.end_date is null then '-998'
        else convert(varchar, s_exerp_subscription.end_date, 112)    end end_dim_date_key,
       s_exerp_subscription.ets ets,
       case when p_exerp_subscription.bk_hash in('-997', '-998', '-999') then p_exerp_subscription.bk_hash
           when l_exerp_subscription.extension_subscription_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_subscription.extension_subscription_id as varchar(4000)),'z#@$k%&P'))),2)   end extension_dim_exerp_subscription_key,
       case when p_exerp_subscription.bk_hash in('-997', '-998', '-999') then p_exerp_subscription.bk_hash
           when l_exerp_subscription.freeze_period_product_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_subscription.freeze_period_product_id as varchar(4000)),'z#@$k%&P'))),2)   end freeze_period_dim_exerp_product_key,
       s_exerp_subscription.period_count period_count,
       s_exerp_subscription.period_unit period_unit,
       s_exerp_subscription.price price,
       case when s_exerp_subscription.price_update_excluded = 1 then 'Y'        else 'N'  end price_update_excluded_flag,
       case when p_exerp_subscription.bk_hash in('-997', '-998', '-999') then p_exerp_subscription.bk_hash
           when l_exerp_subscription.reassign_subscription_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_subscription.reassign_subscription_id as varchar(4000)),'z#@$k%&P'))),2)   end reassign_dim_exerp_subscription_key,
       s_exerp_subscription.renewal_type renewal_type,
       case when s_exerp_subscription.requires_main = 1 then 'Y'        else 'N'  end requires_main_flag,
       case when p_exerp_subscription.bk_hash in('-997', '-998', '-999') then p_exerp_subscription.bk_hash
           when s_exerp_subscription.start_date is null then '-998'
        else convert(varchar, s_exerp_subscription.start_date, 112)    end start_dim_date_key,
       case when p_exerp_subscription.bk_hash in('-997', '-998', '-999') then p_exerp_subscription.bk_hash
           when s_exerp_subscription.stop_cancel_datetime is null then '-998'
        else convert(varchar, s_exerp_subscription.stop_cancel_datetime, 112)    end stop_cancel_dim_date_key,
       case when p_exerp_subscription.bk_hash in ('-997','-998','-999') then p_exerp_subscription.bk_hash
       when s_exerp_subscription.stop_cancel_datetime is null then '-998'
       else '1' + replace(substring(convert(varchar,s_exerp_subscription.stop_cancel_datetime,114), 1, 5),':','') end stop_cancel_dim_time_key,
       case when p_exerp_subscription.bk_hash in('-997', '-998', '-999') then p_exerp_subscription.bk_hash
           when s_exerp_subscription.stop_datetime is null then '-998'
        else convert(varchar, s_exerp_subscription.stop_datetime, 112)    end stop_dim_date_key,
       case when p_exerp_subscription.bk_hash in ('-997','-998','-999') then p_exerp_subscription.bk_hash
             when l_exerp_subscription.stop_person_id is null then '-998'
             else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(substring(l_exerp_subscription.stop_person_id, PATINDEX('%[0-9]%',l_exerp_subscription.stop_person_id), 500) as int) as varchar(500)),'z#@$k%&P'))),2)
         end stop_dim_employee_key,
       case when p_exerp_subscription.bk_hash in ('-997','-998','-999') then p_exerp_subscription.bk_hash
       when s_exerp_subscription.stop_datetime is null then '-998'
       else '1' + replace(substring(convert(varchar,s_exerp_subscription.stop_datetime,114), 1, 5),':','') end stop_dim_time_key,
       s_exerp_subscription.sub_state sub_state,
       s_exerp_subscription.state subscription_state,
       case when p_exerp_subscription.bk_hash in('-997', '-998', '-999') then p_exerp_subscription.bk_hash
           when l_exerp_subscription.transfer_subscription_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_subscription.transfer_subscription_id as varchar(4000)),'z#@$k%&P'))),2)   end transfer_dim_exerp_subscription_key,
       case when s_exerp_subscription.type_price_update_excluded = 1 then 'Y'        else 'N'  end type_price_update_excluded_flag,
       isnull(h_exerp_subscription.dv_deleted,0) dv_deleted,
       p_exerp_subscription.p_exerp_subscription_id,
       p_exerp_subscription.dv_batch_id,
       p_exerp_subscription.dv_load_date_time,
       p_exerp_subscription.dv_load_end_date_time
  from dbo.h_exerp_subscription
  join dbo.p_exerp_subscription
    on h_exerp_subscription.bk_hash = p_exerp_subscription.bk_hash
  join #p_exerp_subscription_insert
    on p_exerp_subscription.bk_hash = #p_exerp_subscription_insert.bk_hash
   and p_exerp_subscription.p_exerp_subscription_id = #p_exerp_subscription_insert.p_exerp_subscription_id
  join dbo.l_exerp_subscription
    on p_exerp_subscription.bk_hash = l_exerp_subscription.bk_hash
   and p_exerp_subscription.l_exerp_subscription_id = l_exerp_subscription.l_exerp_subscription_id
  join dbo.s_exerp_subscription
    on p_exerp_subscription.bk_hash = s_exerp_subscription.bk_hash
   and p_exerp_subscription.s_exerp_subscription_id = s_exerp_subscription.s_exerp_subscription_id
 left join 	d_exerp_person		on l_exerp_subscription.person_id = d_exerp_person.person_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_subscription
   where d_exerp_subscription.bk_hash in (select bk_hash from #p_exerp_subscription_insert)

  insert dbo.d_exerp_subscription(
             bk_hash,
             dim_exerp_subscription_key,
             subscription_id,
             billed_until_dim_date_key,
             binding_end_dim_date_key,
             binding_price,
             creation_dim_date_key,
             creation_dim_time_key,
             dim_club_key,
             dim_exerp_product_key,
             dim_mms_member_key,
             end_dim_date_key,
             ets,
             extension_dim_exerp_subscription_key,
             freeze_period_dim_exerp_product_key,
             period_count,
             period_unit,
             price,
             price_update_excluded_flag,
             reassign_dim_exerp_subscription_key,
             renewal_type,
             requires_main_flag,
             start_dim_date_key,
             stop_cancel_dim_date_key,
             stop_cancel_dim_time_key,
             stop_dim_date_key,
             stop_dim_employee_key,
             stop_dim_time_key,
             sub_state,
             subscription_state,
             transfer_dim_exerp_subscription_key,
             type_price_update_excluded_flag,
             deleted_flag,
             p_exerp_subscription_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_exerp_subscription_key,
         subscription_id,
         billed_until_dim_date_key,
         binding_end_dim_date_key,
         binding_price,
         creation_dim_date_key,
         creation_dim_time_key,
         dim_club_key,
         dim_exerp_product_key,
         dim_mms_member_key,
         end_dim_date_key,
         ets,
         extension_dim_exerp_subscription_key,
         freeze_period_dim_exerp_product_key,
         period_count,
         period_unit,
         price,
         price_update_excluded_flag,
         reassign_dim_exerp_subscription_key,
         renewal_type,
         requires_main_flag,
         start_dim_date_key,
         stop_cancel_dim_date_key,
         stop_cancel_dim_time_key,
         stop_dim_date_key,
         stop_dim_employee_key,
         stop_dim_time_key,
         sub_state,
         subscription_state,
         transfer_dim_exerp_subscription_key,
         type_price_update_excluded_flag,
         dv_deleted,
         p_exerp_subscription_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_subscription)
--Done!
end
