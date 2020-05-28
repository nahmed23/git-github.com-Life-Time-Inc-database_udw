CREATE PROC [dbo].[proc_d_mms_mms_tran] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_mms_tran)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_mms_tran_insert') is not null drop table #p_mms_mms_tran_insert
create table dbo.#p_mms_mms_tran_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_mms_tran.p_mms_mms_tran_id,
       p_mms_mms_tran.bk_hash
  from dbo.p_mms_mms_tran
 where p_mms_mms_tran.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_mms_tran.dv_batch_id > @max_dv_batch_id
        or p_mms_mms_tran.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_mms_tran.bk_hash,
       p_mms_mms_tran.bk_hash fact_mms_sales_transaction_key,
       p_mms_mms_tran.mms_tran_id mms_tran_id,
       l_mms_mms_tran.club_id club_id,
       case when p_mms_mms_tran.bk_hash in ('-997', '-998', '-999') then p_mms_mms_tran.bk_hash
            when l_mms_mms_tran.club_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_mms_tran.club_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end dim_club_key,
       case when p_mms_mms_tran.bk_hash in ('-997', '-998', '-999') then p_mms_mms_tran.bk_hash
            when l_mms_mms_tran.drawer_activity_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_mms_tran.drawer_activity_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end dim_mms_drawer_activity_key,
       case when p_mms_mms_tran.bk_hash in ('-997', '-998', '-999') then p_mms_mms_tran.bk_hash
            when l_mms_mms_tran.member_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_mms_tran.member_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end dim_mms_member_key,
       case when p_mms_mms_tran.bk_hash in ('-997', '-998', '-999') then p_mms_mms_tran.bk_hash
            when l_mms_mms_tran.membership_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_mms_tran.membership_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end dim_mms_membership_key,
       case when p_mms_mms_tran.bk_hash in ('-997', '-998', '-999') then p_mms_mms_tran.bk_hash
            when l_mms_mms_tran.reimbursement_program_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_mms_tran.reimbursement_program_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end dim_mms_reimbursement_program_key,
       case when p_mms_mms_tran.bk_hash in ('-997', '-998', '-999') then p_mms_mms_tran.bk_hash
            when l_mms_mms_tran.reason_code_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_mms_tran.reason_code_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end dim_mms_transaction_reason_key,
       s_mms_mms_tran.domain_name domain_name,
       l_mms_mms_tran.employee_id employee_id,
       case when p_mms_mms_tran.bk_hash in('-997', '-998', '-999') then p_mms_mms_tran.bk_hash
          when s_mms_mms_tran.inserted_date_time is null then '-998'
       else convert(varchar, s_mms_mms_tran.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_mms_mms_tran.bk_hash in ('-997','-998','-999') then p_mms_mms_tran.bk_hash
       when s_mms_mms_tran.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_mms_tran.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       l_mms_mms_tran.member_id member_id,
       case when l_mms_mms_tran.val_tran_type_id = 4 then 'Y'
            else 'N'
        end membership_adjustment_flag,
       case when l_mms_mms_tran.val_tran_type_id = 1 then 'Y'
            else 'N'
        end membership_charge_flag,
       l_mms_mms_tran.membership_id membership_id,
       case when p_mms_mms_tran.bk_hash in ('-997','-998','-999') then p_mms_mms_tran.bk_hash
             when l_mms_mms_tran.tran_voided_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_mms_tran.tran_voided_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end mms_tran_voided_bk_hash,
       case when p_mms_mms_tran.bk_hash in ('-997', '-998', '-999') then p_mms_mms_tran.bk_hash
            when dim_date.month_ending_dim_date_key is null then '-998'
            else dim_date.month_ending_dim_date_key
        end month_ending_post_dim_date_key,
       case when p_mms_mms_tran.bk_hash in ('-997', '-998', '-999') then 'USD'
            when r_mms_val_currency_code.currency_code is null then 'USD'
            else r_mms_val_currency_code.currency_code
        end original_currency_code,
       case when p_mms_mms_tran.bk_hash in ('-997', '-998', '-999') then p_mms_mms_tran.bk_hash
            when l_mms_mms_tran.original_mms_tran_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_mms_tran.original_mms_tran_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end original_fact_mms_sales_transaction_key,
       l_mms_mms_tran.original_mms_tran_id original_mms_tran_id,
       isnull(s_mms_mms_tran.pos_amount,0) pos_amount,
       case when l_mms_mms_tran.val_tran_type_id = 3 then 'Y'
            else 'N'
        end pos_flag,
       s_mms_mms_tran.post_date_time post_date_time,
       case when p_mms_mms_tran.bk_hash in ('-997', '-998', '-999') then p_mms_mms_tran.bk_hash
            when s_mms_mms_tran.post_date_time is null then '-998'
            else convert(varchar, s_mms_mms_tran.post_date_time, 112)
        end post_dim_date_key,
       case when p_mms_mms_tran.bk_hash in ('-997', '-998', '-999') then p_mms_mms_tran.bk_hash
            when s_mms_mms_tran.post_date_time is null then '-998'
            else '1' + replace(substring(convert(varchar,s_mms_mms_tran.post_date_time,114), 1, 5),':','')
        end post_dim_time_key,
       isnull(s_mms_mms_tran.receipt_comment, '') receipt_comment,
       isnull(s_mms_mms_tran.receipt_number, '') receipt_number,
       case when l_mms_mms_tran.val_tran_type_id = 5 then 'Y'
            else 'N'
        end refund_flag,
       case when s_mms_mms_tran.reverse_tran_flag = 1 then 'Y'
            else 'N'
        end reversal_flag,
       case when p_mms_mms_tran.bk_hash in ('-997', '-998', '-999') then p_mms_mms_tran.bk_hash
            when l_mms_mms_tran.employee_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_mms_tran.employee_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end sales_entered_dim_employee_key,
       s_mms_mms_tran.tran_amount tran_amount,
       s_mms_mms_tran.tran_date tran_date,
       case when p_mms_mms_tran.bk_hash in ('-997', '-998', '-999') then p_mms_mms_tran.bk_hash
       when s_mms_mms_tran.tran_date is null then '-998' else convert(varchar, s_mms_mms_tran.tran_date, 112) end tran_dim_date_key,
       l_mms_mms_tran.tran_voided_id tran_voided_id,
       case when s_mms_mms_tran.tran_edited_flag = 1 then 'Y'
            else 'N'
        end transaction_edited_flag,
       case when p_mms_mms_tran.bk_hash in ('-997', '-998', '-999') then p_mms_mms_tran.bk_hash
       when l_mms_mms_tran.tran_edited_employee_id is null then '-998' else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_mms_tran.tran_edited_employee_id as int) as varchar(500)),'z#@$k%&P'))),2) end transaction_entered_dim_employee_key,
       s_mms_mms_tran.updated_date_time updated_date_time,
       l_mms_mms_tran.val_currency_code_id val_currency_code_id,
       l_mms_mms_tran.val_tran_type_id val_tran_type_id,
       case when l_mms_mms_tran.tran_voided_id is null then 'N'
            else 'Y'
        end voided_flag,
       isnull(h_mms_mms_tran.dv_deleted,0) dv_deleted,
       p_mms_mms_tran.p_mms_mms_tran_id,
       p_mms_mms_tran.dv_batch_id,
       p_mms_mms_tran.dv_load_date_time,
       p_mms_mms_tran.dv_load_end_date_time
  from dbo.h_mms_mms_tran
  join dbo.p_mms_mms_tran
    on h_mms_mms_tran.bk_hash = p_mms_mms_tran.bk_hash
  join #p_mms_mms_tran_insert
    on p_mms_mms_tran.bk_hash = #p_mms_mms_tran_insert.bk_hash
   and p_mms_mms_tran.p_mms_mms_tran_id = #p_mms_mms_tran_insert.p_mms_mms_tran_id
  join dbo.l_mms_mms_tran
    on p_mms_mms_tran.bk_hash = l_mms_mms_tran.bk_hash
   and p_mms_mms_tran.l_mms_mms_tran_id = l_mms_mms_tran.l_mms_mms_tran_id
  join dbo.s_mms_mms_tran
    on p_mms_mms_tran.bk_hash = s_mms_mms_tran.bk_hash
   and p_mms_mms_tran.s_mms_mms_tran_id = s_mms_mms_tran.s_mms_mms_tran_id
  left join r_mms_val_currency_code
    on l_mms_mms_tran.val_currency_code_id = r_mms_val_currency_code.val_currency_code_id
   and r_mms_val_currency_code.dv_load_end_date_time = 'dec 31, 9999'
  left join dim_date
    on convert(varchar, s_mms_mms_tran.post_date_time, 112) = dim_date.dim_date_key


-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_mms_tran
   where d_mms_mms_tran.bk_hash in (select bk_hash from #p_mms_mms_tran_insert)

  insert dbo.d_mms_mms_tran(
             bk_hash,
             fact_mms_sales_transaction_key,
             mms_tran_id,
             club_id,
             dim_club_key,
             dim_mms_drawer_activity_key,
             dim_mms_member_key,
             dim_mms_membership_key,
             dim_mms_reimbursement_program_key,
             dim_mms_transaction_reason_key,
             domain_name,
             employee_id,
             inserted_dim_date_key,
             inserted_dim_time_key,
             member_id,
             membership_adjustment_flag,
             membership_charge_flag,
             membership_id,
             mms_tran_voided_bk_hash,
             month_ending_post_dim_date_key,
             original_currency_code,
             original_fact_mms_sales_transaction_key,
             original_mms_tran_id,
             pos_amount,
             pos_flag,
             post_date_time,
             post_dim_date_key,
             post_dim_time_key,
             receipt_comment,
             receipt_number,
             refund_flag,
             reversal_flag,
             sales_entered_dim_employee_key,
             tran_amount,
             tran_date,
             tran_dim_date_key,
             tran_voided_id,
             transaction_edited_flag,
             transaction_entered_dim_employee_key,
             updated_date_time,
             val_currency_code_id,
             val_tran_type_id,
             voided_flag,
             deleted_flag,
             p_mms_mms_tran_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_mms_sales_transaction_key,
         mms_tran_id,
         club_id,
         dim_club_key,
         dim_mms_drawer_activity_key,
         dim_mms_member_key,
         dim_mms_membership_key,
         dim_mms_reimbursement_program_key,
         dim_mms_transaction_reason_key,
         domain_name,
         employee_id,
         inserted_dim_date_key,
         inserted_dim_time_key,
         member_id,
         membership_adjustment_flag,
         membership_charge_flag,
         membership_id,
         mms_tran_voided_bk_hash,
         month_ending_post_dim_date_key,
         original_currency_code,
         original_fact_mms_sales_transaction_key,
         original_mms_tran_id,
         pos_amount,
         pos_flag,
         post_date_time,
         post_dim_date_key,
         post_dim_time_key,
         receipt_comment,
         receipt_number,
         refund_flag,
         reversal_flag,
         sales_entered_dim_employee_key,
         tran_amount,
         tran_date,
         tran_dim_date_key,
         tran_voided_id,
         transaction_edited_flag,
         transaction_entered_dim_employee_key,
         updated_date_time,
         val_currency_code_id,
         val_tran_type_id,
         voided_flag,
         dv_deleted,
         p_mms_mms_tran_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_mms_tran)
--Done!
end
