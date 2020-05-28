CREATE PROC [dbo].[proc_dim_club_currency_code] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from dim_club_currency_code)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#dim_club_currency_code') is not null drop table #dim_club_currency_code
create table dbo.#dim_club_currency_code with(distribution=hash(dummy_bk_hash_key), location=user_db, heap) as
select d_mms_club.bk_hash as dummy_bk_hash_key, 
		case when d_mms_club.club_id is null then '-998'
	    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,d_mms_club.club_id),'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull(isnull(r_mms_val_currency_code.currency_code,'USD'),'z#@$k%&P'))),2)
       end dim_club_currency_code_key,
       d_mms_club.club_id as club_id , 
       d_mms_club.dim_club_key as dim_club_key,
	   case when d_mms_club.bk_hash in ('-999','-997','-998') then null
	   else
	   club_gl_acct.gl_cash_entry_company_name 
	   end as  gl_cash_entry_company_name,
	   case when d_mms_club.bk_hash in ('-999','-997','-998') then null
	   else
	   club_gl_acct.gl_cash_entry_account 
	   end as  gl_cash_entry_account,
	   case when d_mms_club.bk_hash in ('-999','-997','-998') then null
	   else
	   club_gl_acct.gl_receivables_entry_account 
	   end as  gl_receivables_entry_account,
	   case when d_mms_club.bk_hash in ('-999','-997','-998') then null
	   else
	   club_gl_acct.gl_cash_entry_cash_sub_account 
	   end as  gl_cash_entry_cash_sub_account,
	   case when d_mms_club.bk_hash in ('-999','-997','-998') then null
	   else
	   club_gl_acct.gl_cash_entry_credit_card_sub_account 
	   end as  gl_cash_entry_credit_card_sub_account,
	   case when d_mms_club.bk_hash in ('-999','-997','-998') then null
	   else
	   club_gl_acct.gl_receivables_entry_sub_account 
	   end as  gl_receivables_entry_sub_account,
	   case when d_mms_club.bk_hash in ('-999','-997','-998') then null
	   else
	   club_gl_acct.gl_receivables_entry_company_name 
	   end as  gl_receivables_entry_company_name,
       r_mms_val_currency_code.currency_code as currency_code,
       case when club_gl_acct.dv_load_date_time >= isnull(r_mms_val_currency_code.dv_load_date_time,'jan 1, 1753')
            then club_gl_acct.dv_load_date_time
            else r_mms_val_currency_code.dv_load_date_time
        end dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       case when club_gl_acct.dv_batch_id >= isnull(r_mms_val_currency_code.dv_batch_id,-1)
            then club_gl_acct.dv_batch_id
            else r_mms_val_currency_code.dv_batch_id
        end dv_batch_id,
       getdate() dv_inserted_date_time,
       suser_sname() dv_insert_user
   from 
       d_mms_club d_mms_club
  left join d_mms_club_gl_account club_gl_acct
   on d_mms_club.club_id=club_gl_acct.club_id
  left join r_mms_val_currency_code r_mms_val_currency_code
   on club_gl_acct.val_currency_code_id=r_mms_val_currency_code.val_currency_code_id
  and r_mms_val_currency_code.dv_load_end_date_time = 'dec 31, 9999'
  where d_mms_club.dv_batch_id >= @load_dv_batch_id

union

select d_mms_club.bk_hash as dummy_bk_hash_key,
		case when d_mms_club.club_id is null then '-998'
	    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,d_mms_club.club_id),'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull(isnull(r_mms_val_currency_code.currency_code,'USD'),'z#@$k%&P'))),2)
        end dim_club_currency_code_key,  
       d_mms_club.club_id as club_id , 
       d_mms_club.dim_club_key as dim_club_key,
	   case when d_mms_club.bk_hash in ('-999','-997','-998') then null
	   else
	   club_gl_acct.gl_cash_entry_company_name 
	   end as  gl_cash_entry_company_name,
	   case when d_mms_club.bk_hash in ('-999','-997','-998') then null
	   else
	   club_gl_acct.gl_cash_entry_account 
	   end as  gl_cash_entry_account,
	   case when d_mms_club.bk_hash in ('-999','-997','-998') then null
	   else
	   club_gl_acct.gl_receivables_entry_account 
	   end as  gl_receivables_entry_account,
	   case when d_mms_club.bk_hash in ('-999','-997','-998') then null
	   else
	   club_gl_acct.gl_cash_entry_cash_sub_account 
	   end as  gl_cash_entry_cash_sub_account,
	   case when d_mms_club.bk_hash in ('-999','-997','-998') then null
	   else
	   club_gl_acct.gl_cash_entry_credit_card_sub_account 
	   end as  gl_cash_entry_credit_card_sub_account,
	   case when d_mms_club.bk_hash in ('-999','-997','-998') then null
	   else
	   club_gl_acct.gl_receivables_entry_sub_account 
	   end as  gl_receivables_entry_sub_account,
	   case when d_mms_club.bk_hash in ('-999','-997','-998') then null
	   else
	   club_gl_acct.gl_receivables_entry_company_name 
	   end as  gl_receivables_entry_company_name,
       r_mms_val_currency_code.currency_code as currency_code,
       case when club_gl_acct.dv_load_date_time >= isnull(r_mms_val_currency_code.dv_load_date_time,'jan 1, 1753')
            then club_gl_acct.dv_load_date_time
            else r_mms_val_currency_code.dv_load_date_time
        end dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       case when club_gl_acct.dv_batch_id >= isnull(r_mms_val_currency_code.dv_batch_id,-1)
            then club_gl_acct.dv_batch_id
            else r_mms_val_currency_code.dv_batch_id
        end dv_batch_id,
       getdate() dv_inserted_date_time,
       suser_sname() dv_insert_user
  from 
       d_mms_club_gl_account club_gl_acct
  join d_mms_club d_mms_club
   on d_mms_club.club_id=club_gl_acct.club_id
  left join r_mms_val_currency_code r_mms_val_currency_code
   on club_gl_acct.val_currency_code_id=r_mms_val_currency_code.val_currency_code_id
  and r_mms_val_currency_code.dv_load_end_date_time = 'dec 31, 9999'
  where club_gl_acct.dv_batch_id >= @load_dv_batch_id
	 
	 
-- Delete and re-insert as a single transaction
--   Delete records from the table that exist
--   Insert records from records from current and missing batches

begin tran

  delete dbo.dim_club_currency_code
   where dummy_bk_hash_key in (select dummy_bk_hash_key from dbo.#dim_club_currency_code) 

  insert into dim_club_currency_code
  (dummy_bk_hash_key,
   dim_club_currency_code_key,
   club_id,
   currency_code,
   dim_club_key,
   gl_cash_entry_company_name,
   gl_cash_entry_account,
   gl_cash_entry_cash_sub_account,
   gl_receivables_entry_company_name,
   gl_cash_entry_credit_card_sub_account,
   gl_receivables_entry_account,
   gl_receivables_entry_sub_account,
   dv_load_date_time,
   dv_load_end_date_time,
   dv_batch_id,
   dv_inserted_date_time,
   dv_insert_user
  )
   select dummy_bk_hash_key,
   dim_club_currency_code_key,
   club_id,
   currency_code,
   dim_club_key,
   gl_cash_entry_company_name,
   gl_cash_entry_account,
   gl_cash_entry_cash_sub_account,
   gl_receivables_entry_company_name,
   gl_cash_entry_credit_card_sub_account,
   gl_receivables_entry_account,
   gl_receivables_entry_sub_account,
   dv_load_date_time,
   dv_load_end_date_time,
   dv_batch_id,
   dv_inserted_date_time,
   dv_insert_user
   from #dim_club_currency_code

commit tran

end
