CREATE PROC [dbo].[proc_fact_mms_club_pos_drawer_activity_amount] @dv_batch_id [varchar](500) AS
Begin
set xact_abort on
set nocount on


-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
if object_id('tempdb..#dv_batch_id') is not null drop table #dv_batch_id
create table dbo.#dv_batch_id with(distribution=round_robin, location=user_db, heap) as
select 	isnull(max(dv_batch_id),-2) max_dv_batch_id,
		@dv_batch_id as current_dv_batch_id
		from dbo.fact_mms_club_pos_drawer_activity_amount
	
	--select * from #dv_batch_id

--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~STEP - 1~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~	
--For a dimension record, the complete record needs to be rebuilt for a change in any field in any of the participating tables, Hence:
-----STEP 1: Collecting Business Keys from the base table - that are corresponding to the changed Recs from all the participating tables & itself
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if object_id('tempdb..#p_drawer_activity_amount_id') is not null drop table #p_drawer_activity_amount_id
		create table dbo.#p_drawer_activity_amount_id with(distribution=hash(drawer_activity_amount_id), location=user_db, heap) as	
				select p_mms_drawer_activity_amount.drawer_activity_amount_id
					   from p_mms_drawer_activity_amount 
				join l_mms_drawer_activity_amount
				on p_mms_drawer_activity_amount.l_mms_drawer_activity_amount_id = l_mms_drawer_activity_amount.l_mms_drawer_activity_amount_id
				join #dv_batch_id 
				on p_mms_drawer_activity_amount.dv_batch_id > #dv_batch_id.max_dv_batch_id
				or p_mms_drawer_activity_amount.dv_batch_id = #dv_batch_id.current_dv_batch_id
				where p_mms_drawer_activity_amount.dv_load_end_date_time = 'Dec 31, 9999'
				union
				select p_mms_drawer_activity_amount.drawer_activity_amount_id 
				from p_mms_drawer_activity_amount 
				join l_mms_drawer_activity_amount
				on p_mms_drawer_activity_amount.l_mms_drawer_activity_amount_id = l_mms_drawer_activity_amount.l_mms_drawer_activity_amount_id
		        join p_mms_drawer_activity
				on p_mms_drawer_activity.drawer_activity_id = l_mms_drawer_activity_amount.drawer_activity_id
				join l_mms_drawer_activity
				on l_mms_drawer_activity.drawer_activity_id = l_mms_drawer_activity_amount.drawer_activity_id
				join #dv_batch_id 
				on (p_mms_drawer_activity_amount.dv_batch_id > #dv_batch_id.max_dv_batch_id
				or p_mms_drawer_activity_amount.dv_batch_id = #dv_batch_id.current_dv_batch_id)
				and (p_mms_drawer_activity.dv_batch_id > #dv_batch_id.max_dv_batch_id
				or p_mms_drawer_activity.dv_batch_id = #dv_batch_id.current_dv_batch_id)
				where p_mms_drawer_activity_amount.dv_load_end_date_time = 'Dec 31, 9999' 
				and p_mms_drawer_activity.dv_load_end_date_time = 'Dec 31, 9999' 
				union
				select p_mms_drawer_activity_amount.drawer_activity_amount_id 
				from p_mms_drawer_activity_amount 
				join l_mms_drawer_activity_amount
				on p_mms_drawer_activity_amount.l_mms_drawer_activity_amount_id = l_mms_drawer_activity_amount.l_mms_drawer_activity_amount_id
				join p_mms_drawer_activity
				on p_mms_drawer_activity.drawer_activity_id = l_mms_drawer_activity_amount.drawer_activity_id
				join l_mms_drawer_activity
				on l_mms_drawer_activity.drawer_activity_id = l_mms_drawer_activity_amount.drawer_activity_id
				join p_mms_drawer
				on p_mms_drawer.drawer_id = l_mms_drawer_activity.drawer_id
				--join l_mms_drawer
				--on p_mms_drawer.drawer_id = l_mms_drawer.drawer_id
				join #dv_batch_id 
				on (p_mms_drawer_activity_amount.dv_batch_id > #dv_batch_id.max_dv_batch_id
				or p_mms_drawer_activity_amount.dv_batch_id = #dv_batch_id.current_dv_batch_id)
				and (p_mms_drawer_activity.dv_batch_id > #dv_batch_id.max_dv_batch_id
				or p_mms_drawer_activity.dv_batch_id = #dv_batch_id.current_dv_batch_id)
				and (p_mms_drawer.dv_batch_id > #dv_batch_id.max_dv_batch_id
				or p_mms_drawer.dv_batch_id = #dv_batch_id.current_dv_batch_id)
				where p_mms_drawer_activity_amount.dv_load_end_date_time = 'Dec 31, 9999' 
				and p_mms_drawer_activity.dv_load_end_date_time = 'Dec 31, 9999' 
				and p_mms_drawer.dv_load_end_date_time = 'Dec 31, 9999' 
				union
				select p_mms_drawer_activity_amount.drawer_activity_amount_id 
				from p_mms_drawer_activity_amount 
				join l_mms_drawer_activity_amount
				on p_mms_drawer_activity_amount.l_mms_drawer_activity_amount_id = l_mms_drawer_activity_amount.l_mms_drawer_activity_amount_id
				join r_mms_val_currency_code
				on  r_mms_val_currency_code.val_currency_code_id = l_mms_drawer_activity_amount.val_currency_code_id
				join #dv_batch_id 
				on (p_mms_drawer_activity_amount.dv_batch_id > #dv_batch_id.max_dv_batch_id
				or p_mms_drawer_activity_amount.dv_batch_id = #dv_batch_id.current_dv_batch_id)
				and (r_mms_val_currency_code.dv_batch_id > #dv_batch_id.max_dv_batch_id
				or r_mms_val_currency_code.dv_batch_id = #dv_batch_id.current_dv_batch_id)
				where p_mms_drawer_activity_amount.dv_load_end_date_time = 'Dec 31, 9999'
				and r_mms_val_currency_code.dv_load_end_date_time = 'Dec 31, 9999'
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~END OF STEP 1: BUSINESS KEY COLLECTION~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~STEP - 2:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
---STEP 2A: Preparing the required fields to build the dimension/fact table from the individual participating tables--------
---i.e. Business keys collected in "STEP 1" drives collection of records from each participating table!
---#p_mms_drawer_activity_amount
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  				
  if object_id('tempdb..#p_mms_drawer_activity_amount') is not null drop table #p_mms_drawer_activity_amount
		create table dbo.#p_mms_drawer_activity_amount with(distribution=hash(fact_club_pos_drawer_activity_amount_key), location=user_db, heap) as
				select 
				 --md5 calculation for bk_hash in etl_Proc: convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(drawer_activity_amount_id as varchar(500)),'z#@$k%&P'),2) bk_hash,
				p_mms_drawer_activity_amount.bk_hash fact_club_pos_drawer_activity_amount_key,
				p_mms_drawer_activity_amount.drawer_activity_amount_id,
				l_mms_drawer_activity_amount.val_payment_type_id,
				--md5 calculation for bk_hash in etl_Proc: convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(p_mms_drawer_activity_amount.val_payment_type_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+'MMS POS Payment Type')),2)
				case 
					when p_mms_drawer_activity_amount.bk_hash in ('-997','-998','-999') then p_mms_drawer_activity_amount.bk_hash
					when l_mms_drawer_activity_amount.val_payment_type_id is null then '-998' 
				else
				     'r_mms_val_payment_type_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_drawer_activity_amount.val_payment_type_id as varchar(500)),'z#@$k%&P'))),2)
				end payment_type_dim_description_key,
				s_mms_drawer_activity_amount.tran_total_amount,
				s_mms_drawer_activity_amount.actual_total_amount,
				p_mms_drawer_activity_amount.dv_batch_id dv_batch_id,
				p_mms_drawer_activity_amount.dv_load_date_time dv_load_date_time,
				p_mms_drawer_activity_amount.dv_load_end_date_time dv_load_end_date_time				
				from p_mms_drawer_activity_amount 
		left	join l_mms_drawer_activity_amount
				on p_mms_drawer_activity_amount.l_mms_drawer_activity_amount_id = l_mms_drawer_activity_amount.l_mms_drawer_activity_amount_id
				left	join s_mms_drawer_activity_amount
				on s_mms_drawer_activity_amount.s_mms_drawer_activity_amount_id = p_mms_drawer_activity_amount.s_mms_drawer_activity_amount_id
				join #dv_batch_id 
				on p_mms_drawer_activity_amount.dv_batch_id > #dv_batch_id.max_dv_batch_id
				or p_mms_drawer_activity_amount.dv_batch_id = #dv_batch_id.current_dv_batch_id
				where p_mms_drawer_activity_amount.dv_load_end_date_time = 'Dec 31, 9999' 
   
   
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~STEP - 2:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
---STEP 2B: Preparing the required fields to build the dimension/fact table from the individual participating tables--------
---i.e. Business keys collected in "STEP 1" drives collection of records from each participating table!
---#p_mms_drawer_activity--3531664
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   
  if object_id('tempdb..#p_mms_drawer_activity') is not null drop table #p_mms_drawer_activity
		create table dbo.#p_mms_drawer_activity with(distribution=hash(fact_club_pos_drawer_activity_amount_key), location=user_db, heap) as   
				select  
				--md5 calculation for bk_hash in etl_Proc: convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(drawer_activity_amount_id as varchar(500)),'z#@$k%&P'),2) bk_hash,
				p_mms_drawer_activity_amount.bk_hash fact_club_pos_drawer_activity_amount_key,
				p_mms_drawer_activity_amount.drawer_activity_amount_id,
				p_mms_drawer_activity.drawer_activity_id,
				s_mms_drawer_activity.close_date_time,
				r_date.r_date_id dim_date,
				r_date.month_ending_r_date_id month_ending_dim_date,
				r_date.year CalendarYear,
				p_mms_drawer_activity_amount.dv_batch_id dv_batch_id,
				p_mms_drawer_activity_amount.dv_load_date_time dv_load_date_time,
				p_mms_drawer_activity_amount.dv_load_end_date_time dv_load_end_date_time
				from p_mms_drawer_activity
				 join l_mms_drawer_activity
				on p_mms_drawer_activity.l_mms_drawer_activity_id = l_mms_drawer_activity.l_mms_drawer_activity_id 
				    join s_mms_drawer_activity
				on p_mms_drawer_activity.s_mms_drawer_activity_id = s_mms_drawer_activity.s_mms_drawer_activity_id 
				 join l_mms_drawer_activity_amount
				on l_mms_drawer_activity_amount.drawer_activity_id = l_mms_drawer_activity.drawer_activity_id 
				 join p_mms_drawer_activity_amount 
				on l_mms_drawer_activity_amount.l_mms_drawer_activity_amount_id  = p_mms_drawer_activity_amount.l_mms_drawer_activity_amount_id
				   left join r_date 
				on cast(cast(s_mms_drawer_activity.close_date_time as varchar(12)) as datetime) = r_date.calendar_date 	
				 join #dv_batch_id 
				on (p_mms_drawer_activity_amount.dv_batch_id > #dv_batch_id.max_dv_batch_id
				or p_mms_drawer_activity_amount.dv_batch_id = #dv_batch_id.current_dv_batch_id)
				and (p_mms_drawer_activity.dv_batch_id > #dv_batch_id.max_dv_batch_id
				or p_mms_drawer_activity.dv_batch_id = #dv_batch_id.current_dv_batch_id)
				--and (r_date.dv_batch_id > #dv_batch_id.max_dv_batch_id
				--or r_date.dv_batch_id = #dv_batch_id.current_dv_batch_id)
				where p_mms_drawer_activity_amount.dv_load_end_date_time = 'Dec 31, 9999' 
				and p_mms_drawer_activity.dv_load_end_date_time = 'Dec 31, 9999' 
				--and r_date.dv_load_end_date_time = 'Dec 31, 9999'
											
				--select * from #p_mms_drawer_activity
	--select * from 	#p_mms_drawer_activity
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~STEP - 2:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
---STEP 2C: Preparing the required fields to build the dimension/fact table from the individual participating tables--------
---i.e. Business keys collected in "STEP 1" drives collection of records from each participating table!
---#p_mms_drawer
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      
  if object_id('tempdb..#p_mms_drawer') is not null drop table #p_mms_drawer
		create table dbo.#p_mms_drawer with(distribution=hash(fact_club_pos_drawer_activity_amount_key), location=user_db, heap) as   				
				select 
				--md5 calculation for bk_hash in etl_Proc: convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(drawer_activity_amount_id as varchar(500)),'z#@$k%&P'),2) bk_hash,
				p_mms_drawer_activity_amount.bk_hash fact_club_pos_drawer_activity_amount_key,
				p_mms_drawer_activity_amount.drawer_activity_amount_id,
				l_mms_drawer.club_id,
				case 
					when l_mms_drawer.club_id is null then '-998' 
					else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_drawer.club_id as varchar(500)),'z#@$k%&P'))),2)
				end DimLocationKey,
				p_mms_drawer_activity_amount.dv_batch_id dv_batch_id,
				p_mms_drawer_activity_amount.dv_load_date_time dv_load_date_time,
				p_mms_drawer_activity_amount.dv_load_end_date_time dv_load_end_date_time	
				from p_mms_drawer_activity_amount 
				join l_mms_drawer_activity_amount
				on p_mms_drawer_activity_amount.l_mms_drawer_activity_amount_id = l_mms_drawer_activity_amount.l_mms_drawer_activity_amount_id
				join p_mms_drawer_activity
				on p_mms_drawer_activity.drawer_activity_id = l_mms_drawer_activity_amount.drawer_activity_id
				join l_mms_drawer_activity
				on l_mms_drawer_activity.drawer_activity_id = l_mms_drawer_activity_amount.drawer_activity_id
				join p_mms_drawer
				on p_mms_drawer.drawer_id = l_mms_drawer_activity.drawer_id
				join l_mms_drawer
				on p_mms_drawer.l_mms_drawer_id = l_mms_drawer.l_mms_drawer_id
				join #dv_batch_id 
				on p_mms_drawer_activity_amount.dv_batch_id > #dv_batch_id.max_dv_batch_id
				or p_mms_drawer_activity_amount.dv_batch_id = #dv_batch_id.current_dv_batch_id
				and (p_mms_drawer_activity.dv_batch_id > #dv_batch_id.max_dv_batch_id
				or p_mms_drawer_activity.dv_batch_id = #dv_batch_id.current_dv_batch_id)
				and (p_mms_drawer.dv_batch_id > #dv_batch_id.max_dv_batch_id
				or p_mms_drawer.dv_batch_id = #dv_batch_id.current_dv_batch_id)
				where p_mms_drawer_activity_amount.dv_load_end_date_time = 'Dec 31, 9999' 
				and p_mms_drawer_activity.dv_load_end_date_time = 'Dec 31, 9999' 
				and p_mms_drawer.dv_load_end_date_time = 'Dec 31, 9999' 
				
				
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~STEP - 2:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
---STEP 2D: Preparing the required fields to build the dimension/fact table from the individual participating tables--------
---i.e. Business keys collected in "STEP 1" drives collection of records from each participating table!
---#r_mms_val_currency_code
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 				      
  if object_id('tempdb..#r_mms_val_currency_code') is not null drop table #r_mms_val_currency_code
		create table dbo.#r_mms_val_currency_code with(distribution=hash(fact_club_pos_drawer_activity_amount_key), location=user_db, heap) as 
				select  
				p_mms_drawer_activity_amount.bk_hash fact_club_pos_drawer_activity_amount_key,
				p_mms_drawer_activity_amount.drawer_activity_amount_id,
				r_mms_val_currency_code.currency_code,
				isnull(r_mms_val_currency_code.currency_code,'USD') OriginalCurrencyCode,
				p_mms_drawer_activity_amount.dv_batch_id dv_batch_id,
				p_mms_drawer_activity_amount.dv_load_date_time dv_load_date_time,
				p_mms_drawer_activity_amount.dv_load_end_date_time dv_load_end_date_time	
				from p_mms_drawer_activity_amount 
				join l_mms_drawer_activity_amount
				on p_mms_drawer_activity_amount.l_mms_drawer_activity_amount_id = l_mms_drawer_activity_amount.l_mms_drawer_activity_amount_id
				join r_mms_val_currency_code
				on  r_mms_val_currency_code.val_currency_code_id = l_mms_drawer_activity_amount.val_currency_code_id
				join #dv_batch_id 
				on (p_mms_drawer_activity_amount.dv_batch_id > #dv_batch_id.max_dv_batch_id
				or p_mms_drawer_activity_amount.dv_batch_id = #dv_batch_id.current_dv_batch_id)
				and (r_mms_val_currency_code.dv_batch_id > #dv_batch_id.max_dv_batch_id
				or r_mms_val_currency_code.dv_batch_id = #dv_batch_id.current_dv_batch_id)
				where p_mms_drawer_activity_amount.dv_load_end_date_time = 'Dec 31, 9999'
				and r_mms_val_currency_code.dv_load_end_date_time = 'Dec 31, 9999'
				
----------------(*) These table now hold records for all the business keys in the driving table created in STEP 1   -----------------
--~~~~~~~~~~~~~~~END OF STEP 2: Requried Fields from different participating fields have been created as #TEMP tables~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~STEP - 3:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
----------------STEP 3: INSERT INTO DIM TABLE: By Joining the temp STEP 2's #temp tables, forming the main Dim table record-----------
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-- Delete and re-insert
-- do as a single transaction
-- delete records from the fact table that exist
-- insert records from records from current and missing batches
    begin tran
    delete dbo.fact_mms_club_pos_drawer_activity_amount
    where drawer_activity_amount_id in (select drawer_activity_amount_id from dbo.#p_drawer_activity_amount_id) 
	
				insert dbo.fact_mms_club_pos_drawer_activity_amount
				(
							fact_mms_club_pos_drawer_activity_amount_key,
							drawer_activity_amount_id,
							payment_type_dim_mms_description_key,
							dim_mms_location_key,
							transaction_total_amount,
							actual_total_amount,
							dim_mms_merchant_number_key,
							original_currency_code,
							usd_monthly_average_dim_mms_exchange_rate_key,
							usd_dim_mms_plan_exchange_rate_key,
							--local_currency_monthly_average_dim_mms_exchange_rate_key,
							--local_currency_dim_mms_plan_exchange_rate_key,
							dim_mms_drawer_activity_key,
							dim_mms_location_currency_code_key,
							dv_load_date_time,
							dv_load_end_date_time,
							dv_batch_id,
							dv_inserted_date_time,
							dv_insert_user
				)
				select distinct 
							#p_mms_drawer_activity_amount.fact_club_pos_drawer_activity_amount_key,
							case 
								when #p_mms_drawer_activity_amount.fact_club_pos_drawer_activity_amount_key in ('-997','-998','-999') then #p_mms_drawer_activity_amount.fact_club_pos_drawer_activity_amount_key
								else #p_mms_drawer_activity_amount.drawer_activity_amount_id
							end drawer_activity_amount_id,
							#p_mms_drawer_activity_amount.payment_type_dim_description_key,
							case 
								when #p_mms_drawer_activity_amount.fact_club_pos_drawer_activity_amount_key in ('-997','-998','-999') then #p_mms_drawer_activity_amount.fact_club_pos_drawer_activity_amount_key
								when #p_mms_drawer.DimLocationKey is null then '-998'
								else #p_mms_drawer.DimLocationKey 
							end dim_location_key,
							isnull(#p_mms_drawer_activity_amount.tran_total_amount,0),
							isnull(#p_mms_drawer_activity_amount.actual_total_amount,0),
							--md5 calculation for bk_hash in etl_Proc: convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(club_id and OriginalCurrencyCode as varchar(500)),'z#@$k%&P'),2) bk_hash,
							case 
								when #p_mms_drawer_activity_amount.fact_club_pos_drawer_activity_amount_key in ('-997','-998','-999') then #p_mms_drawer_activity_amount.fact_club_pos_drawer_activity_amount_key
								when #p_mms_drawer.club_id is null then '-998'
								else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#p_mms_drawer.club_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+
								isnull(cast(isnull(#r_mms_val_currency_code.OriginalCurrencyCode,'USD') as varchar(500)),'z#@$k%&P'))),2) 
							end	dim_merchant_number_key,
							isnull(#r_mms_val_currency_code.OriginalCurrencyCode,'USD'),
							--md5 calculation for bk_hash in etl_Proc: convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(month_ending_dim_date and OriginalCurrencyCode as varchar(500)),'z#@$k%&P'),2) bk_hash,
							case 
								when #p_mms_drawer_activity_amount.fact_club_pos_drawer_activity_amount_key in ('-997','-998','-999') 
								then #p_mms_drawer_activity_amount.fact_club_pos_drawer_activity_amount_key
								when (#p_mms_drawer_activity.month_ending_dim_date is null) 
								then '-998'
								else convert(char(32),hashbytes('md5',('P%#&z$@k'+
									 isnull(convert(varchar,convert(datetime,convert(varchar,#p_mms_drawer_activity.month_ending_dim_date)),120),'z#@$k%&P')+
									 'P%#&z$@k'+isnull(cast(isnull(#r_mms_val_currency_code.OriginalCurrencyCode,'USD') as varchar(500)),'z#@$k%&P')+
									 'P%#&z$@k'+'USD'+'P%#&z$@k'+'Monthly Average Exchange Rate')),2)
							end 		usd_monthly_average_dim_exchange_rate_key,
							--md5 calculation for bk_hash in etl_Proc: convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(CalendarYear and OriginalCurrencyCode as varchar(500)),'z#@$k%&P'),2) bk_hash,
							case 
								when #p_mms_drawer_activity_amount.fact_club_pos_drawer_activity_amount_key in ('-997','-998','-999') 
								then #p_mms_drawer_activity_amount.fact_club_pos_drawer_activity_amount_key
								when (#r_mms_val_currency_code.OriginalCurrencyCode  is null) 
								then '-998'
								else 	convert(char(32),hashbytes('md5',('P%#&z$@k'+
								isnull(isnull(#r_mms_val_currency_code.OriginalCurrencyCode,'USD'),'z#@$k%&P')+'P%#&z$@k'+'USD')),2)						 
							end 		usd_dim_plan_exchange_rate_key, 
							--md5 calculation for bk_hash in etl_Proc: convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(month_ending_dim_date and OriginalCurrencyCode as varchar(500)),'z#@$k%&P'),2) bk_hash,
							--case 
							--	when #p_mms_drawer_activity_amount.fact_club_pos_drawer_activity_amount_key in ('-997','-998','-999') then #p_mms_drawer_activity_amount.fact_club_pos_drawer_activity_amount_key
							--	when (#p_mms_drawer_activity.month_ending_dim_date is null) then '-998'
							--	when #r_mms_val_currency_code.Currency_Code is null then '-998'
							--	else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#p_mms_drawer_activity.month_ending_dim_date as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(#r_mms_val_currency_code.OriginalCurrencyCode as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+'Monthly Average Exchange Rate')),2)
							--end local_currency_monthly_average_dim_exchange_rate_key,
							--md5 calculation for bk_hash in etl_Proc: convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(CalendarYear and OriginalCurrencyCode as varchar(500)),'z#@$k%&P'),2) bk_hash,
							--case 
							--	when #p_mms_drawer_activity_amount.fact_club_pos_drawer_activity_amount_key in ('-997','-998','-999') then #p_mms_drawer_activity_amount.fact_club_pos_drawer_activity_amount_key
							--	when #p_mms_drawer_activity.CalendarYear is null then '-998'
							--	when #r_mms_val_currency_code.Currency_Code is null then '-998'
							--	else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#p_mms_drawer_activity.CalendarYear as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(#r_mms_val_currency_code.OriginalCurrencyCode as varchar(500)),'z#@$k%&P'))),2)
							--end local_currency_dim_plan_exchange_rate_key,
							--md5 calculation for bk_hash in etl_Proc: convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(drawer_activity_id as varchar(500)),'z#@$k%&P'),2) bk_hash,
							case 
								when #p_mms_drawer_activity_amount.fact_club_pos_drawer_activity_amount_key in ('-997','-998','-999') then #p_mms_drawer_activity_amount.fact_club_pos_drawer_activity_amount_key
								when #p_mms_drawer_activity.drawer_activity_id is null
								then '-998'
								else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#p_mms_drawer_activity.drawer_activity_id as varchar(500)),'z#@$k%&P'))),2)
							end dim_mms_drawer_activity_key,
							--md5 calculation for bk_hash in etl_Proc: convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(OriginalCurrencyCode as varchar(500)),'z#@$k%&P'),2) bk_hash,
							case 
								when #p_mms_drawer_activity_amount.fact_club_pos_drawer_activity_amount_key in ('-997','-998','-999') then #p_mms_drawer_activity_amount.fact_club_pos_drawer_activity_amount_key
								when #p_mms_drawer.club_id is null then '-998'
								else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#p_mms_drawer.club_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(#r_mms_val_currency_code.OriginalCurrencyCode as varchar(500)),'z#@$k%&P'))),2)
							end dim_mms_location_currency_code_key, 
							case 	when 	#p_mms_drawer_activity_amount.dv_load_date_time > isnull(#p_mms_drawer_activity.dv_load_date_time,'')
										and #p_mms_drawer_activity_amount.dv_load_date_time > isnull(#p_mms_drawer.dv_load_date_time,'')
										and #p_mms_drawer_activity_amount.dv_load_date_time > isnull(#r_mms_val_currency_code.dv_load_date_time,'')
									then #p_mms_drawer_activity_amount.dv_load_date_time
									when 	isnull(#p_mms_drawer_activity.dv_load_date_time,'') > isnull(#p_mms_drawer.dv_load_date_time,'')
										and isnull(#p_mms_drawer_activity.dv_load_date_time,'') > isnull(#r_mms_val_currency_code.dv_load_date_time,'')
									then isnull(#p_mms_drawer_activity.dv_load_date_time,'')
									when isnull(#p_mms_drawer.dv_load_date_time,'') > isnull(#r_mms_val_currency_code.dv_load_date_time,'')
									then isnull(#p_mms_drawer.dv_load_date_time,'')
								else isnull(#r_mms_val_currency_code.dv_load_date_time,'')
							end dv_load_date_time,
							case 	when 	#p_mms_drawer_activity_amount.dv_load_end_date_time > isnull(#p_mms_drawer_activity.dv_load_end_date_time,'')
										and #p_mms_drawer_activity_amount.dv_load_end_date_time > isnull(#p_mms_drawer.dv_load_end_date_time,'')
										and #p_mms_drawer_activity_amount.dv_load_end_date_time > isnull(#r_mms_val_currency_code.dv_load_end_date_time,'')
									then #p_mms_drawer_activity_amount.dv_load_end_date_time
									when 	isnull(#p_mms_drawer_activity.dv_load_end_date_time,'') > isnull(#p_mms_drawer.dv_load_end_date_time,'')
										and isnull(#p_mms_drawer_activity.dv_load_end_date_time,'') > isnull(#r_mms_val_currency_code.dv_load_end_date_time,'')
									then isnull(#p_mms_drawer_activity.dv_load_end_date_time,'')
									when isnull(#p_mms_drawer.dv_load_end_date_time,'') > isnull(#r_mms_val_currency_code.dv_load_end_date_time,'')
									then isnull(#p_mms_drawer.dv_load_end_date_time,'')
								else isnull(#r_mms_val_currency_code.dv_load_end_date_time,'')
							end dv_load_end_date_time,					
							case 	when 	#p_mms_drawer_activity_amount.dv_batch_id > isnull(#p_mms_drawer_activity.dv_batch_id,-2)
										and #p_mms_drawer_activity_amount.dv_batch_id > isnull(#p_mms_drawer.dv_batch_id,-2)
										and #p_mms_drawer_activity_amount.dv_batch_id > isnull(#r_mms_val_currency_code.dv_batch_id,-2)
									then #p_mms_drawer_activity_amount.dv_batch_id
									when 	isnull(#p_mms_drawer_activity.dv_batch_id,-2) > isnull(#p_mms_drawer.dv_batch_id,-2)
										and isnull(#p_mms_drawer_activity.dv_batch_id,-2) > isnull(#r_mms_val_currency_code.dv_batch_id,-2)
									then isnull(#p_mms_drawer_activity.dv_batch_id,-2)
									when isnull(#p_mms_drawer.dv_batch_id,-2) > isnull(#r_mms_val_currency_code.dv_batch_id,-2)
									then #p_mms_drawer.dv_batch_id
								else #r_mms_val_currency_code.dv_batch_id
							end dv_batch_id,
								getdate(),
								suser_sname()	 
					from 		#p_mms_drawer_activity_amount
					left join	#p_mms_drawer_activity	
					on 			#p_mms_drawer_activity_amount.fact_club_pos_drawer_activity_amount_key = #p_mms_drawer_activity.fact_club_pos_drawer_activity_amount_key
					left join   #p_mms_drawer
					on 			#p_mms_drawer_activity_amount.fact_club_pos_drawer_activity_amount_key = #p_mms_drawer.fact_club_pos_drawer_activity_amount_key
					left join	#r_mms_val_currency_code
					on			#p_mms_drawer_activity_amount.fact_club_pos_drawer_activity_amount_key = #r_mms_val_currency_code.fact_club_pos_drawer_activity_amount_key

	 	 
	 commit tran
 ---------------------------------------END OF STEP 3: END OF DIM INSERTS--------------------------------------

end

