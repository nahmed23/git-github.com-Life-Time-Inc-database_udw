CREATE PROC [dbo].[proc_fact_mms_member_usage] @dv_batch_id [varchar](500) AS
Begin
set xact_abort on
set nocount on

/* Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.*/
if object_id('tempdb..#dv_batch_id') is not null drop table #dv_batch_id
create table dbo.#dv_batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
    @dv_batch_id as current_dv_batch_id
    from dbo.fact_mms_member_usage

/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~STEP - 1~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~	*/
/*For a dimension record, the complete record needs to be rebuilt for a change in any field in any of the participating tables, Hence:*/
/*---STEP 1: Collecting Business Keys from the base table - that are corresponding to the changed Recs from all the participating tables & itself*/
/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
if object_id('tempdb..#Business_keys') is not null drop table #Business_keys
create table dbo.#Business_keys with(distribution=hash(fact_mms_member_usage_key), location=user_db, heap) as
select fact_mms_member_usage_key
from (select p_mms_member_usage.bk_hash fact_mms_member_usage_key 
        from p_mms_member_usage
        join #dv_batch_id 
		  on (p_mms_member_usage.dv_batch_id > #dv_batch_id.max_dv_batch_id
		      or p_mms_member_usage.dv_batch_id = #dv_batch_id.current_dv_batch_id)
	   where p_mms_member_usage.dv_load_end_date_time = 'Dec 31, 9999') Business_keys_Unioned
/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~END OF STEP 1: BUSINESS KEY COLLECTION~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/



/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~STEP - 2:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
/*-STEP 2: Preparing the required fields to build the dimension table from the individual participating tables--------*/
/*-i.e. Business keys collected in "STEP 1" drives collection of records from each participating table!*/
/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 	  */
	  
if object_id('tempdb..#mms_member_usage') is not null drop table #mms_member_usage
create table dbo.#mms_member_usage with(distribution=hash(fact_mms_member_usage_key), location=user_db, heap) as
   select p_mms_member_usage.bk_hash fact_mms_member_usage_key,
          p_mms_member_usage.member_usage_id,
		  l_mms_member_usage.club_id,
		  l_mms_member_usage.member_id,
		  s_mms_member_usage.usage_date_time,
		  s_mms_member_usage.checkin_delinquent_flag,
		  l_mms_member_usage.department_id,
          p_mms_member_usage.p_mms_member_usage_id,
		  case when p_mms_member_usage.bk_hash in('-997', '-998', '-999') then p_mms_member_usage.bk_hash
				when s_mms_member_usage.usage_date_time is null then '-998'
			else convert(varchar,s_mms_member_usage.usage_date_time, 112)    end check_in_dim_date_key,
		  case when p_mms_member_usage.bk_hash in ('-997','-998','-999') then p_mms_member_usage.bk_hash
				when s_mms_member_usage.usage_date_time is null then '-998'
				else '1' + replace(substring(convert(varchar,s_mms_member_usage.usage_date_time,114), 1, 5),':','') end check_in_dim_time_key,

          p_mms_member_usage.dv_load_end_date_time,
          p_mms_member_usage.dv_batch_id,
          p_mms_member_usage.dv_load_date_time          
     from #Business_keys
     join p_mms_member_usage 
       on p_mms_member_usage.bk_hash = #Business_keys.fact_mms_member_usage_key
     join l_mms_member_usage 
       on p_mms_member_usage.l_mms_member_usage_id = l_mms_member_usage.l_mms_member_usage_id
      and p_mms_member_usage.bk_hash = l_mms_member_usage.bk_hash
     join s_mms_member_usage 
       on p_mms_member_usage.s_mms_member_usage_id = s_mms_member_usage.s_mms_member_usage_id
      and p_mms_member_usage.bk_hash = s_mms_member_usage.bk_hash
      and p_mms_member_usage.dv_load_end_date_time = 'Dec 31, 9999'
 

 
	  
if object_id('tempdb..#dim_mms_member') is not null drop table #dim_mms_member
create table dbo.#dim_mms_member with(distribution=hash(fact_mms_member_usage_key), location=user_db, heap) as
   select p_mms_member_usage.bk_hash fact_mms_member_usage_key,
 		  d_mms_member.dim_mms_member_key dim_mms_primary_member_key,
		  checkin_member.dim_mms_membership_key,
		  checkin_member.date_of_birth,
		  checkin_member.gender_abbreviation 
     from #Business_keys
     join p_mms_member_usage 
       on p_mms_member_usage.bk_hash = #Business_keys.fact_mms_member_usage_key
     join l_mms_member_usage 
       on p_mms_member_usage.l_mms_member_usage_id = l_mms_member_usage.l_mms_member_usage_id
      and p_mms_member_usage.bk_hash = l_mms_member_usage.bk_hash
     join s_mms_member_usage 
       on p_mms_member_usage.s_mms_member_usage_id = s_mms_member_usage.s_mms_member_usage_id
      and p_mms_member_usage.bk_hash = s_mms_member_usage.bk_hash
      and p_mms_member_usage.dv_load_end_date_time = 'Dec 31, 9999'
left join d_mms_member
	   on l_mms_member_usage.member_id = d_mms_member.member_id
	  and d_mms_member.member_type_dim_description_key  = (select distinct rtrim(dim_description_key) from dim_description where source_object ='r_mms_val_member_type' and description ='Primary')
left join d_mms_member checkin_member
	   on l_mms_member_usage.member_id = checkin_member.member_id

/*~~~~~~~~~~~~~~~~~~END OF STEP 2: Requried Fields from different participating fields have been created as #TEMP tables~~~~~~~~~~~~~~~~~~~~*/


/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~STEP - 3:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
    /*---------STEP 3: INSERT INTO DIM TABLE: By Joining the temp STEP 2's #temp tables, forming the main Dim table record-----------*/
/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

/* Delete and re-insert*/
/* do as a single transaction*/
/* delete records from the fact table that exist*/
/* insert records from records from current and missing batches*/
    begin tran
    delete dbo.fact_mms_member_usage
    where fact_mms_member_usage_key in (select fact_mms_member_usage_key from dbo.#mms_member_usage) 

 
    insert into fact_mms_member_usage
    (fact_mms_member_usage_key,
     member_usage_id,
     dim_club_key,
     dim_mms_primary_member_key,
     dim_mms_checkin_member_key,
     check_in_dim_date_time,
     gender_abbreviation,
     member_age_years,
     delinquent_checkin_flag,
     department_dim_mms_description_key,
     dim_mms_membership_key,
     p_mms_member_usage_id,
	 check_in_dim_date_key,
	 check_in_dim_time_key,
 	 dv_load_date_time,
     dv_load_end_date_time,
     dv_batch_id,
     dv_inserted_date_time,
     dv_insert_user)

 Select 
     #mms_member_usage.fact_mms_member_usage_key,
     #mms_member_usage.member_usage_id,
	 case when #mms_member_usage.fact_mms_member_usage_key in ('-997','-998','-999') then #mms_member_usage.fact_mms_member_usage_key
          when #mms_member_usage.club_id is null then '-998'
          /*util_bk_hash[l_mms_member_usage.club_id,h_mms_club.club_id]*/
	      else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#mms_member_usage.club_id as varchar(500)),'z#@$k%&P'))),2)
     end dim_club_key,
	 #dim_mms_member.dim_mms_primary_member_key,
	 case when #mms_member_usage.fact_mms_member_usage_key in ('-997','-998','-999') then #mms_member_usage.fact_mms_member_usage_key
          when #mms_member_usage.member_id is null then '-998'
          /*util_bk_hash[l_mms_member_usage.member_id,h_mms_member.member_id]*/
	      else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#mms_member_usage.member_id as varchar(500)),'z#@$k%&P'))),2)
     end dim_mms_checkin_member_key,
	 #mms_member_usage.usage_date_time check_in_dim_date_time,
     #dim_mms_member.gender_abbreviation,
	 datediff(yy, #dim_mms_member.date_of_birth, #mms_member_usage.usage_date_time)
     -case when month(#dim_mms_member.date_of_birth) > month(#mms_member_usage.usage_date_time)
             or (month(#dim_mms_member.date_of_birth) = month(#mms_member_usage.usage_date_time)
            and day(#dim_mms_member.date_of_birth) > day(#mms_member_usage.usage_date_time)) then 1 
 		   else 0 end member_age_years,
    case when #mms_member_usage.fact_mms_member_usage_key in ('-997','-998','-999') then null
	      when #mms_member_usage.checkin_delinquent_flag=1 then 'Y'
          else 'N'
     end delinquent_checkin_flag,	 
	 case when #mms_member_usage.fact_mms_member_usage_key in ('-997','-998','-999') then #mms_member_usage.fact_mms_member_usage_key
          when #mms_member_usage.department_id is null then '-998'
          /*util_bk_hash[l_mms_member_usage.department_id,h_mms_department.department_id]*/
	      else 'h_mms_departments_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#mms_member_usage.department_id as varchar(500)),'z#@$k%&P'))),2)
     end department_dim_mms_description_key,
	 #dim_mms_member.dim_mms_membership_key,
	 #mms_member_usage.p_mms_member_usage_id,
	 #mms_member_usage.check_in_dim_date_key,
	 #mms_member_usage.check_in_dim_time_key,	 
	 #mms_member_usage.dv_load_date_time,
     #mms_member_usage.dv_load_end_date_time,
     #mms_member_usage.dv_batch_id,
     getdate(),
     suser_sname()	 
	 from #mms_member_usage
left join #dim_mms_member
       on #mms_member_usage.fact_mms_member_usage_key = #dim_mms_member.fact_mms_member_usage_key
 
   	 commit tran
/*-------------------------------------END OF STEP 3: END OF DIM INSERTS--------------------------------------*/
end
