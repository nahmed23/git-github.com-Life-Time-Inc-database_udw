CREATE PROC [dbo].[proc_fact_boss_daily_roster] @begin_extract_date_time [varchar](500) AS
  /* Input parameter: @begin_extract_date_time varchar(100) /*$$begin_extract_date_time*/*/
 
 begin
 
 set xact_abort on
 set nocount on

 /* Date portion of @begin_extract_date_time*/
declare @begin_extract_dim_date_key char(8) = (convert(char(8), convert(datetime, @begin_extract_date_time, 120), 112))
 /* Calculate the max rebuild date as today plus 90 days*/
declare @max_rebuild_dim_date_key char(8) = (select convert(char(8), dateadd(d,90,getdate()), 112))

 /* Major differences between reservation types*/
 /*   Type 1 reservation_type: A,E,C,G are for reservations associated with player records with date ranges*/
 /*   Type 2 reservation_type: L,D are for reservations associated with player records with a single date */

/*/*------------------------------------------------------------------------------------------------*/*/
/* Type 1 reservations*/
/*/*------------------------------------------------------------------------------------------------*/*/

 /* Note that the following small queries are used due to inefficiencies in multi-tables queries using greater than or less than comparison and for logic readability*/

 /* Get the set of type 1 reservations active for all or part of the rebuild period*/
 /* Limit to "active" status reservations with the appropriate reservation types*/
if object_id('tempdb..#d_boss_asi_reserv_temp') is not null drop table #d_boss_asi_reserv_temp
create table #d_boss_asi_reserv_temp with(distribution=hash(dim_boss_reservation_key),location=user_db) as
select d_boss_asi_reserv.dim_boss_reservation_key,
       d_boss_asi_reserv.reservation_id,
	   d_boss_asi_reserv.dim_employee_key,
       d_boss_asi_reserv.start_dim_date_key,
       d_boss_asi_reserv.end_dim_date_key,
       d_boss_asi_reserv.day_plan_ints,
	   d_boss_asi_reserv.reservation_type,	   
       d_boss_asi_reserv.dv_load_date_time,
       d_boss_asi_reserv.dv_batch_id
  from d_boss_asi_reserv
 where (d_boss_asi_reserv.start_dim_date_key <= @max_rebuild_dim_date_key
   and d_boss_asi_reserv.end_dim_date_key >= @begin_extract_dim_date_key
   and d_boss_asi_reserv.reservation_status = 'A')
        or (@begin_extract_dim_date_key <= '19000101'
         and d_boss_asi_reserv.bk_hash in ('-997','-998','-999'))
  /*and d_boss_asi_reserv.reservation_type in ('A','E','C','G','L','D')*/

 /* Get the set of dates to rebuild*/
if object_id('tempdb..#dim_date') is not null drop table #dim_date
create table #dim_date with(distribution=hash(dim_date_key),location=user_db) as
select dim_date.dim_date_key,
       dim_date.day_number_in_week,
	   row_number() over(order by dim_date.dim_date_key) as row_num
  from dim_date
 where dim_date.dim_date_key >= @begin_extract_dim_date_key
   and dim_date.dim_date_key <= @max_rebuild_dim_date_key

 /* Take all reservations active during the rebuild period*/
 /* and multiply out by the reservation day plan days*/
 /*   d_boss_asi_reserv.day_plan_ints 1 = Sunday*/
 /*   dim_date.day_number_in_week 1 = Sunday*/
 /*for type L,D maintain single record since they are one time event*/
if object_id('tempdb..#meeting_date_d_boss_asi_reserv_temp') is not null drop table #meeting_date_d_boss_asi_reserv_temp
create table #meeting_date_d_boss_asi_reserv_temp with(distribution=hash(dim_boss_reservation_key),location=user_db) as
select #dim_date.dim_date_key meeting_dim_date_key,
       #d_boss_asi_reserv_temp.dim_boss_reservation_key,
	   #d_boss_asi_reserv_temp.start_dim_date_key,
       #d_boss_asi_reserv_temp.end_dim_date_key,
       #d_boss_asi_reserv_temp.reservation_id,
	   #d_boss_asi_reserv_temp.dim_employee_key,
	   #d_boss_asi_reserv_temp.reservation_type,
       #d_boss_asi_reserv_temp.dv_load_date_time,
       #d_boss_asi_reserv_temp.dv_batch_id
  from #d_boss_asi_reserv_temp
  join #dim_date
    on (case when #d_boss_asi_reserv_temp.reservation_type in ('L','D') then 1 else 0 end =#dim_date.row_num) or
	( case when #d_boss_asi_reserv_temp.reservation_type in('A','E','C','G') and 
	#d_boss_asi_reserv_temp.start_dim_date_key <= #dim_date.dim_date_key
   and #d_boss_asi_reserv_temp.end_dim_date_key >= #dim_date.dim_date_key
   and charindex(convert(varchar, #dim_date.day_number_in_week), #d_boss_asi_reserv_temp.day_plan_ints) != 0 then 1 else 0 end =1)

 /* Find the asi_player records active during the rebuild period that are associated with the type 1&2 reservations from above*/
 /* Note that type 1 asi_player records are for date ranges as comparted to type 2 asi_player records that are for a single date*/

if object_id('tempdb..#d_boss_asi_player_temp') is not null drop table #d_boss_asi_player_temp
create table #d_boss_asi_player_temp with(distribution=hash(dim_boss_reservation_key),location=user_db) as
select d_boss_asi_player.dim_boss_reservation_key,
       d_boss_asi_player.start_dim_date_key,
       d_boss_asi_player.cancel_dim_date_key,
	   d_boss_asi_player.used_dim_date_key,	   
	   d_boss_asi_player.check_in_dim_date_key,	
       d_boss_asi_player.check_in_dim_time_key,
       d_boss_asi_player.asi_player_id,
       d_boss_asi_player.dim_mms_member_key,
       d_boss_asi_player.fact_mms_sales_transaction_key,
       d_boss_asi_player.member_code,
       d_boss_asi_player.member_flag,
       d_boss_asi_player.notes,
	   d_boss_asi_player.paid,
	   d_boss_asi_player.mms_swipe_flag,
       d_boss_asi_player.checked_in_flag player_checked_in_flag,
       d_boss_asi_player.dv_load_date_time,
       d_boss_asi_player.dv_batch_id
  from d_boss_asi_player
		where d_boss_asi_player.dim_boss_reservation_key in (select dim_boss_reservation_key from #d_boss_asi_reserv_temp)
		    or (@begin_extract_dim_date_key <= '19000101' and d_boss_asi_player.bk_hash in ('-997','-998','-999'))

 /* Join #meeting_date_d_boss_asi_reserv_temp with #d_boss_asi_player_temp where the asi_player date range is within the asi_reserv date range*/
 /*   Note that cancel_date is the day after the last date the player can use the reservation*/
 /*   reservation meeting_date >= player start_date*/
 /*   reservation meeting_date < player cancel_date*/
  /*   asi_player start date <= max rebuild date*/
 /*   asi_player cancel date is null or asi_player cancel date >= begin extract date*/
 /*   asi_player start date != asi_player cancel date*/
 /* Join to the optional d_boss_attendance records using the reservation, member_code/mbr_code and meeting date = attendance date*/
if object_id('tempdb..#boss_daily_roster') is not null drop table #boss_daily_roster
create table #boss_daily_roster with(distribution=hash(dim_boss_reservation_key),location=user_db) as
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#meeting_date_d_boss_asi_reserv_temp.reservation_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(#d_boss_asi_player_temp.member_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(case 
		when #meeting_date_d_boss_asi_reserv_temp.reservation_type in ('A','E','C','G') then #meeting_date_d_boss_asi_reserv_temp.meeting_dim_date_key 
		when #meeting_date_d_boss_asi_reserv_temp.reservation_type in ('L','D') then #d_boss_asi_player_temp.used_dim_date_key 
		end,'z#@$k%&P'))),2) fact_boss_daily_roster_key,
       #meeting_date_d_boss_asi_reserv_temp.reservation_id,
       #d_boss_asi_player_temp.asi_player_id,
       d_boss_attendance.checked_in_flag attendance_checked_in_flag, /* If the left join doesn't find an attendance record we want this to be null so no conversion is necessary*/
       #meeting_date_d_boss_asi_reserv_temp.dim_boss_reservation_key,
	   #meeting_date_d_boss_asi_reserv_temp.dim_employee_key,
       #d_boss_asi_player_temp.dim_mms_member_key,
       #d_boss_asi_player_temp.fact_mms_sales_transaction_key,
		case 
			when #meeting_date_d_boss_asi_reserv_temp.reservation_type in ('A','E','C','G') then #meeting_date_d_boss_asi_reserv_temp.meeting_dim_date_key 
			when #meeting_date_d_boss_asi_reserv_temp.reservation_type in ('L','D') then #d_boss_asi_player_temp.used_dim_date_key 
		end as meeting_dim_date_key,
	   #d_boss_asi_player_temp.check_in_dim_date_key,	
       #d_boss_asi_player_temp.check_in_dim_time_key,
       #d_boss_asi_player_temp.member_code,
       #d_boss_asi_player_temp.member_flag,
       #d_boss_asi_player_temp.notes,
	   #d_boss_asi_player_temp.paid,
	   #d_boss_asi_player_temp.mms_swipe_flag,
       #d_boss_asi_player_temp.cancel_dim_date_key player_cancel_dim_date_key,
       #d_boss_asi_player_temp.player_checked_in_flag,
       #d_boss_asi_player_temp.start_dim_date_key player_start_dim_date_key,
       case when #meeting_date_d_boss_asi_reserv_temp.dv_load_date_time >= isnull(#d_boss_asi_player_temp.dv_load_date_time,'Jan 1, 1753')
            then #meeting_date_d_boss_asi_reserv_temp.dv_load_date_time
            else isnull(#d_boss_asi_player_temp.dv_load_date_time,'Jan 1, 1753') end dv_load_date_time,
       convert(datetime, '99991231', 112) dv_load_end_date_time,
       case when #meeting_date_d_boss_asi_reserv_temp.dv_batch_id >= isnull(#d_boss_asi_player_temp.dv_batch_id,-1)
            then #meeting_date_d_boss_asi_reserv_temp.dv_batch_id
            else isnull(#d_boss_asi_player_temp.dv_batch_id,-1) end dv_batch_id,
       /* Duplicates will be generated due to invalid BOSS asi_player records so prepare for deletes.  There are at least two situations and maybe more that cause this.*/
       /*   There are some asi_player records where there are 2+ records with no cancel_date*/
       /*   There are also some asi_player records with date ranges that overlap*/
       row_number() over(partition by #meeting_date_d_boss_asi_reserv_temp.dim_boss_reservation_key, 
	   case 
			when #meeting_date_d_boss_asi_reserv_temp.reservation_type in ('A','E','C','G') then #meeting_date_d_boss_asi_reserv_temp.meeting_dim_date_key 
			when #meeting_date_d_boss_asi_reserv_temp.reservation_type in ('L','D') then #d_boss_asi_player_temp.used_dim_date_key 
		end, #d_boss_asi_player_temp.member_code order by #d_boss_asi_player_temp.asi_player_id desc) r
  from #meeting_date_d_boss_asi_reserv_temp
  join #d_boss_asi_player_temp
    on #meeting_date_d_boss_asi_reserv_temp.dim_boss_reservation_key = #d_boss_asi_player_temp.dim_boss_reservation_key
	and case 
	when #meeting_date_d_boss_asi_reserv_temp.reservation_type in ('A','E','C','G') 
		and #meeting_date_d_boss_asi_reserv_temp.meeting_dim_date_key >= #d_boss_asi_player_temp.start_dim_date_key
		and #meeting_date_d_boss_asi_reserv_temp.meeting_dim_date_key < case when #d_boss_asi_player_temp.cancel_dim_date_key = '-998' then '99991231' else #d_boss_asi_player_temp.cancel_dim_date_key end
		and #d_boss_asi_player_temp.start_dim_date_key <= @max_rebuild_dim_date_key and   #d_boss_asi_player_temp.start_dim_date_key != #d_boss_asi_player_temp.cancel_dim_date_key
		and case when #d_boss_asi_player_temp.cancel_dim_date_key = '-998' then '99991231' else #d_boss_asi_player_temp.cancel_dim_date_key end >= @begin_extract_dim_date_key 
		then 1 
	 when #meeting_date_d_boss_asi_reserv_temp.reservation_type in ('L','D') 
	    and #meeting_date_d_boss_asi_reserv_temp.start_dim_date_key <= #d_boss_asi_player_temp.used_dim_date_key
		and #meeting_date_d_boss_asi_reserv_temp.end_dim_date_key >= #d_boss_asi_player_temp.used_dim_date_key
		and #d_boss_asi_player_temp.used_dim_date_key >= @begin_extract_dim_date_key and #d_boss_asi_player_temp.used_dim_date_key <= @max_rebuild_dim_date_key
		then 1
	 else 0 end =1
  left join d_boss_attendance
    on #d_boss_asi_player_temp.dim_boss_reservation_key = d_boss_attendance.dim_boss_reservation_key
   and #d_boss_asi_player_temp.member_code = d_boss_attendance.mbr_code
   and #meeting_date_d_boss_asi_reserv_temp.meeting_dim_date_key = d_boss_attendance.attendance_dim_date_key

 /* Delete duplicates where there is more than one instance (due to invalid source data) of a reservation, date and player*/
delete #boss_daily_roster
 where r > 1

 /* Using res_cancel delete records for the reservation/date combination from fact_boss_boss_daily_roster*/
delete #boss_daily_roster
 where #boss_daily_roster.dim_boss_reservation_key + '_' + #boss_daily_roster.meeting_dim_date_key
    in (select #boss_daily_roster.dim_boss_reservation_key + '_' + #boss_daily_roster.meeting_dim_date_key
          from #boss_daily_roster
          join d_boss_res_cancel
            on #boss_daily_roster.dim_boss_reservation_key = d_boss_res_cancel.dim_boss_reservation_key
           and #boss_daily_roster.meeting_dim_date_key = d_boss_res_cancel.cancel_dim_date_key)


/*-Get incremental load for #d_boss_asi_res_inst */
if object_id('tempdb..#d_boss_asi_res_inst') is not null drop table #d_boss_asi_res_inst
create table dbo.#d_boss_asi_res_inst with(distribution = hash(dim_boss_reservation_key), location=user_db) as
select d_boss_asi_res_inst.asi_res_inst_id,
       d_boss_asi_res_inst.dim_boss_reservation_key,
       d_boss_asi_res_inst.dim_employee_key,
       d_boss_asi_res_inst.instructor_start_dim_date_key,
       d_boss_asi_res_inst.instructor_end_dim_date_key,
       d_boss_asi_res_inst.instructor_type,
       d_boss_asi_res_inst.dv_load_date_time,
       d_boss_asi_res_inst.dv_batch_id
  from d_boss_asi_res_inst
  join (select distinct dim_boss_reservation_key from #boss_daily_roster) dim_boss_reservation_key_list
    on d_boss_asi_res_inst.dim_boss_reservation_key = dim_boss_reservation_key_list.dim_boss_reservation_key

	
	
/* There may be more than one instructor associated with a participation record.  */
 /* In that case use substitue (S) over primary (P).  */
 /* Note that Assistants (A) are only for the secondary instructor.  */
 /* Since S comes alphabetically before P and P before A we can just use a descending sort to determine the rank of the primary instructor.*/
 /* In case there are multiples order by latest start date then earliest end date then latest reservation instructor record id.*/
 /* Since A comes alphabetically before P and S we can use an ascending sort to determine the rank of the associate instructor.*/
 /* Note that there may not be an asi_res_inst record so use a left join.*/
 
if object_id('tempdb.dbo.#combined') is not null drop table #combined
create table dbo.#combined with(distribution = hash(fact_boss_daily_roster_key), location=user_db) as
select #boss_daily_roster.fact_boss_daily_roster_key fact_boss_daily_roster_key,
       #boss_daily_roster.reservation_id reservation_id,
       #boss_daily_roster.asi_player_id asi_player_id,
	   #boss_daily_roster.attendance_checked_in_flag attendance_checked_in_flag,
	   #boss_daily_roster.dim_boss_reservation_key dim_boss_reservation_key,
       #boss_daily_roster.dim_mms_member_key dim_mms_member_key,
       #boss_daily_roster.fact_mms_sales_transaction_key fact_mms_sales_transaction_key,
       #boss_daily_roster.meeting_dim_date_key meeting_dim_date_key,
	   #boss_daily_roster.check_in_dim_date_key check_in_dim_date_key,
	   #boss_daily_roster.check_in_dim_time_key check_in_dim_time_key,
       #boss_daily_roster.member_code member_code,
	   #boss_daily_roster.member_flag member_flag,
	   #boss_daily_roster.notes notes,
	   #boss_daily_roster.paid paid,
	   #boss_daily_roster.mms_swipe_flag mms_swipe_flag,
	   #boss_daily_roster.player_cancel_dim_date_key player_cancel_dim_date_key,
	   #boss_daily_roster.player_checked_in_flag player_checked_in_flag,
	   #boss_daily_roster.player_start_dim_date_key player_start_dim_date_key,
	   #boss_daily_roster.dim_employee_key reserv_dim_employee_key,
	   #d_boss_asi_res_inst.dim_employee_key res_inst_dim_employee_key,
       #d_boss_asi_res_inst.instructor_type instructor_type,
       case when #boss_daily_roster.dv_load_date_time >= isnull(#d_boss_asi_res_inst.dv_load_date_time,'Jan 1, 1753')
            then #boss_daily_roster.dv_load_date_time
           else isnull(#d_boss_asi_res_inst.dv_load_date_time,'Jan 1, 1753')
             end dv_load_date_time,
       case when #boss_daily_roster.dv_batch_id >= isnull(#d_boss_asi_res_inst.dv_batch_id,-1)
            then #boss_daily_roster.dv_batch_id
           else isnull(#d_boss_asi_res_inst.dv_batch_id,-1)
        end dv_batch_id,
       row_number() over(partition by #boss_daily_roster.dim_boss_reservation_key, #boss_daily_roster.meeting_dim_date_key,#boss_daily_roster.asi_player_id 
               	   order by #d_boss_asi_res_inst.instructor_type desc, #d_boss_asi_res_inst.instructor_start_dim_date_key desc,
				   #d_boss_asi_res_inst.instructor_end_dim_date_key, #d_boss_asi_res_inst.asi_res_inst_id desc) primary_rank,
       row_number() over(partition by #boss_daily_roster.dim_boss_reservation_key, #boss_daily_roster.meeting_dim_date_key,#boss_daily_roster.asi_player_id 
	                order by #d_boss_asi_res_inst.instructor_type, #d_boss_asi_res_inst.instructor_start_dim_date_key desc,
					#d_boss_asi_res_inst.instructor_end_dim_date_key, #d_boss_asi_res_inst.asi_res_inst_id desc) secondary_rank
  into #combined
  from #boss_daily_roster
    left join #d_boss_asi_res_inst
    on #boss_daily_roster.dim_boss_reservation_key = #d_boss_asi_res_inst.dim_boss_reservation_key
   and #boss_daily_roster.meeting_dim_date_key >= #d_boss_asi_res_inst.instructor_start_dim_date_key
   and #boss_daily_roster.meeting_dim_date_key <= #d_boss_asi_res_inst.instructor_end_dim_date_key
   
 /* Combine the primary data with the optional secondary data*/
 /* Note that a full outer join is required since there are a few cases where there is an A record, but not a P or S.*/
 /* Therefore all returned columns require a case statement to determine if there is a primary record*/

if object_id('tempdb..#fact_boss_daily_roster') is not null drop table #fact_boss_daily_roster
create table dbo.#fact_boss_daily_roster with(distribution = hash(fact_boss_daily_roster_key), location=user_db) as
with 
  primary_combined (fact_boss_daily_roster_key, reservation_id, asi_player_id, attendance_checked_in_flag,dim_boss_reservation_key,
                   dim_mms_member_key, fact_mms_sales_transaction_key, meeting_dim_date_key, check_in_dim_date_key,
                   check_in_dim_time_key,member_code,member_flag,notes,paid,mms_swipe_flag,player_cancel_dim_date_key,
                   player_checked_in_flag,player_start_dim_date_key,reserv_dim_employee_key,res_inst_dim_employee_key,
                   instructor_type,dv_load_date_time, dv_batch_id) as
      (select #combined.fact_boss_daily_roster_key,
	          #combined.reservation_id,
              #combined.asi_player_id,
              #combined.attendance_checked_in_flag,
              #combined.dim_boss_reservation_key,
              #combined.dim_mms_member_key,
              #combined.fact_mms_sales_transaction_key,
              #combined.meeting_dim_date_key,
              #combined.check_in_dim_date_key,
			  #combined.check_in_dim_time_key,
			  #combined.member_code,
			  #combined.member_flag,
			  #combined.notes,
			  #combined.paid,
			  #combined.mms_swipe_flag,
			  #combined.player_cancel_dim_date_key,
			  #combined.player_checked_in_flag,
			  #combined.player_start_dim_date_key,
			  #combined.reserv_dim_employee_key,
			  #combined.res_inst_dim_employee_key,
			  #combined.instructor_type,
              #combined.dv_load_date_time,
              #combined.dv_batch_id
         from #combined
        where #combined.primary_rank = 1
          and (#combined.instructor_type in ('S','P')
               or #combined.instructor_type is null)),
  secondary_combined (fact_boss_daily_roster_key, reservation_id, asi_player_id, attendance_checked_in_flag,dim_boss_reservation_key,
                   dim_mms_member_key, fact_mms_sales_transaction_key, meeting_dim_date_key, check_in_dim_date_key,
                   check_in_dim_time_key,member_code,member_flag,notes,paid,mms_swipe_flag,player_cancel_dim_date_key,
                   player_checked_in_flag,player_start_dim_date_key,reserv_dim_employee_key,res_inst_dim_employee_key,
                   instructor_type,dv_load_date_time, dv_batch_id) as
      (select #combined.fact_boss_daily_roster_key,
	          #combined.reservation_id,
              #combined.asi_player_id,
              #combined.attendance_checked_in_flag,
              #combined.dim_boss_reservation_key,
              #combined.dim_mms_member_key,
              #combined.fact_mms_sales_transaction_key,
              #combined.meeting_dim_date_key,
              #combined.check_in_dim_date_key,
			  #combined.check_in_dim_time_key,
			  #combined.member_code,
			  #combined.member_flag,
			  #combined.notes,
			  #combined.paid,
			  #combined.mms_swipe_flag,
			  #combined.player_cancel_dim_date_key,
			  #combined.player_checked_in_flag,
			  #combined.player_start_dim_date_key,
			  #combined.reserv_dim_employee_key,
			  #combined.res_inst_dim_employee_key,
			  #combined.instructor_type,
              #combined.dv_load_date_time,
              #combined.dv_batch_id
         from #combined
        where #combined.secondary_rank = 1
          and #combined.instructor_type = 'A')
		  
select isnull(primary_combined.fact_boss_daily_roster_key, secondary_combined.fact_boss_daily_roster_key) fact_boss_daily_roster_key,
       case when primary_combined.fact_boss_daily_roster_key is not null
            then primary_combined.reservation_id
            else secondary_combined.reservation_id
        end reservation_id,
       case when primary_combined.fact_boss_daily_roster_key is not null
            then primary_combined.asi_player_id
            else secondary_combined.asi_player_id
        end asi_player_id,
       case when primary_combined.fact_boss_daily_roster_key is not null
            then primary_combined.attendance_checked_in_flag
            else secondary_combined.attendance_checked_in_flag
        end attendance_checked_in_flag,
       case when primary_combined.fact_boss_daily_roster_key is not null
            then primary_combined.dim_boss_reservation_key
            else secondary_combined.dim_boss_reservation_key
        end dim_boss_reservation_key,
	   case when primary_combined.fact_boss_daily_roster_key is not null
            then primary_combined.dim_mms_member_key
            else secondary_combined.dim_mms_member_key
        end dim_mms_member_key,
	   case when primary_combined.fact_boss_daily_roster_key is not null
            then primary_combined.fact_mms_sales_transaction_key
            else secondary_combined.fact_mms_sales_transaction_key
        end fact_mms_sales_transaction_key,
	   case when primary_combined.fact_boss_daily_roster_key is not null
            then primary_combined.meeting_dim_date_key
            else secondary_combined.meeting_dim_date_key
        end meeting_dim_date_key,
	   case when primary_combined.fact_boss_daily_roster_key is not null
            then primary_combined.check_in_dim_date_key
            else secondary_combined.check_in_dim_date_key
        end check_in_dim_date_key,
	   case when primary_combined.fact_boss_daily_roster_key is not null
            then primary_combined.check_in_dim_time_key
            else secondary_combined.check_in_dim_time_key
        end check_in_dim_time_key,
	   case when primary_combined.fact_boss_daily_roster_key is not null
            then primary_combined.member_code
            else secondary_combined.member_code
        end member_code,
	   case when primary_combined.fact_boss_daily_roster_key is not null
            then primary_combined.member_flag
            else secondary_combined.member_flag
        end member_flag,
	   case when primary_combined.fact_boss_daily_roster_key is not null
            then primary_combined.notes
            else secondary_combined.notes
        end notes,
	   case when primary_combined.fact_boss_daily_roster_key is not null
            then primary_combined.paid
            else secondary_combined.paid
        end paid,
	   case when primary_combined.fact_boss_daily_roster_key is not null
            then primary_combined.mms_swipe_flag
            else secondary_combined.mms_swipe_flag
        end mms_swipe_flag,
	   case when primary_combined.fact_boss_daily_roster_key is not null
            then primary_combined.player_cancel_dim_date_key
            else secondary_combined.player_cancel_dim_date_key
        end player_cancel_dim_date_key,
	   case when primary_combined.fact_boss_daily_roster_key is not null
            then primary_combined.player_checked_in_flag
            else secondary_combined.player_checked_in_flag
        end player_checked_in_flag,
	   case when primary_combined.fact_boss_daily_roster_key is not null
            then primary_combined.player_start_dim_date_key
            else secondary_combined.player_start_dim_date_key
        end player_start_dim_date_key,
	   case when primary_combined.fact_boss_daily_roster_key is not null
            then isnull(primary_combined.instructor_type, 'P')
            else secondary_combined.instructor_type
        end instructor_type,
       case when primary_combined.fact_boss_daily_roster_key is not null
            then isnull(primary_combined.res_inst_dim_employee_key, primary_combined.reserv_dim_employee_key)
            else secondary_combined.res_inst_dim_employee_key
        end primary_dim_employee_key,
       case when primary_combined.fact_boss_daily_roster_key is not null
            then isnull(secondary_combined.res_inst_dim_employee_key, '-997')
            else '-997'
        end secondary_dim_employee_key,
       case when primary_combined.dv_load_date_time >= isnull(secondary_combined.dv_load_date_time,'Jan 1, 1753')
            then primary_combined.dv_load_date_time
            else isnull(secondary_combined.dv_load_date_time,'Jan 1, 1753')
        end dv_load_date_time,
       '99991231' dv_load_end_date_time,
       case when primary_combined.dv_batch_id >= isnull(secondary_combined.dv_batch_id,-1)
            then primary_combined.dv_batch_id
            else isnull(secondary_combined.dv_batch_id,-1)
        end dv_batch_id
  from primary_combined
  full join secondary_combined
    on primary_combined.dim_boss_reservation_key = secondary_combined.dim_boss_reservation_key
	and primary_combined.meeting_dim_date_key = secondary_combined.meeting_dim_date_key
	and primary_combined.asi_player_id = secondary_combined.asi_player_id
/* Delete and re-insert as a single transaction*/
 /*   Delete records from the table that exist*/
 /*   Insert records from records from current and missing batches*/
 
 begin tran
 
 /* Delete any records with dates >= begin_extract_date_time (date portion only) and then insert*/
 /* Go 3 months into the future as it does today.  This should be about 2 million records on average*/
   delete dbo.fact_boss_daily_roster
    where fact_boss_daily_roster.meeting_dim_date_key >= @begin_extract_dim_date_key
 
   insert into fact_boss_daily_roster
         (fact_boss_daily_roster_key,
          reservation_id,
          member_code,
          meeting_dim_date_key,
 		  check_in_dim_date_key,
 		  check_in_dim_time_key,
          asi_player_id,
          attendance_checked_in_flag,
          dim_boss_reservation_key,
          dim_mms_member_key,
          fact_mms_sales_transaction_key,
          member_flag,
          notes,
 		  paid,
 		  mms_swipe_flag,
          player_cancel_dim_date_key,
          player_checked_in_flag,
          player_start_dim_date_key,
		  instructor_type,
		  primary_dim_employee_key,
		  secondary_dim_employee_key,
          dv_load_date_time,
          dv_load_end_date_time,
          dv_batch_id,
          dv_inserted_date_time,
          dv_insert_user)
   select fact_boss_daily_roster_key,
          reservation_id,
          member_code,
          meeting_dim_date_key,
 		  check_in_dim_date_key,
 		  check_in_dim_time_key,
          asi_player_id,
          attendance_checked_in_flag,
          dim_boss_reservation_key,
          dim_mms_member_key,
          fact_mms_sales_transaction_key,
          member_flag,
          notes,
 		  paid,
 		  mms_swipe_flag,
          player_cancel_dim_date_key,
          player_checked_in_flag,
          player_start_dim_date_key,
		  instructor_type,
		  primary_dim_employee_key,
		  secondary_dim_employee_key,
          dv_load_date_time,
          dv_load_end_date_time,
          dv_batch_id,
          getdate() ,
          suser_sname()
     from #fact_boss_daily_roster
  
 commit tran
 
 end
