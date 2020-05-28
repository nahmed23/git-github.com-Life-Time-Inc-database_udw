CREATE PROC [dbo].[proc_wrk_boss_reservation_meeting_date] @begin_extract_date_time [varchar](500) AS
 -- Input parameter: @begin_extract_date_time varchar(100) --$$begin_extract_date_time
 
 
begin

set xact_abort on
set nocount on


 -- Date portion of @begin_extract_date_time
declare @begin_extract_dim_date_key char(8) = (convert(char(8), convert(datetime, @begin_extract_date_time, 120), 112))
 -- Calculate the max rebuild date as today plus 90 days
declare @max_rebuild_dim_date_key char(8) = (select convert(char(8), dateadd(d,90,getdate()), 112))


 -- Note that the following small queries are used due to inefficiencies in multi-tables queries using greater than or less than comparison and for logic readability

if object_id('tempdb..#d_boss_asi_reserv') is not null drop table #d_boss_asi_reserv
create table #d_boss_asi_reserv with(distribution=hash(dim_boss_reservation_key),location=user_db) as
select d_boss_asi_reserv.dim_boss_reservation_key,
       d_boss_asi_reserv.reservation_id,
	   d_boss_asi_reserv.dim_employee_key,
       d_boss_asi_reserv.start_dim_date_key,
       d_boss_asi_reserv.end_dim_date_key,
       d_boss_asi_reserv.day_plan_ints,
       d_boss_asi_reserv.dv_load_date_time,
       d_boss_asi_reserv.dv_batch_id
  from d_boss_asi_reserv
 where d_boss_asi_reserv.start_dim_date_key <= @max_rebuild_dim_date_key
   and d_boss_asi_reserv.end_dim_date_key >= @begin_extract_dim_date_key
   and d_boss_asi_reserv.reservation_status = 'A'
   ---and d_boss_asi_reserv.reservation_type in ('A','E','C','G')

 -- Get the set of dates to rebuild
if object_id('tempdb..#dim_date') is not null drop table #dim_date
create table #dim_date with(distribution=hash(dim_date_key),location=user_db) as
select dim_date.dim_date_key,
       dim_date.day_number_in_week
  from dim_date
 where dim_date.dim_date_key >= @begin_extract_dim_date_key
   and dim_date.dim_date_key <= @max_rebuild_dim_date_key

 -- Take all reservations active during the rebuild period
 -- and multiply out by the reservation day plan days
 --   d_boss_asi_reserv.day_plan_ints 1 = Sunday
 --   dim_date.day_number_in_week 1 = Sunday
if object_id('tempdb..#d_boss_asi_reserv_meeting_date') is not null drop table #d_boss_asi_reserv_meeting_date
create table #d_boss_asi_reserv_meeting_date with(distribution=hash(dim_boss_reservation_key),location=user_db) as
select #d_boss_asi_reserv.dim_boss_reservation_key,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#d_boss_asi_reserv.dim_boss_reservation_key,'z#@$k%&P')
										+'P%#&z$@k'+isnull(#dim_date.dim_date_key,'z#@$k%&P'))),2) dim_boss_reservation_meeting_dim_date_key,
       #d_boss_asi_reserv.reservation_id,
	   #d_boss_asi_reserv.dim_employee_key,
	   #d_boss_asi_reserv.start_dim_date_key,
	   #d_boss_asi_reserv.end_dim_date_key,
	   #dim_date.dim_date_key meeting_dim_date_key,
       #d_boss_asi_reserv.dv_load_date_time,
       #d_boss_asi_reserv.dv_batch_id
  from #d_boss_asi_reserv
  join #dim_date
    on #d_boss_asi_reserv.start_dim_date_key <= #dim_date.dim_date_key
   and #d_boss_asi_reserv.end_dim_date_key >= #dim_date.dim_date_key
   and charindex(convert(varchar, #dim_date.day_number_in_week), #d_boss_asi_reserv.day_plan_ints) != 0
   
-- Using res_cancel delete records for the reservation/date combination from wrk_boss_reservation_meeting_date
delete #d_boss_asi_reserv_meeting_date
 where #d_boss_asi_reserv_meeting_date.dim_boss_reservation_key + '_' + #d_boss_asi_reserv_meeting_date.meeting_dim_date_key
    in (select #d_boss_asi_reserv_meeting_date.dim_boss_reservation_key + '_' + #d_boss_asi_reserv_meeting_date.meeting_dim_date_key
          from #d_boss_asi_reserv_meeting_date
          join d_boss_res_cancel
            on #d_boss_asi_reserv_meeting_date.dim_boss_reservation_key = d_boss_res_cancel.dim_boss_reservation_key
           and #d_boss_asi_reserv_meeting_date.meeting_dim_date_key = d_boss_res_cancel.cancel_dim_date_key) 
		   
		   
if object_id('tempdb.dbo.#d_boss_asi_res_inst') is not null drop table #d_boss_asi_res_inst
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
  join #d_boss_asi_reserv
    on d_boss_asi_res_inst.dim_boss_reservation_key = #d_boss_asi_reserv.dim_boss_reservation_key
	
	-- There may be more than one instructor associated with a participation record.  
 -- In that case use substitue (S) over primary (P).  
 -- Note that Assistants (A) are only for the secondary instructor.  
 -- Since S comes alphabetically before P and P before A we can just use a descending sort to determine the rank of the primary instructor.
 -- In case there are multiples order by latest start date then earliest end date then latest reservation instructor record id.
 -- Since A comes alphabetically before P and S we can use an ascending sort to determine the rank of the associate instructor.
 -- Note that there may not be an asi_res_inst record so use a left join.
if object_id('tempdb.dbo.#combined') is not null drop table #combined
create table dbo.#combined with(distribution = hash(dim_boss_reservation_key), location=user_db) as
select #d_boss_asi_reserv_meeting_date.dim_boss_reservation_key dim_boss_reservation_key,
       #d_boss_asi_reserv_meeting_date.dim_boss_reservation_meeting_dim_date_key dim_boss_reservation_meeting_dim_date_key,
       #d_boss_asi_reserv_meeting_date.reservation_id reservation_id,
	   #d_boss_asi_reserv_meeting_date.start_dim_date_key start_dim_date_key,
	   #d_boss_asi_reserv_meeting_date.end_dim_date_key end_dim_date_key,
       #d_boss_asi_reserv_meeting_date.meeting_dim_date_key meeting_dim_date_key,
       #d_boss_asi_reserv_meeting_date.dim_employee_key reserv_dim_employee_key,
       #d_boss_asi_res_inst.dim_employee_key res_inst_dim_employee_key,
	   #d_boss_asi_res_inst.instructor_start_dim_date_key instructor_start_dim_date_key,
	   #d_boss_asi_res_inst.instructor_end_dim_date_key instructor_end_dim_date_key,
       #d_boss_asi_res_inst.instructor_type instructor_type,
       case when #d_boss_asi_reserv_meeting_date.dv_load_date_time >= isnull(#d_boss_asi_res_inst.dv_load_date_time,'Jan 1, 1753')
            then #d_boss_asi_reserv_meeting_date.dv_load_date_time
           else isnull(#d_boss_asi_res_inst.dv_load_date_time,'Jan 1, 1753')
             end dv_load_date_time,
       case when #d_boss_asi_reserv_meeting_date.dv_batch_id >= isnull(#d_boss_asi_res_inst.dv_batch_id,-1)
            then #d_boss_asi_reserv_meeting_date.dv_batch_id
           else isnull(#d_boss_asi_res_inst.dv_batch_id,-1)
        end dv_batch_id,
       row_number() over(partition by #d_boss_asi_reserv_meeting_date.dim_boss_reservation_key, #d_boss_asi_reserv_meeting_date.meeting_dim_date_key
               	   order by #d_boss_asi_res_inst.instructor_type desc, #d_boss_asi_res_inst.instructor_start_dim_date_key desc,
				   #d_boss_asi_res_inst.instructor_end_dim_date_key, #d_boss_asi_res_inst.asi_res_inst_id desc) primary_rank,
       row_number() over(partition by #d_boss_asi_reserv_meeting_date.dim_boss_reservation_key, #d_boss_asi_reserv_meeting_date.meeting_dim_date_key 
	                order by #d_boss_asi_res_inst.instructor_type, #d_boss_asi_res_inst.instructor_start_dim_date_key desc,
					#d_boss_asi_res_inst.instructor_end_dim_date_key, #d_boss_asi_res_inst.asi_res_inst_id desc) secondary_rank
  into #combined
  from #d_boss_asi_reserv_meeting_date
    left join #d_boss_asi_res_inst
    on #d_boss_asi_reserv_meeting_date.dim_boss_reservation_key = #d_boss_asi_res_inst.dim_boss_reservation_key
   and #d_boss_asi_reserv_meeting_date.meeting_dim_date_key >= #d_boss_asi_res_inst.instructor_start_dim_date_key
   and #d_boss_asi_reserv_meeting_date.meeting_dim_date_key <= #d_boss_asi_res_inst.instructor_end_dim_date_key
   

 -- Combine the primary data with the optional secondary data
 -- Note that a full outer join is required since there are a few cases where there is an A record, but not a P or S.
 -- Therefore all returned columns require a case statement to determine if there is a primary record

if object_id('tempdb..#wrk_boss_reservation_meeting_date') is not null drop table #wrk_boss_reservation_meeting_date
create table dbo.#wrk_boss_reservation_meeting_date with(distribution = hash(dim_boss_reservation_key), location=user_db) as
with 
  primary_combined (dim_boss_reservation_meeting_dim_date_key, dim_boss_reservation_key, reservation_id, start_dim_date_key,end_dim_date_key,
                   meeting_dim_date_key, reserv_dim_employee_key, res_inst_dim_employee_key, instructor_type, dv_load_date_time, dv_batch_id) as
      (select #combined.dim_boss_reservation_meeting_dim_date_key,
	          #combined.dim_boss_reservation_key,
              #combined.reservation_id,
              #combined.start_dim_date_key,
              #combined.end_dim_date_key,
              #combined.meeting_dim_date_key,
              #combined.reserv_dim_employee_key,
              #combined.res_inst_dim_employee_key,
              #combined.instructor_type,
              #combined.dv_load_date_time,
              #combined.dv_batch_id
         from #combined
        where #combined.primary_rank = 1
          and (#combined.instructor_type in ('S','P')
               or #combined.instructor_type is null)),
  secondary_combined (dim_boss_reservation_meeting_dim_date_key, dim_boss_reservation_key, reservation_id, start_dim_date_key,end_dim_date_key,
                   meeting_dim_date_key, reserv_dim_employee_key, res_inst_dim_employee_key, instructor_type, dv_load_date_time, dv_batch_id) as
      (select #combined.dim_boss_reservation_meeting_dim_date_key,
	          #combined.dim_boss_reservation_key,
              #combined.reservation_id,
              #combined.start_dim_date_key,
              #combined.end_dim_date_key,
              #combined.meeting_dim_date_key,
              #combined.reserv_dim_employee_key,
              #combined.res_inst_dim_employee_key,
              #combined.instructor_type,
              #combined.dv_load_date_time,
              #combined.dv_batch_id
         from #combined
        where #combined.secondary_rank = 1
          and #combined.instructor_type = 'A')
		  
select isnull(primary_combined.dim_boss_reservation_key, secondary_combined.dim_boss_reservation_key) dim_boss_reservation_key,
       isnull(primary_combined.dim_boss_reservation_meeting_dim_date_key, secondary_combined.dim_boss_reservation_meeting_dim_date_key) dim_boss_reservation_meeting_dim_date_key,
       case when primary_combined.dim_boss_reservation_key is not null
            then primary_combined.reservation_id
            else secondary_combined.reservation_id
        end reservation_id,
       case when primary_combined.dim_boss_reservation_key is not null
            then primary_combined.start_dim_date_key
            else secondary_combined.start_dim_date_key
        end start_dim_date_key,
       case when primary_combined.dim_boss_reservation_key is not null
            then primary_combined.end_dim_date_key
            else secondary_combined.end_dim_date_key
        end end_dim_date_key,
       case when primary_combined.dim_boss_reservation_key is not null
            then primary_combined.meeting_dim_date_key
            else secondary_combined.meeting_dim_date_key
        end meeting_dim_date_key,
       case when primary_combined.dim_boss_reservation_key is not null
            then isnull(primary_combined.res_inst_dim_employee_key, primary_combined.reserv_dim_employee_key)
            else secondary_combined.res_inst_dim_employee_key
        end primary_dim_employee_key,
       case when primary_combined.dim_boss_reservation_key is not null
            then isnull(primary_combined.instructor_type, 'P')
            else secondary_combined.instructor_type
        end instructor_type,
       case when primary_combined.dim_boss_reservation_key is not null
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


   

begin tran

-- Delete any records with dates >= begin_extract_date_time (date portion only) and then insert
-- Go 3 months into the future as it does today.  This should be about 2 million records on average

  delete dbo.wrk_boss_reservation_meeting_date
   where wrk_boss_reservation_meeting_date.meeting_dim_date_key >= @begin_extract_dim_date_key
   
   

	INSERT INTO wrk_boss_reservation_meeting_date (
     dim_boss_reservation_key
	,dim_boss_reservation_meeting_dim_date_key
    ,reservation_id
	,start_dim_date_key
	,end_dim_date_key
	,meeting_dim_date_key
	,primary_dim_employee_key
	,instructor_type
	,secondary_dim_employee_key 
	,dv_load_date_time
	,dv_load_end_date_time
	,dv_batch_id
	,dv_inserted_date_time
	,dv_insert_user
		)
	SELECT dim_boss_reservation_key
	    ,dim_boss_reservation_meeting_dim_date_key
        ,reservation_id
	    ,start_dim_date_key
	    ,end_dim_date_key
	    ,meeting_dim_date_key
	    ,primary_dim_employee_key
	    ,instructor_type
	    ,secondary_dim_employee_key 
	    ,dv_load_date_time
	    ,convert(datetime, '99991231', 112)
	    ,dv_batch_id
	    ,getdate()
	    ,suser_sname()
	FROM #wrk_boss_reservation_meeting_date

	COMMIT TRAN
			
END
 
