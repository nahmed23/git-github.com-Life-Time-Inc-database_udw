CREATE PROC [dbo].[proc_fact_goal] @year [varchar](500) AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON
	

if object_id('tempdb..#temp1') is not null drop table #temp1
     create table dbo.#temp1
         with (distribution = hash (club_id),location = user_db) as
select
     clubid club_id
     ,ClubCode club_code
     ,description
     ,case when stage_FactGoal_NBOB.month_year is null then '-998'   
	  else convert(varchar, stage_FactGoal_NBOB.month_year, 112) end month_year
     ,budget
from 
     stage_FactGoal_NBOB

if object_id('tempdb..#temp2') is not null drop table #temp2
     create table dbo.#temp2
         with (distribution = hash (club_id),location = user_db) as
select
     #temp1.club_id club_id
	 ,case when #temp1.club_id is null then '-998' 
	 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,#temp1.club_id),'z#@$k%&P'))),2)  end dim_club_key
	 ,#temp1.club_code club_code
     ,#temp1.month_year goal_effective_dim_date_key
	 ,dim_goal_line_item.dim_goal_line_item_key dim_goal_line_item_key
	 ,#temp1.description  description
     ,0 goal_quantity
     ,0 percentage
     ,#temp1.budget goal_dollar_amount
     ,dim_club.local_currency_code original_currency_code
	 ,'-998' usd_monthly_average_dim_exchange_rate_key
     ,'-998' local_currency_monthly_average_dim_exchange_rate_key
     ,getdate() dv_load_date_time 
     ,'dec 31, 9999' dv_load_end_date_time 
     ,'-1' dv_batch_id
     ,getdate() dv_inserted_date_time 
     ,suser_sname() dv_insert_user 
from  #temp1
left join dim_club dim_club
on dim_club.club_id =  #temp1.club_id
left join dim_goal_line_item dim_goal_line_item
on  #temp1.description = dim_goal_line_item.description 
and dim_goal_line_item.category_description = 'pt old and new business' 

BEGIN TRAN

delete from fact_goal
where fact_goal_id in (select fact_goal_id
                                 from fact_goal
                                     join #temp2
                                         on fact_goal.dim_club_key = #temp2.dim_club_key
                                         and fact_goal.dim_goal_line_item_key = #temp2.dim_goal_line_item_key
                                             where fact_goal.goal_effective_dim_date_key >= @year
											 )
insert into fact_goal (
     club_id,
	 dim_club_key,
	 club_code,
     goal_effective_dim_date_key,
	 dim_goal_line_item_key,    
	 description,
	 goal_quantity,
	 percentage,
	 goal_dollar_amount,
	 original_currency_code,
	 usd_monthly_average_dim_exchange_rate_key,
	 local_currency_monthly_average_dim_exchange_rate_key,
     dv_load_date_time,
     dv_load_end_date_time,
     dv_batch_id,
     dv_inserted_date_time,
     dv_insert_user
)
select
      club_id,
	 dim_club_key,
	 club_code,
     goal_effective_dim_date_key,
	 dim_goal_line_item_key,    
	 description,
	 goal_quantity,
	 percentage,
	 goal_dollar_amount,
	 original_currency_code,
	 usd_monthly_average_dim_exchange_rate_key,
	 local_currency_monthly_average_dim_exchange_rate_key,
     dv_load_date_time,
     dv_load_end_date_time,
     dv_batch_id,
     dv_inserted_date_time,
     dv_insert_user
from 
    #temp2
	


update fact_goal
set goal_dollar_amount = #temp2.goal_dollar_amount 
from #temp2
where fact_goal.goal_effective_dim_date_key = #temp2.goal_effective_dim_date_key
and fact_goal.dim_goal_line_item_key = #temp2.dim_goal_line_item_key
and fact_goal.club_id =  #temp2.club_id

COMMIT TRAN
END
