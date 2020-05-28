CREATE PROC [dbo].[proc_fact_revenue_goal] AS
begin

set xact_abort on
set nocount on

if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
select GoalEffectiveDimDateKey goal_effective_dim_date_key,
case when stage_factgoal.clubid is null then '-998' else dim_club.dim_club_key end dim_club_key,
GoalDollarAmount goal_dollar_amount,
OriginalCurrencyCode original_currency_code,
convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,USDMonthlyAverageDimExchangeRateKey),'z#@$k%&P'))),2) usd_monthly_average_dim_exchange_rate_key,
convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,LocalCurrencyMonthlyAverageDimExchangeRateKey),'z#@$k%&P'))),2) local_currency_monthly_average_dim_exchange_rate_key,
stage_factgoal.dim_reporting_hierarchy_key,
   stage_factgoal.dv_load_date_time,
   stage_factgoal.dv_load_end_date_time,
   stage_factgoal.dv_batch_id,
   stage_factgoal.dv_inserted_date_time,
   stage_factgoal.dv_insert_user
into #etl_step_1
from stage_factgoal
left join dim_club dim_club
on dim_club.club_id = stage_FactGoal.clubid

begin tran
     
delete dbo.fact_revenue_goal
  
	
	 insert into fact_revenue_goal
	 (
	 goal_effective_dim_date_key,
	 dim_club_key,
	 goal_dollar_amount,
	 original_currency_code,
	 usd_monthly_average_dim_exchange_rate_key,
	 local_currency_monthly_average_dim_exchange_rate_key,
	 dim_reporting_hierarchy_key,
	 dv_load_date_time,
     dv_load_end_date_time,
     dv_batch_id,
     dv_inserted_date_time,
     dv_insert_user
	 )
	 select
	 goal_effective_dim_date_key,
	 dim_club_key,
	 goal_dollar_amount,
	 original_currency_code,
	 usd_monthly_average_dim_exchange_rate_key,
	 local_currency_monthly_average_dim_exchange_rate_key,
	 dim_reporting_hierarchy_key,
	 dv_load_date_time,
     dv_load_end_date_time,
     dv_batch_id,
     dv_inserted_date_time,
     dv_insert_user
	 from #etl_step_1
	 
    commit tran
     
end
