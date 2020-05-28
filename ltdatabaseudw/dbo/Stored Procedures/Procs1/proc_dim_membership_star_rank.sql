CREATE PROC [dbo].[proc_dim_membership_star_rank] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

if object_id('tempdb..#etl_step1') is not null drop table #etl_step1
create table dbo.#etl_step1 with(distribution=hash(dim_mms_member_key), location=user_db) as
select fact_combined_member_spend.dim_mms_member_key
	,dim_mms_member_tenure.member_id
	,dim_mms_member_tenure.customer_name
	,dim_mms_member_tenure.first_name
	,dim_mms_member_tenure.last_name
	,dim_mms_member_tenure.gender
	,dim_mms_member_tenure.join_date
	,dim_mms_member_tenure.membership_id
	,case when dim_club.marketing_club_level in ('Diamond','Diamond Premier') then 'Diamond'
	      when dim_club.marketing_club_level in ('Onyx','Onyx Limited','Onyx Premier') then 'Onyx'
		  else dim_club.marketing_club_level end as marketing_club_level
	,SUM(last_12_month_spend_amount)  OVER (PARTITION BY membership_id)  last_12_month_spend_amount
	,SUM(total_spend_amount)  OVER (PARTITION BY membership_id) total_spend_amount
from fact_combined_member_spend
join dim_club
on fact_combined_member_spend.home_dim_club_key=dim_club.dim_club_key
join [marketing].[v_dim_mms_member_tenure] dim_mms_member_tenure
ON fact_combined_member_spend.dim_mms_member_key=dim_mms_member_tenure.dim_mms_member_key
where dim_mms_member_tenure.member_active_flag='Y' 
and dim_club.marketing_club_level in ('Bronze','Diamond','Diamond Premier','Gold','Onyx',
'Onyx Limited','Onyx Premier','Platinum')

if object_id('tempdb..#etl_step2') is not null drop table #etl_step2
create table dbo.#etl_step2 with(distribution=hash(dim_mms_member_key), location=user_db) as
select dim_mms_member_key,member_id,customer_name,first_name,last_name,gender,join_date,membership_id 
,case when marketing_club_level='Bronze' AND last_12_month_spend_amount >= 750 THEN 4
      when marketing_club_level='Gold' AND last_12_month_spend_amount >= 1250 THEN 4
	  when marketing_club_level='Platinum' AND last_12_month_spend_amount >= 1500 THEN 4
	  when marketing_club_level='Onyx' AND last_12_month_spend_amount >= 1750 THEN 4
	  when marketing_club_level='Diamond' AND last_12_month_spend_amount >= 2500 THEN 4
 END AS val_star_rank_id
from dbo.#etl_step1 etl_step1 WHERE last_12_month_spend_amount >=750
union all
select dim_mms_member_key,member_id,customer_name,first_name,last_name,gender,join_date,membership_id 
,case when marketing_club_level='Bronze' AND last_12_month_spend_amount >= 1500 THEN 5
      when marketing_club_level='Gold' AND last_12_month_spend_amount >= 2200  THEN 5
	  when marketing_club_level='Platinum' AND last_12_month_spend_amount >= 2500  THEN 5
	  when marketing_club_level='Onyx' AND last_12_month_spend_amount >= 3500  THEN 5
	  when marketing_club_level='Diamond' AND last_12_month_spend_amount >= 5000  THEN 5
 END AS val_star_rank_id
from dbo.#etl_step1 etl_step1 WHERE last_12_month_spend_amount >=1500
union all
select dim_mms_member_key,member_id,customer_name,first_name,last_name,gender,join_date,membership_id 
,case when marketing_club_level='Bronze' AND total_spend_amount >= 15000  THEN 3
      when marketing_club_level='Gold' AND total_spend_amount >= 15000   THEN 3
	  when marketing_club_level='Platinum' AND total_spend_amount >= 20000   THEN 3
	  when marketing_club_level='Onyx' AND total_spend_amount >= 20000   THEN 3
	  when marketing_club_level='Diamond' AND total_spend_amount >= 25000   THEN 3
END AS val_star_rank_id
from dbo.#etl_step1 etl_step1 WHERE total_spend_amount >=15000

if object_id('tempdb..#etl_step3') is not null drop table #etl_step3
create table dbo.#etl_step3 with(distribution=hash(dim_mms_member_key), location=user_db) as
SELECT dim_mms_member_key
	,member_id
	,customer_name
	,first_name
	,last_name
	,gender
	,join_date
	,membership_id
	,1 AS val_star_rank_id
FROM [marketing].[v_dim_mms_member_tenure]
WHERE member_active_flag = 'Y'
UNION ALL
SELECT dim_mms_member_key
	,member_id
	,customer_name
	,first_name
	,last_name
	,gender
	,join_date
	,membership_id
	,2 AS val_star_rank_id
FROM [marketing].[v_dim_mms_member_tenure]
WHERE member_active_flag = 'Y'
	AND is_tenure_more_than_12_months = 'Y'	
UNION ALL
SELECT dim_mms_member_key
	,member_id
	,customer_name
	,first_name
	,last_name
	,gender
	,join_date
	,membership_id
	,val_star_rank_id
from dbo.#etl_step2 where val_star_rank_id is not null


truncate table dbo.dim_membership_star_rank;

begin tran

   insert into dim_membership_star_rank
        (dim_mms_member_key
        ,member_id
	    ,customer_name
	    ,first_name
	    ,last_name
	    ,gender
	    ,join_date
	    ,membership_id
	    ,val_star_rank_id
        ,dv_load_date_time
        ,dv_load_end_date_time
        ,dv_batch_id
        ,dv_inserted_date_time
        ,dv_insert_user)
  select dim_mms_member_key
        ,member_id
	    ,customer_name
	    ,first_name
	    ,last_name
	    ,gender
	    ,join_date
	    ,membership_id
	    ,val_star_rank_id
        ,'Jan 1,1900'
		,'Dec 31, 9999'
        ,@dv_batch_id
        ,getdate() 
        ,suser_sname()
    from #etl_step3
 
commit tran

end

