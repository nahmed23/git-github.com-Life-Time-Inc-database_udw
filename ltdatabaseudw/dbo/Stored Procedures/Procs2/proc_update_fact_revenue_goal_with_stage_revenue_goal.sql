CREATE PROC [dbo].[proc_update_fact_revenue_goal_with_stage_revenue_goal] @year [varchar](4) AS 

if object_id('tempdb..#temp1') is not null drop table #temp1
     create table dbo.#temp1
         with (distribution = hash (club_id),location = user_db) as
select
     club_id,
     case when stage_revenue_goal.club_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(stage_revenue_goal.club_id as int) as varchar(500)),'z#@$k%&P'))),2)
              end dim_club_key
     ,v_dim_reporting_hierarchy.dim_reporting_hierarchy_key
     ,january
     ,february
     ,march
     ,april
     ,may
     ,june
     ,july
     ,august
     ,september
     ,october
     ,november
	 ,december
from stage_revenue_goal stage_revenue_goal
     join marketing.v_dim_reporting_hierarchy v_dim_reporting_hierarchy
         ON v_dim_reporting_hierarchy.reporting_division = stage_revenue_goal.division
		 AND v_dim_reporting_hierarchy.reporting_sub_division = stage_revenue_goal.sub_division
         AND v_dim_reporting_hierarchy.reporting_department = stage_revenue_goal.revenue_department
         AND v_dim_reporting_hierarchy.reporting_product_group = stage_revenue_goal.revenue_product_group_name

/*--Inserting goal_effective_dim_date_key into fact_revenue_goal-----*/

delete from fact_revenue_goal
where fact_revenue_goal_id in (select fact_revenue_goal_id
                                 from fact_revenue_goal
                                     join #temp1
                                         on fact_revenue_goal.dim_club_key = #temp1.dim_club_key
                                         and fact_revenue_goal.dim_reporting_hierarchy_key = #temp1.dim_reporting_hierarchy_key
                                             where fact_revenue_goal.goal_effective_dim_date_key >= @year+'0101'
                               )

insert into fact_revenue_goal (
     dim_club_key,
     dim_reporting_hierarchy_key,
     goal_dollar_amount,
     goal_effective_dim_date_key,
     dv_load_date_time,
     dv_load_end_date_time,
     dv_batch_id,
     dv_inserted_date_time,
     dv_insert_user
)
/*-january*/
select dim_club_key,
       dim_reporting_hierarchy_key,
       january,
       @year+'0101',
       'jan 1, 1753',
       'dec 31, 9999',
       '-1',
       getdate(),
       suser_sname()
from #temp1

 /*february*/
 UNION
 select dim_club_key,
       dim_reporting_hierarchy_key,
       february,
       @year+'0201',
       'jan 1, 1753',
       'dec 31, 9999',
       '-1',
       getdate(),
       suser_sname()
from #temp1
/*-march*/
 UNION
 select dim_club_key,
       dim_reporting_hierarchy_key,
       march,
       @year+'0301',
       'jan 1, 1753',
       'dec 31, 9999',
       '-1',
       getdate(),
       suser_sname()
from #temp1

/*-april*/
 UNION
 select dim_club_key,
       dim_reporting_hierarchy_key,
       april,
       @year+'0401',
       'jan 1, 1753',
       'dec 31, 9999',
       '-1',
       getdate(),
       suser_sname()
from #temp1

/*-may*/
 UNION
 select dim_club_key,
       dim_reporting_hierarchy_key,
       may,
       @year+'0501',
       'jan 1, 1753',
       'dec 31, 9999',
       '-1',
       getdate(),
       suser_sname()
from #temp1

/*-june*/
 UNION
 select dim_club_key,
       dim_reporting_hierarchy_key,
       june,
       @year+'0601',
       'jan 1, 1753',
       'dec 31, 9999',
       '-1',
       getdate(),
       suser_sname()
from #temp1

/*-july*/
 UNION
 select dim_club_key,
       dim_reporting_hierarchy_key,
       july,
       @year+'0701',
       'jan 1, 1753',
       'dec 31, 9999',
       '-1',
       getdate(),
       suser_sname()
from #temp1

/*august*/
 UNION
 select dim_club_key,
       dim_reporting_hierarchy_key,
       august,
       @year+'0801',
       'jan 1, 1753',
       'dec 31, 9999',
       '-1',
       getdate(),
       suser_sname()
from #temp1

/*-september*/
 UNION
 select dim_club_key,
       dim_reporting_hierarchy_key,
       september,
       @year+'0901',
       'jan 1, 1753',
       'dec 31, 9999',
       '-1',
       getdate(),
       suser_sname()
from #temp1

/*-october*/
 UNION
 select dim_club_key,
       dim_reporting_hierarchy_key,
       october,
       @year+'1001',
       'jan 1, 1753',
       'dec 31, 9999',
       '-1',
       getdate(),
       suser_sname()
from #temp1

/*-november*/
 UNION
 select dim_club_key,
       dim_reporting_hierarchy_key,
       november,
       @year+'1101',
       'jan 1, 1753',
       'dec 31, 9999',
       '-1',
       getdate(),
       suser_sname()
from #temp1

/*-december*/
 UNION
 select dim_club_key,
       dim_reporting_hierarchy_key,
       december,
       @year+'1201',
       'jan 1, 1753',
       'dec 31, 9999',
       '-1',
       getdate(),
       suser_sname()
from #temp1


/*---Updating the goal in fact_revenue_goal table ------------*/

  UPDATE fact_revenue_goal
  SET goal_dollar_amount = ISNULL(#temp1.january,0)
  from #temp1
  WHERE fact_revenue_goal.dim_reporting_hierarchy_key = #temp1.dim_reporting_hierarchy_key
  and fact_revenue_goal.dim_club_key = #temp1.dim_club_key 
  and fact_revenue_goal.goal_effective_dim_date_key = @year+'0101'
  
  
  UPDATE fact_revenue_goal
  SET goal_dollar_amount = ISNULL(#temp1.february,0)
  from #temp1
  WHERE fact_revenue_goal.dim_reporting_hierarchy_key = #temp1.dim_reporting_hierarchy_key
  and fact_revenue_goal.dim_club_key = #temp1.dim_club_key 
  and fact_revenue_goal.goal_effective_dim_date_key = @year+'0201'
 
 
  UPDATE fact_revenue_goal
  SET goal_dollar_amount = ISNULL(#temp1.march,0)
  from #temp1
  WHERE fact_revenue_goal.dim_reporting_hierarchy_key = #temp1.dim_reporting_hierarchy_key
  and fact_revenue_goal.dim_club_key = #temp1.dim_club_key 
  and fact_revenue_goal.goal_effective_dim_date_key =  @year+'0301'
  
 
  UPDATE fact_revenue_goal
  SET goal_dollar_amount = ISNULL(#temp1.april,0)
  from #temp1
  WHERE fact_revenue_goal.dim_reporting_hierarchy_key = #temp1.dim_reporting_hierarchy_key
  and fact_revenue_goal.dim_club_key = #temp1.dim_club_key 
  and fact_revenue_goal.goal_effective_dim_date_key = @year+'0401'

  
  UPDATE fact_revenue_goal
  SET goal_dollar_amount = ISNULL(#temp1.may,0)
  from #temp1
  WHERE fact_revenue_goal.dim_reporting_hierarchy_key = #temp1.dim_reporting_hierarchy_key
  and fact_revenue_goal.dim_club_key = #temp1.dim_club_key 
  and fact_revenue_goal.goal_effective_dim_date_key = @year+'0501'
 
  
  UPDATE fact_revenue_goal
  SET goal_dollar_amount = ISNULL(#temp1.june,0)
  from #temp1
  WHERE fact_revenue_goal.dim_reporting_hierarchy_key = #temp1.dim_reporting_hierarchy_key
  and fact_revenue_goal.dim_club_key = #temp1.dim_club_key 
  and fact_revenue_goal.goal_effective_dim_date_key = @year+'0601'

 
  UPDATE fact_revenue_goal
  SET goal_dollar_amount = ISNULL(#temp1.july,0)
  from #temp1
  WHERE fact_revenue_goal.dim_reporting_hierarchy_key = #temp1.dim_reporting_hierarchy_key
  and fact_revenue_goal.dim_club_key = #temp1.dim_club_key 
  and fact_revenue_goal.goal_effective_dim_date_key = @year+'0701'

 
  UPDATE fact_revenue_goal
  SET goal_dollar_amount = ISNULL(#temp1.august,0)
  from #temp1
  WHERE fact_revenue_goal.dim_reporting_hierarchy_key = #temp1.dim_reporting_hierarchy_key
  and fact_revenue_goal.dim_club_key = #temp1.dim_club_key 
  and fact_revenue_goal.goal_effective_dim_date_key = @year+'0801'

 
  UPDATE fact_revenue_goal
  SET goal_dollar_amount = ISNULL(#temp1.september,0)
  from #temp1
  WHERE fact_revenue_goal.dim_reporting_hierarchy_key = #temp1.dim_reporting_hierarchy_key
  and fact_revenue_goal.dim_club_key = #temp1.dim_club_key 
  and fact_revenue_goal.goal_effective_dim_date_key = @year+'0901'

 
  UPDATE fact_revenue_goal
  SET goal_dollar_amount = ISNULL(#temp1.october,0)
  from #temp1
  WHERE fact_revenue_goal.dim_reporting_hierarchy_key = #temp1.dim_reporting_hierarchy_key
  and fact_revenue_goal.dim_club_key = #temp1.dim_club_key 
  and fact_revenue_goal.goal_effective_dim_date_key = @year+'1001'

 
  UPDATE fact_revenue_goal
  SET goal_dollar_amount = ISNULL(#temp1.november,0)
  from #temp1
  WHERE fact_revenue_goal.dim_reporting_hierarchy_key = #temp1.dim_reporting_hierarchy_key
  and fact_revenue_goal.dim_club_key = #temp1.dim_club_key 
  and fact_revenue_goal.goal_effective_dim_date_key = @year+'1101'

  UPDATE fact_revenue_goal
  SET goal_dollar_amount = ISNULL(#temp1.december,0)
  from #temp1
  WHERE fact_revenue_goal.dim_reporting_hierarchy_key = #temp1.dim_reporting_hierarchy_key
  and fact_revenue_goal.dim_club_key = #temp1.dim_club_key 
  and fact_revenue_goal.goal_effective_dim_date_key = @year+'1201'
  

