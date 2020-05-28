CREATE PROC [dbo].[proc_dim_spabiz_service_bkp] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
exec dbo.proc_util_task_status_insert 'proc_dim_spabiz_service','proc_dim_spabiz_service start',@current_dv_batch_id

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
exec dbo.proc_util_task_status_insert 'proc_dim_spabiz_service','#dim_spabiz_service',@current_dv_batch_id

if object_id('tempdb..#dim_spabiz_service') is not null drop table #dim_spabiz_service
create table dbo.#dim_spabiz_service with(distribution=round_robin, location=user_db, heap) as
select p_spabiz_service.bk_hash dim_spabiz_service_key,
       p_spabiz_service.service_id service_id,
       p_spabiz_service.store_number store_number,
       case when s_spabiz_service.book_name is null then ''
            else s_spabiz_service.book_name
        end book_name,
       s_spabiz_service.call_after_x_days call_after_x_days,
       case when s_spabiz_service.is_color_balance = 1 then 'Y'
            else 'N'
        end color_balance_flag,
       case when s_spabiz_service.cost is null then 0
            else s_spabiz_service.cost
        end cost,
       s_spabiz_service.cost_type,
       s_spabiz_service.date_created created_date_time,
       case when p_spabiz_service.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_service.delete_date = convert(date, '18991230', 112) then null
            else s_spabiz_service.delete_date
        end deleted_date_time,
       case when s_spabiz_service.service_delete = -1 then 'Y'
            else 'N'
        end deleted_flag,
       case when p_spabiz_service.bk_hash in ('-997','-998','-999') then p_spabiz_service.bk_hash
            when l_spabiz_service.dept_cat is null then '-998'
            when l_spabiz_service.dept_cat = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_service.dept_cat as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_service.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_category_key,
       case when p_spabiz_service.bk_hash in ('-997','-998','-999') then p_spabiz_service.bk_hash
            when l_spabiz_service.store_number is null then '-998'
            when l_spabiz_service.store_number = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_service.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_store_key,
       s_spabiz_service.edit_time edit_date_time,
       s_spabiz_service.finish finish,
       case when s_spabiz_service.cost_type = 0 then 'Y'
            else 'N'
        end fixed_currency_amount_flag,
       case when l_spabiz_service.gl_account is null then 0 
            else l_spabiz_service.gl_account
        end gl_account,
       case when s_spabiz_service.is_hilite_procedure = 1 then 'Y'
            else 'N'
        end highlight_procedure_flag,
       s_spabiz_service.new_extra_time new_customer_extra_time,
       case when s_spabiz_service.pay_comish = 1 then 'Y'
            else 'N'
        end pay_commission_flag,
       case when s_spabiz_service.cost_type = 1 then 'Y'
            else 'N'
        end percent_of_total_price_flag,
       case when s_spabiz_service.quick_id is null then ''
            else s_spabiz_service.quick_id
        end quick_id,
       case when s_spabiz_service.require_staff = 1 then 'Y'
            else 'N'
        end require_staff_flag,
       s_spabiz_service.resource_count resource_count,
       s_spabiz_service.retail_price retail_price,
       s_spabiz_service.service_level service_level,
       case when s_spabiz_service.name is null then ''
            else s_spabiz_service.name
        end service_name,
       case when s_spabiz_service.description is null then ''
            else s_spabiz_service.description
        end service_description,
       s_spabiz_service.process service_process,
       s_spabiz_service.time service_time,
       case when s_spabiz_service.taxable = 1 then 'Y'
            else 'N'
        end taxable_flag,
       case when s_spabiz_service.web_book = 1 then 'Y'
            else 'N'
        end web_book_flag,
       case when s_spabiz_service.web_view = 1 then 'Y'
            else 'N'
        end web_view_flag,
	
       p_spabiz_service.p_spabiz_service_id,
       p_spabiz_service.dv_batch_id,
       p_spabiz_service.dv_load_date_time,
       p_spabiz_service.dv_load_end_date_time
  from dbo.p_spabiz_service
  join dbo.l_spabiz_service
    on p_spabiz_service.l_spabiz_service_id = l_spabiz_service.l_spabiz_service_id
  join dbo.s_spabiz_service
    on p_spabiz_service.s_spabiz_service_id = s_spabiz_service.s_spabiz_service_id
 where p_spabiz_service.dv_load_end_date_time = '9999-12-31 00:00:00.000'
   and (l_spabiz_service.store_number not in (1,100,999) OR p_spabiz_service.bk_hash in ('-999','-998','-997'))

--(152156 row(s) affected)
---101070
    
   
 if object_id('tempdb..#sandbox_service_mapping') is not null drop table #sandbox_service_mapping
create table dbo.#sandbox_service_mapping with(distribution=hash(dim_spabiz_service_key), location=user_db, heap) as
 select 
          p_spabiz_service.bk_hash dim_spabiz_service_key,
	      s_sandbox_service_mapping.category,
	  	  s_sandbox_service_mapping.segment,
		  s_sandbox_service_mapping.commission_mapping,
          p_sandbox_service_mapping.dv_load_end_date_time,
          p_sandbox_service_mapping.dv_batch_id,
          p_sandbox_service_mapping.dv_load_date_time  
     from p_spabiz_service
     join l_spabiz_service
       on p_spabiz_service.l_spabiz_service_id = l_spabiz_service.l_spabiz_service_id
     join s_spabiz_service
       on p_spabiz_service.s_spabiz_service_id = s_spabiz_service.s_spabiz_service_id
      and p_spabiz_service.dv_load_end_date_time = '9999-12-31 00:00:00.000'
      and (l_spabiz_service.store_number not in (1,100,999) OR p_spabiz_service.bk_hash in ('-999','-998','-997'))
   
     join l_sandbox_service_mapping
       on p_spabiz_service.service_id = l_sandbox_service_mapping.service_id
      and p_spabiz_service.store_number = l_sandbox_service_mapping.store_number   
     join p_sandbox_service_mapping
       on l_sandbox_service_mapping.l_sandbox_service_mapping_id = p_sandbox_service_mapping.l_sandbox_service_mapping_id
     join s_sandbox_service_mapping
       on p_sandbox_service_mapping.s_sandbox_service_mapping_id = s_sandbox_service_mapping.s_sandbox_service_mapping_id	  
      and p_sandbox_service_mapping.dv_load_end_date_time = 'Dec 31, 9999' 
	  

--101070
 	   
--service_name -> category parsing
if object_id('tempdb..#sc') is not null drop table #sc
create table #sc (dim_spabiz_service_key char(32), i int, cat_name varchar(500))

if object_id('tempdb..#s') is not null drop table #s
select dim_spabiz_service_key,
       service_name new_name
into #s
from #dim_spabiz_service
where charindex('>',service_name) > 0

declare @c int = 1

while (select count(*) from #s where new_name like '%>%') > 0 
begin

    insert into #sc
    select dim_spabiz_service_key,@c, substring(new_name,1,charindex('>',new_name)-1)
    from #s
    where charindex('>',new_name) > 0

    update #s
       set new_name = substring(new_name,charindex('>',new_name)+1,len(new_name))
    where charindex('>',new_name) > 0
  set @c = @c+1

end

if object_id('tempdb..#service_category') is not null drop table #service_category
select sc.dim_spabiz_service_key,
       max(case when i = 1 then cat_name else null end) level_1_service_category,
       max(case when i = 2 then cat_name else null end) level_2_service_category,
       max(case when i = 3 then cat_name else null end) level_3_service_category
into #service_category
from #sc sc
group by sc.dim_spabiz_service_key
--parsed out!


truncate table dim_spabiz_service

      insert dbo.dim_spabiz_service(
                 dim_spabiz_service_key,
                 service_id,
                 store_number,
                 book_name,
                 call_after_x_days,
                 color_balance_flag,
                 cost,
                 cost_type,
                 created_date_time,
                 deleted_date_time,
                 deleted_flag,
                 dim_spabiz_category_key,
                 dim_spabiz_store_key,
                 edit_date_time,
                 finish,
                 fixed_currency_amount_flag,
                 gl_account,
                 highlight_procedure_flag,
                 new_customer_extra_time,
                 pay_commission_flag,
                 percent_of_total_price_flag,
                 quick_id,
                 require_staff_flag,
                 resource_count,
                 retail_price,
                 service_level,
                 service_name,
                 service_description,
                 service_process,
                 service_time,
                 spabiz_category,
                 level_1_service_category,
                 level_2_service_category,
                 level_3_service_category,
                 taxable_flag,
                 web_book_flag,
                 web_view_flag,
				 category,
				 segment,
				 commission_mapping,
                 p_spabiz_service_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
		  select #dim_spabiz_service.dim_spabiz_service_key,
				 service_id,
				 store_number,
				 book_name,
				 call_after_x_days,
				 color_balance_flag,
				 cost,
				 cost_type,
				 created_date_time,
				 deleted_date_time,
				 deleted_flag,
				 #dim_spabiz_service.dim_spabiz_category_key,
				 dim_spabiz_store_key,
				 edit_date_time,
				 finish,
				 fixed_currency_amount_flag,
				 gl_account,
				 highlight_procedure_flag,
				 new_customer_extra_time,
				 pay_commission_flag,
				 percent_of_total_price_flag,
				 quick_id,
				 require_staff_flag,
				 resource_count,
				 retail_price,
				 service_level,
				 service_name,
				 service_description,
				 service_process,
				 service_time,
				 v_dim_spabiz_sub_category.level_1_name spabiz_category,
				 #service_category.level_1_service_category,
				 #service_category.level_2_service_category,
				 #service_category.level_3_service_category,
				 taxable_flag,
				 web_book_flag,
				 web_view_flag,
				 #sandbox_service_mapping.category,
           		 #sandbox_service_mapping.segment,	
                 #sandbox_service_mapping.commission_mapping,				 
				 p_spabiz_service_id,
                 case when isnull(#dim_spabiz_service.dv_load_date_time,'') > isnull(#sandbox_service_mapping.dv_load_date_time,'')
                      then isnull(#dim_spabiz_service.dv_load_date_time,'')
                      else isnull(#sandbox_service_mapping.dv_load_date_time,'') 
            	 end dv_load_date_time,
                 case when isnull(#dim_spabiz_service.dv_load_end_date_time,'') > isnull(#sandbox_service_mapping.dv_load_end_date_time,'')
                      then isnull(#dim_spabiz_service.dv_load_end_date_time,'')
                      else isnull(#sandbox_service_mapping.dv_load_end_date_time,'') 
            	 end dv_load_end_date_time,
                 case when #dim_spabiz_service.dv_batch_id > isnull(#sandbox_service_mapping.dv_batch_id,'-2')
                      then #dim_spabiz_service.dv_batch_id
                      else isnull(#sandbox_service_mapping.dv_batch_id,'') 
            	 end dv_batch_id,	 
				 getdate(),
				 suser_sname()
			from #dim_spabiz_service
			left join #sandbox_service_mapping
			  on #dim_spabiz_service.dim_spabiz_service_key = #sandbox_service_mapping.dim_spabiz_service_key		
			left join #service_category
			  on #dim_spabiz_service.dim_spabiz_service_key = #service_category.dim_spabiz_service_key
			left join marketing.v_dim_spabiz_sub_category
			  on #dim_spabiz_service.dim_spabiz_category_key = v_dim_spabiz_sub_category.dim_spabiz_category_key


--Done!

end
