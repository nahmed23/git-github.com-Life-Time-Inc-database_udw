CREATE PROC [dbo].[proc_fact_spabiz_non_member_summary] @dv_batch_id [bigint] AS
begin

set xact_abort on
set nocount on

--year data
create table #ticket_item_category_totals with(distribution = hash(dim_spabiz_customer_key)) as
select dim_spabiz_customer_key,
       isnull(s.category,'') category,
       case when dt.data_type_name = 'service' then 'service' else 'non-service' end data_type,
       sum(td.product_amount) yearly_product_amount,
       sum(td.product_quantity) yearly_product_quantity,
       sum(td.service_amount) yearly_service_amount,
       sum(td.service_quantity) yearly_service_quantity,
       max(td.ticket_item_date_time) max_date_time
  from d_spabiz_ticket_data td
  join dim_spabiz_service s
    on td.dim_spabiz_service_key = s.dim_spabiz_service_key
  join d_spabiz_data_type dt
    on td.dim_spabiz_data_type_key = dt.dim_spabiz_data_type_key
 where td.status_id = 1
   and td.ticket_item_date_time >= dateadd(dd,-366,convert(date,getdate())) 
   and td.dim_spabiz_customer_key <> '-998'
 group by dim_spabiz_customer_key, isnull(s.category,''), case when dt.data_type_name = 'service' then 'service' else 'non-service' end
 
create table #ticket_item_year_totals with(distribution = hash(dim_spabiz_customer_key)) as
select dim_spabiz_customer_key,
       sum(yearly_product_amount) yearly_product_amount,
       sum(yearly_product_quantity) yearly_product_quantity,
       sum(yearly_service_amount) yearly_service_amount,
       sum(yearly_service_quantity) yearly_service_quantity,
       sum(case when data_type = 'service' and category = 'body' then yearly_service_amount else 0 end) yearly_body_service_amount,
       sum(case when data_type = 'service' and category = 'body' then yearly_service_quantity else 0 end) yearly_body_service_quantity,
       sum(case when data_type = 'service' and category = 'hair' then yearly_service_amount else 0 end) yearly_hair_service_amount,
       sum(case when data_type = 'service' and category = 'hair' then yearly_service_quantity else 0 end) yearly_hair_service_quantity,
       sum(case when data_type = 'service' and category = 'skin' then yearly_service_amount else 0 end) yearly_skin_service_amount,
       sum(case when data_type = 'service' and category = 'skin' then yearly_service_quantity else 0 end) yearly_skin_service_quantity,
       sum(case when data_type = 'service' and category = 'nail' then yearly_service_amount else 0 end) yearly_nail_service_amount,
       sum(case when data_type = 'service' and category = 'nail' then yearly_service_quantity else 0 end) yearly_nail_service_quantity,
       sum(case when data_type = 'service' and category = 'medi' then yearly_service_amount else 0 end) yearly_medi_service_amount,
       sum(case when data_type = 'service' and category = 'medi' then yearly_service_quantity else 0 end) yearly_medi_service_quantity
  from #ticket_item_category_totals 
 group by dim_spabiz_customer_key
 
 --"most recent" (rank = 1)
 create table #last_purchase with(distribution = hash(dim_spabiz_customer_key)) as
 select #ticket_item_category_totals.dim_spabiz_customer_key,
        #ticket_item_category_totals.category,
        #ticket_item_category_totals.data_type,
        td.ticket_item_date_time,
        td.fact_spabiz_ticket_item_key,
        rank() over (partition by #ticket_item_category_totals.dim_spabiz_customer_key, #ticket_item_category_totals.category, #ticket_item_category_totals.data_type order by td.fact_spabiz_ticket_item_key) category_rank,
        rank() over (partition by #ticket_item_category_totals.dim_spabiz_customer_key order by td.ticket_item_date_time desc,td.fact_spabiz_ticket_item_key) customer_rank
  from d_spabiz_ticket_data td
  join dim_spabiz_service s
    on td.dim_spabiz_service_key = s.dim_spabiz_service_key
  join d_spabiz_data_type dt
    on td.dim_spabiz_data_type_key = dt.dim_spabiz_data_type_key
  join #ticket_item_category_totals
    on td.dim_spabiz_customer_key = #ticket_item_category_totals.dim_spabiz_customer_key
   and 'service' = #ticket_item_category_totals.data_type
   and isnull(s.category,'') = isnull(#ticket_item_category_totals.category,'')
   and td.ticket_item_date_time = #ticket_item_category_totals.max_date_time
 where td.status_id = 1
   and dt.data_type_id = 5

delete from #last_purchase where category_rank <> 1 and customer_rank <> 1


create table #future_ap with(distribution = hash(dim_spabiz_customer_key)) as
select ad.dim_spabiz_customer_key,
       s.category,
       min(ad.start_date_time) next_start_date_time
  from d_spabiz_ap_data ad
  join dim_spabiz_service s
    on ad.dim_spabiz_service_key = s.dim_spabiz_service_key
 where ad.deleted_flag = 'N'
   and ad.data_type_id = 5
   and ad.dim_spabiz_customer_key <> '-998'
   and ad.start_date_time >= convert(date,getdate())
 group by ad.dim_spabiz_customer_key, s.category
 

 --next appointment (rank = 1)
 create table #next_ap with(distribution = hash(dim_spabiz_customer_key)) as
 select #future_ap.dim_spabiz_customer_key,
        #future_ap.category,
        ad.start_date_time,
        ad.fact_spabiz_appointment_item_key,
        rank() over (partition by #future_ap.dim_spabiz_customer_key, #future_ap.category order by ad.start_date_time asc, ad.fact_spabiz_appointment_item_key) category_rank,
        rank() over (partition by #future_ap.dim_spabiz_customer_key order by ad.start_date_time asc,ad.fact_spabiz_appointment_item_key) customer_rank
  from d_spabiz_ap_data ad
  join dim_spabiz_service s
    on ad.dim_spabiz_service_key = s.dim_spabiz_service_key
  join #future_ap
    on ad.dim_spabiz_customer_key = #future_ap.dim_spabiz_customer_key
   and ad.start_date_time = #future_ap.next_start_date_time
   and s.category = #future_ap.category
 where ad.deleted_flag = 'N'
   and ad.data_type_id = 5
   and ad.dim_spabiz_customer_key <> '-998'
   and ad.start_date_time >= convert(date,getdate())

delete from #next_ap where category_rank <> 1 and customer_rank <> 1

--create table #all_customers with (distribution = hash(dim_spabiz_customer_key)) as
--select dim_spabiz_customer_key from #last_purchase where category_rank = 1 and data_type = 'service'
--union
--select dim_spabiz_customer_key from #last_purchase where customer_rank = 1
--union
--select dim_spabiz_customer_key from #ticket_item_category_totals
--union
--select dim_spabiz_customer_key from #next_ap where category_rank = 1
--union
--select dim_spabiz_customer_key from #next_ap where customer_rank = 1


truncate table fact_spabiz_non_member_summary


insert into fact_spabiz_non_member_summary (
dim_spabiz_customer_key,
latest_service_purchase_date_time,
latest_service_purchase_fact_spabiz_ticket_item_key,
latest_body_ticket_item_date_time,
latest_body_fact_spabiz_ticket_item_key,
latest_hair_ticket_item_date_time,
latest_hair_fact_spabiz_ticket_item_key,
latest_skin_ticket_item_date_time,
latest_skin_fact_spabiz_ticket_item_key,
latest_nail_ticket_item_date_time,
latest_nail_fact_spabiz_ticket_item_key,
latest_medi_ticket_item_date_time,
latest_medi_fact_spabiz_ticket_item_key,
yearly_product_amount,
yearly_product_quantity,
yearly_service_amount,
yearly_service_quantity,
yearly_body_service_amount,
yearly_body_service_quantity,
yearly_hair_service_amount,
yearly_hair_service_quantity,
yearly_skin_service_amount,
yearly_skin_service_quantity,
yearly_nail_service_amount,
yearly_nail_service_quantity,
yearly_medi_service_amount,
yearly_medi_service_quantity,
next_appointment_date_time,
next_appointment_fact_spabiz_appointment_item_key,
next_body_appointment_date_time,
next_body_fact_spabiz_appointment_item_key,
next_hair_appointment_date_time,
next_hair_fact_spabiz_appointment_item_key,
next_skin_appointment_date_time,
next_skin_fact_spabiz_appointment_item_key,
next_nail_appointment_date_time,
next_nail_fact_spabiz_appointment_item_key,
next_medi_appointment_date_time,
next_medi_fact_spabiz_appointment_item_key,
dv_load_date_time,
dv_load_end_date_time,
dv_batch_id,
dv_inserted_date_time,
dv_insert_user
)
select ac.dim_spabiz_customer_key,

       min(clp.ticket_item_date_time) latest_service_purchase_date_time,
       isnull(min(clp.fact_spabiz_ticket_item_key),'-998') latest_service_purchase_fact_spabiz_ticket_item_key,
       max(case when lp.category = 'body' then lp.ticket_item_date_time else null end) latest_body_ticket_item_date_time,
       isnull(max(case when lp.category = 'body' then lp.fact_spabiz_ticket_item_key else null end),'-998') latest_body_fact_spabiz_ticket_item_key,
       max(case when lp.category = 'hair' then lp.ticket_item_date_time else null end) latest_hair_ticket_item_date_time,
       isnull(max(case when lp.category = 'hair' then lp.fact_spabiz_ticket_item_key else null end),'-998') latest_hair_fact_spabiz_ticket_item_key,
       max(case when lp.category = 'skin' then lp.ticket_item_date_time else null end) latest_skin_ticket_item_date_time,
       isnull(max(case when lp.category = 'skin' then lp.fact_spabiz_ticket_item_key else null end),'-998') latest_skin_fact_spabiz_ticket_item_key,
       max(case when lp.category = 'nail' then lp.ticket_item_date_time else null end) latest_nail_ticket_item_date_time,
       isnull(max(case when lp.category = 'nail' then lp.fact_spabiz_ticket_item_key else null end),'-998') latest_nail_fact_spabiz_ticket_item_key,
       max(case when lp.category = 'medi' then lp.ticket_item_date_time else null end) latest_medi_ticket_item_date_time,
       isnull(max(case when lp.category = 'medi' then lp.fact_spabiz_ticket_item_key else null end),'-998') latest_medi_fact_spabiz_ticket_item_key,
       isnull(max(t.yearly_product_amount),0) yearly_product_amount,
       isnull(max(t.yearly_product_quantity),0) yearly_product_quantity,
       isnull(max(t.yearly_service_amount),0) yearly_service_amount,
       isnull(max(t.yearly_service_quantity),0) yearly_service_quantity,
       isnull(max(t.yearly_body_service_amount),0) yearly_body_service_amount,
       isnull(max(t.yearly_body_service_quantity),0) yearly_body_service_quantity,
       isnull(max(t.yearly_hair_service_amount),0) yearly_hair_service_amount,
       isnull(max(t.yearly_hair_service_quantity),0) yearly_hair_service_quantity,
       isnull(max(t.yearly_skin_service_amount),0) yearly_skin_service_amount,
       isnull(max(t.yearly_skin_service_quantity),0) yearly_skin_service_quantity,
       isnull(max(t.yearly_nail_service_amount),0) yearly_nail_service_amount,
       isnull(max(t.yearly_nail_service_quantity),0) yearly_nail_service_quantity,
       isnull(max(t.yearly_medi_service_amount),0) yearly_medi_service_amount,
       isnull(max(t.yearly_medi_service_quantity),0) yearly_medi_service_quantity,
       max(cnap.start_date_time) next_appointment_date_time,
       isnull(max(cnap.fact_spabiz_appointment_item_key),'-998') next_appointment_fact_spabiz_appointment_item_key,
       max(case when nap.category = 'body' then nap.start_date_time else null end) next_body_appointment_date_time,
       isnull(max(case when nap.category = 'body' then nap.fact_spabiz_appointment_item_key else null end),'-998') next_body_fact_spabiz_appointment_item_key,
       max(case when nap.category = 'hair' then nap.start_date_time else null end) next_hair_appointment_date_time,
       isnull(max(case when nap.category = 'hair' then nap.fact_spabiz_appointment_item_key else null end),'-998') next_hair_fact_spabiz_appointment_item_key,
       max(case when nap.category = 'skin' then nap.start_date_time else null end) next_skin_appointment_date_time,
       isnull(max(case when nap.category = 'skin' then nap.fact_spabiz_appointment_item_key else null end),'-998') next_skin_fact_spabiz_appointment_item_key,
       max(case when nap.category = 'nail' then nap.start_date_time else null end) next_nail_appointment_date_time,
       isnull(max(case when nap.category = 'nail' then nap.fact_spabiz_appointment_item_key else null end),'-998') next_nail_fact_spabiz_appointment_item_key,
       max(case when nap.category = 'medi' then nap.start_date_time else null end) next_medi_appointment_date_time,
       isnull(max(case when nap.category = 'medi' then nap.fact_spabiz_appointment_item_key else null end),'-998') next_medi_fact_spabiz_appointment_item_key,
       getdate(),
       'Dec 31, 9999',
       @dv_batch_id,
       getdate(),
       suser_sname()
--from #all_customers ac
 from d_spabiz_customer ac
 left join #last_purchase lp
   on lp.dim_spabiz_customer_key = ac.dim_spabiz_customer_key
  and lp.category_rank = 1
  and lp.data_type = 'service'
 left join #last_purchase clp
   on clp.dim_spabiz_customer_key = ac.dim_spabiz_customer_key
  and clp.customer_rank = 1
 left join #ticket_item_year_totals t
   on t.dim_spabiz_customer_key = ac.dim_spabiz_customer_key
 left join #next_ap nap
   on ac.dim_spabiz_customer_key = nap.dim_spabiz_customer_key
  and nap.category_rank = 1
 left join #next_ap cnap
   on ac.dim_spabiz_customer_key = cnap.dim_spabiz_customer_key
  and cnap.customer_rank = 1
where ac.member_id in ('','0')
group by ac.dim_spabiz_customer_key


drop table #ticket_item_category_totals
drop table #ticket_item_year_totals
drop table #last_purchase
drop table #future_ap
drop table #next_ap




end
