CREATE PROC [dbo].[proc_dim_mms_club_pos_pricing_discount] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

/*
Notes:
   All pricing discount ids are taken from link tables when its doing a join with secondary source tables. Sceondary source tables - these are
   tables other than driving table.
   Only in case of actual driving table fetching it from pit of driving table.
   Conditions on dv_batch_id/dv_load_end_date_time are imposed on pit or r tables of secondary tables with the understanding
   it will fetch only current records and do a join with driving table(link table)
*/

/* Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.*/
if object_id('tempdb..#dv_batch_id') is not null drop table #dv_batch_id
create table dbo.#dv_batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
    @dv_batch_id as current_dv_batch_id
    from dbo.dim_mms_club_pos_pricing_discount


if object_id('tempdb..#pricing_discount_id') is not null drop table #pricing_discount_id
create table dbo.#pricing_discount_id with(distribution=hash(pricing_discount_id), location=user_db, heap) as
select pricing_discount_id,
       rank() over (order by pricing_discount_id) r
from(  /*-p_mms_sales_promotion*/
    select  l_mms_pricing_discount.pricing_discount_id pricing_discount_id
    from l_mms_pricing_discount
    join p_mms_sales_promotion
    on l_mms_pricing_discount.sales_promotion_id = p_mms_sales_promotion.sales_promotion_id
    and p_mms_sales_promotion.dv_load_end_date_time = 'Dec 31, 9999'
    join #dv_batch_id
    on p_mms_sales_promotion.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or p_mms_sales_promotion.dv_batch_id = #dv_batch_id.current_dv_batch_id
    where p_mms_sales_promotion.dv_load_end_date_time = 'Dec 31, 9999'
    union
	/*-r_mms_val_discount_application_type*/
    select  l_mms_pricing_discount.pricing_discount_id pricing_discount_id
	from l_mms_pricing_discount 
	join r_mms_val_discount_application_type
	on l_mms_pricing_discount.val_discount_application_type_id = r_mms_val_discount_application_type.val_discount_application_type_id
	and r_mms_val_discount_application_type.dv_load_end_date_time = 'Dec 31, 9999'
	join #dv_batch_id
    on r_mms_val_discount_application_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_discount_application_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
    where r_mms_val_discount_application_type.dv_load_end_date_time = 'Dec 31, 9999'
	union
	/*-r_mms_val_discount_combine_rule---*/
    select  l_mms_pricing_discount.pricing_discount_id pricing_discount_id
	from l_mms_pricing_discount 
	join r_mms_val_discount_combine_rule
	on l_mms_pricing_discount.val_discount_combine_rule_id = r_mms_val_discount_combine_rule.val_discount_combine_rule_id
	and r_mms_val_discount_combine_rule.dv_load_end_date_time = 'Dec 31, 9999'
	join #dv_batch_id
    on r_mms_val_discount_combine_rule.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_discount_combine_rule.dv_batch_id = #dv_batch_id.current_dv_batch_id
    where r_mms_val_discount_combine_rule.dv_load_end_date_time = 'Dec 31, 9999'
	union
	/*-r_mms_val_discount_type*/
	select  l_mms_pricing_discount.pricing_discount_id pricing_discount_id
	from l_mms_pricing_discount 
	join r_mms_val_discount_type
	on l_mms_pricing_discount.val_discount_type_id = r_mms_val_discount_type.val_discount_type_id
	and r_mms_val_discount_type.dv_load_end_date_time = 'Dec 31, 9999'
	join #dv_batch_id
    on r_mms_val_discount_type.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or r_mms_val_discount_type.dv_batch_id = #dv_batch_id.current_dv_batch_id
    where r_mms_val_discount_type.dv_load_end_date_time = 'Dec 31, 9999'
    union
	/*Driving table - p_mms_pricing_discount*/
    select  p_mms_pricing_discount.pricing_discount_id pricing_discount_id
    from p_mms_pricing_discount 
    join #dv_batch_id
    on p_mms_pricing_discount.dv_batch_id > #dv_batch_id.max_dv_batch_id
    or p_mms_pricing_discount.dv_batch_id = #dv_batch_id.current_dv_batch_id
    where p_mms_pricing_discount.dv_load_end_date_time = 'Dec 31, 9999'
    )x


	
/*-p_mms_pricing_discount a*/
if object_id('tempdb..#p_mms_pricing_discount') is not null drop table #p_mms_pricing_discount
create table dbo.#p_mms_pricing_discount with(distribution=hash(dim_mms_pricing_discount_key), location=user_db, heap) as
select  
    p_mms_pricing_discount.bk_hash dim_mms_pricing_discount_key,
    p_mms_pricing_discount.pricing_discount_id pricing_discount_id,
	l_mms_pricing_discount.sales_promotion_id sales_promotion_id,
	l_mms_pricing_discount.val_discount_type_id val_discount_type_id,
	l_mms_pricing_discount.val_discount_application_type_id val_discount_application_type_id,
	l_mms_pricing_discount.val_discount_combine_rule_id val_discount_combine_rule_id,
	p_mms_pricing_discount.p_mms_pricing_discount_id p_mms_pricing_discount_id,
    s_mms_pricing_discount.discount_value  discount_value,
	s_mms_pricing_discount.sales_commission_percent sales_commission_percent,
	s_mms_pricing_discount.service_commission_percent service_commission_percent,
	case when p_mms_pricing_discount.bk_hash in ('-997','-998','-999') then 'N'
         when s_mms_pricing_discount.available_for_all_products_flag = 1 then 'Y'
         else 'N'
    end
	available_for_all_products_flag,
	s_mms_pricing_discount.all_products_discount_use_limit all_products_discount_use_limit,
    p_mms_pricing_discount.dv_batch_id dv_batch_id,
    p_mms_pricing_discount.dv_load_date_time dv_load_date_time,
    p_mms_pricing_discount.dv_load_end_date_time dv_load_end_date_time,
    #pricing_discount_id.r
from p_mms_pricing_discount 
join s_mms_pricing_discount
on p_mms_pricing_discount.s_mms_pricing_discount_id = s_mms_pricing_discount.s_mms_pricing_discount_id
join l_mms_pricing_discount
on p_mms_pricing_discount.l_mms_pricing_discount_id = l_mms_pricing_discount.l_mms_pricing_discount_id
join #pricing_discount_id
on p_mms_pricing_discount.pricing_discount_id = #pricing_discount_id.pricing_discount_id
where p_mms_pricing_discount.dv_load_end_date_time = 'Dec 31, 9999'


/*p_mms_sales_promotion b*/
if object_id('tempdb..#p_mms_sales_promotion') is not null drop table #p_mms_sales_promotion
create table dbo.#p_mms_sales_promotion with(distribution=hash(dim_mms_pricing_discount_key), location=user_db, heap) as
select 
    case when l_mms_pricing_discount.pricing_discount_id is null then '-998'
    /*util_bk_hash[l_lt_bucks_cart_details.cdetail_cart,h_lt_bucks_shopping_cart.cart_id]*/
    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_pricing_discount.pricing_discount_id as varchar(500)),'z#@$k%&P'))),2)
    end dim_mms_pricing_discount_key,
    p_mms_sales_promotion.sales_promotion_id sales_promotion_id,
    s_mms_sales_promotion.display_text sales_promotion_pos_display_text,
	s_mms_sales_promotion.receipt_text sales_promotion_receipt_text,
    p_mms_sales_promotion.p_mms_sales_promotion_id p_mms_sales_promotion_id,
    p_mms_sales_promotion.dv_batch_id dv_batch_id,
    p_mms_sales_promotion.dv_load_date_time dv_load_date_time,
    p_mms_sales_promotion.dv_load_end_date_time dv_load_end_date_time
from dbo.p_mms_sales_promotion
join dbo.l_mms_sales_promotion
on p_mms_sales_promotion.l_mms_sales_promotion_id = l_mms_sales_promotion.l_mms_sales_promotion_id
join dbo.s_mms_sales_promotion
on p_mms_sales_promotion.s_mms_sales_promotion_id = s_mms_sales_promotion.s_mms_sales_promotion_id
join dbo.l_mms_pricing_discount
on p_mms_sales_promotion.sales_promotion_id =l_mms_pricing_discount.sales_promotion_id 
join dbo.p_mms_pricing_discount
  on l_mms_pricing_discount.l_mms_pricing_discount_id = p_mms_pricing_discount.l_mms_pricing_discount_id
and p_mms_pricing_discount.dv_load_end_date_time = 'Dec 31, 9999'
join #pricing_discount_id
on l_mms_pricing_discount.pricing_discount_id = #pricing_discount_id.pricing_discount_id
where p_mms_sales_promotion.dv_load_end_date_time = 'Dec 31, 9999'



 /*--r_mms_val_discount_application_type c*/
if object_id('tempdb..#r_mms_val_discount_application_type') is not null drop table #r_mms_val_discount_application_type
create table dbo.#r_mms_val_discount_application_type with(distribution=hash(dim_mms_pricing_discount_key), location=user_db, heap) as
select 
    case when l_mms_pricing_discount.pricing_discount_id is null then '-998'
       /*util_bk_hash[l_lt_bucks_cart_details.cdetail_cart,h_lt_bucks_shopping_cart.cart_id]*/
    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_pricing_discount.pricing_discount_id as varchar(500)),'z#@$k%&P'))),2)
    end dim_mms_pricing_discount_key,
    r_mms_val_discount_application_type.r_mms_val_discount_application_type_id r_mms_val_discount_application_type_id,
    r_mms_val_discount_application_type.val_discount_application_type_id val_discount_application_type_id,
    r_mms_val_discount_application_type.description discount_application_type,
    r_mms_val_discount_application_type.dv_batch_id dv_batch_id,
    r_mms_val_discount_application_type.dv_load_date_time dv_load_date_time,
    r_mms_val_discount_application_type.dv_load_end_date_time dv_load_end_date_time
from r_mms_val_discount_application_type 
join l_mms_pricing_discount
 on r_mms_val_discount_application_type.val_discount_application_type_id = l_mms_pricing_discount.val_discount_application_type_id
 join dbo.p_mms_pricing_discount
  on l_mms_pricing_discount.l_mms_pricing_discount_id = p_mms_pricing_discount.l_mms_pricing_discount_id
and p_mms_pricing_discount.dv_load_end_date_time = 'Dec 31, 9999'
join #pricing_discount_id
 on l_mms_pricing_discount.pricing_discount_id = #pricing_discount_id.pricing_discount_id
where r_mms_val_discount_application_type.dv_load_end_date_time = 'Dec 31, 9999'
 




 /*-r_mms_val_discount_combine_rule d*/
 if object_id('tempdb..#r_mms_val_discount_combine_rule') is not null drop table #r_mms_val_discount_combine_rule
create table dbo.#r_mms_val_discount_combine_rule with(distribution=hash(dim_mms_pricing_discount_key), location=user_db, heap) as
select 
    case when l_mms_pricing_discount.pricing_discount_id is null then '-998'
    /*util_bk_hash[l_lt_bucks_cart_details.cdetail_cart,h_lt_bucks_shopping_cart.cart_id]*/
    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_pricing_discount.pricing_discount_id as varchar(500)),'z#@$k%&P'))),2)
    end dim_mms_pricing_discount_key,
    r_mms_val_discount_combine_rule.r_mms_val_discount_combine_rule_id r_mms_val_discount_combine_rule_id,
    r_mms_val_discount_combine_rule.val_discount_combine_rule_id val_discount_combine_rule_id,
    r_mms_val_discount_combine_rule.description discount_combine_rule,
    r_mms_val_discount_combine_rule.dv_batch_id dv_batch_id,
    r_mms_val_discount_combine_rule.dv_load_date_time dv_load_date_time,
    r_mms_val_discount_combine_rule.dv_load_end_date_time dv_load_end_date_time
from r_mms_val_discount_combine_rule 
join l_mms_pricing_discount
on r_mms_val_discount_combine_rule.val_discount_combine_rule_id = l_mms_pricing_discount.val_discount_combine_rule_id
join dbo.p_mms_pricing_discount
  on l_mms_pricing_discount.l_mms_pricing_discount_id = p_mms_pricing_discount.l_mms_pricing_discount_id
and p_mms_pricing_discount.dv_load_end_date_time = 'Dec 31, 9999'
join #pricing_discount_id
on l_mms_pricing_discount.pricing_discount_id = #pricing_discount_id.pricing_discount_id
where r_mms_val_discount_combine_rule.dv_load_end_date_time = 'Dec 31, 9999'


/*r_mms_val_discount_type e*/
if object_id('tempdb..#r_mms_val_discount_type') is not null drop table #r_mms_val_discount_type
create table dbo.#r_mms_val_discount_type with(distribution=hash(dim_mms_pricing_discount_key), location=user_db, heap) as
select 
    case when l_mms_pricing_discount.pricing_discount_id is null then '-998'
    /*util_bk_hash[l_lt_bucks_cart_details.cdetail_cart,h_lt_bucks_shopping_cart.cart_id]*/
    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_pricing_discount.pricing_discount_id as varchar(500)),'z#@$k%&P'))),2)
    end dim_mms_pricing_discount_key,
	r_mms_val_discount_type.r_mms_val_discount_type_id r_mms_val_discount_type_id,
    r_mms_val_discount_type.description discount_type,
	r_mms_val_discount_type.val_discount_type_id val_discount_type_id,
    r_mms_val_discount_type.dv_batch_id dv_batch_id,
    r_mms_val_discount_type.dv_load_date_time dv_load_date_time,
    r_mms_val_discount_type.dv_load_end_date_time dv_load_end_date_time
from r_mms_val_discount_type 
join l_mms_pricing_discount
on r_mms_val_discount_type.val_discount_type_id = l_mms_pricing_discount.val_discount_type_id
join dbo.p_mms_pricing_discount
  on l_mms_pricing_discount.l_mms_pricing_discount_id = p_mms_pricing_discount.l_mms_pricing_discount_id
and p_mms_pricing_discount.dv_load_end_date_time = 'Dec 31, 9999'
join #pricing_discount_id
on l_mms_pricing_discount.pricing_discount_id = #pricing_discount_id.pricing_discount_id
where r_mms_val_discount_type.dv_load_end_date_time = 'Dec 31, 9999'


/*delete and re-insert*/
declare @start int, @end int
set @start = 1
set @end = (select max(r) from #pricing_discount_id)

while @start <= @end
begin
/* do as a single transaction*/
/*   delete records from the fact table that exist*/
/*   insert records from records from current and missing batches*/
    begin tran
    delete dbo.dim_mms_club_pos_pricing_discount
    where pricing_discount_id in (select pricing_discount_id from #pricing_discount_id where r >= @start and r < @start+60000000)

    insert dbo.dim_mms_club_pos_pricing_discount(
    dim_mms_club_pos_pricing_discount_key,
    pricing_discount_id,
    sales_promotion_pos_display_text,
    sales_promotion_receipt_text,
    discount_type,
    discount_application_type,
    discount_combine_rule,
    discount_value,
    sales_commission_percent,
    service_commission_percent,
    available_for_all_products_flag,
    p_mms_sales_promotion_id,
    val_discount_type_id,
	val_discount_application_type_id,
	val_discount_combine_rule_id,
	p_mms_pricing_discount_id,
    dv_load_date_time,
    dv_load_end_date_time,
    dv_batch_id,
    dv_inserted_date_time,
    dv_insert_user)
	select 
	#p_mms_pricing_discount.dim_mms_pricing_discount_key,
    #p_mms_pricing_discount.pricing_discount_id,
    isnull(#p_mms_sales_promotion.sales_promotion_pos_display_text,''),
    isnull(#p_mms_sales_promotion.sales_promotion_receipt_text,''),
    #r_mms_val_discount_type.discount_type,
    #r_mms_val_discount_application_type.discount_application_type,
    #r_mms_val_discount_combine_rule.discount_combine_rule,
    #p_mms_pricing_discount.discount_value,
    #p_mms_pricing_discount.sales_commission_percent,
	#p_mms_pricing_discount.service_commission_percent,
	#p_mms_pricing_discount.available_for_all_products_flag,
    #p_mms_sales_promotion.p_mms_sales_promotion_id,
	#r_mms_val_discount_type.val_discount_type_id,
	#r_mms_val_discount_application_type.val_discount_application_type_id,
	#r_mms_val_discount_combine_rule.val_discount_combine_rule_id,
	#p_mms_pricing_discount.p_mms_pricing_discount_id ,
    case when #p_mms_pricing_discount.dv_load_date_time > #p_mms_sales_promotion.dv_load_date_time 
	and  #p_mms_pricing_discount.dv_load_date_time > #r_mms_val_discount_application_type.dv_load_date_time
	and  #p_mms_pricing_discount.dv_load_date_time > #r_mms_val_discount_combine_rule.dv_load_date_time
	and  #p_mms_pricing_discount.dv_load_date_time > #r_mms_val_discount_type.dv_load_date_time
	then #p_mms_pricing_discount.dv_load_date_time
	when #p_mms_sales_promotion.dv_load_date_time  > #r_mms_val_discount_application_type.dv_load_date_time
	and  #p_mms_sales_promotion.dv_load_date_time  > #r_mms_val_discount_combine_rule.dv_load_date_time
	and  #p_mms_sales_promotion.dv_load_date_time  > #r_mms_val_discount_type.dv_load_date_time
	then #p_mms_sales_promotion.dv_load_date_time
	when #r_mms_val_discount_application_type.dv_load_date_time > #r_mms_val_discount_combine_rule.dv_load_date_time
	and  #r_mms_val_discount_application_type.dv_load_date_time > #r_mms_val_discount_type.dv_load_date_time
	then #r_mms_val_discount_application_type.dv_load_date_time
	when #r_mms_val_discount_combine_rule.dv_load_date_time > #r_mms_val_discount_type.dv_load_date_time
	then #r_mms_val_discount_combine_rule.dv_load_date_time
	else #r_mms_val_discount_type.dv_load_date_time end dv_load_date_time,
	case when #p_mms_pricing_discount.dv_load_end_date_time > #p_mms_sales_promotion.dv_load_end_date_time 
	and  #p_mms_pricing_discount.dv_load_end_date_time > #r_mms_val_discount_application_type.dv_load_end_date_time
	and  #p_mms_pricing_discount.dv_load_end_date_time > #r_mms_val_discount_combine_rule.dv_load_end_date_time
	and  #p_mms_pricing_discount.dv_load_end_date_time > #r_mms_val_discount_type.dv_load_end_date_time
	then #p_mms_pricing_discount.dv_load_end_date_time
	when #p_mms_sales_promotion.dv_load_end_date_time  > #r_mms_val_discount_application_type.dv_load_end_date_time
	and  #p_mms_sales_promotion.dv_load_end_date_time  > #r_mms_val_discount_combine_rule.dv_load_end_date_time
	and  #p_mms_sales_promotion.dv_load_end_date_time  > #r_mms_val_discount_type.dv_load_end_date_time
	then #p_mms_sales_promotion.dv_load_end_date_time
    when #r_mms_val_discount_application_type.dv_load_end_date_time > #r_mms_val_discount_combine_rule.dv_load_end_date_time
	and  #r_mms_val_discount_application_type.dv_load_end_date_time > #r_mms_val_discount_type.dv_load_end_date_time
	then #r_mms_val_discount_application_type.dv_load_end_date_time
    when #r_mms_val_discount_combine_rule.dv_load_end_date_time > #r_mms_val_discount_type.dv_load_end_date_time
	then #r_mms_val_discount_combine_rule.dv_load_end_date_time
	else #r_mms_val_discount_type.dv_load_end_date_time end dv_load_end_date_time,
	case when #p_mms_pricing_discount.dv_batch_id > #p_mms_sales_promotion.dv_batch_id 
	and  #p_mms_pricing_discount.dv_batch_id > #r_mms_val_discount_application_type.dv_batch_id
	and  #p_mms_pricing_discount.dv_batch_id > #r_mms_val_discount_combine_rule.dv_batch_id
	and  #p_mms_pricing_discount.dv_batch_id > #r_mms_val_discount_type.dv_batch_id
	then #p_mms_pricing_discount.dv_batch_id
	when #p_mms_sales_promotion.dv_batch_id  > #r_mms_val_discount_application_type.dv_batch_id
	and  #p_mms_sales_promotion.dv_batch_id  > #r_mms_val_discount_combine_rule.dv_batch_id
	and  #p_mms_sales_promotion.dv_batch_id  > #r_mms_val_discount_type.dv_batch_id
	then #p_mms_sales_promotion.dv_batch_id
    when #r_mms_val_discount_application_type.dv_batch_id > #r_mms_val_discount_combine_rule.dv_batch_id
	and  #r_mms_val_discount_application_type.dv_batch_id > #r_mms_val_discount_type.dv_batch_id
    then #r_mms_val_discount_application_type.dv_batch_id
	when #r_mms_val_discount_combine_rule.dv_batch_id > #r_mms_val_discount_type.dv_batch_id
	then #r_mms_val_discount_combine_rule.dv_batch_id
	else #r_mms_val_discount_type.dv_batch_id end dv_batch_id,             
    getdate(),
    suser_sname()
from #p_mms_pricing_discount
join #r_mms_val_discount_application_type
on #p_mms_pricing_discount.dim_mms_pricing_discount_key = #r_mms_val_discount_application_type.dim_mms_pricing_discount_key
join #r_mms_val_discount_combine_rule
on #p_mms_pricing_discount.dim_mms_pricing_discount_key=#r_mms_val_discount_combine_rule.dim_mms_pricing_discount_key
join #r_mms_val_discount_type
on #p_mms_pricing_discount.dim_mms_pricing_discount_key=#r_mms_val_discount_type.dim_mms_pricing_discount_key
join #p_mms_sales_promotion
on #p_mms_pricing_discount.dim_mms_pricing_discount_key=#p_mms_sales_promotion.dim_mms_pricing_discount_key
where #p_mms_pricing_discount.r >= @start
and #p_mms_pricing_discount.r < @start+60000000
commit tran

set @start = @start+60000000
end
end

