CREATE PROC [dbo].[proc_etl_lt_bucks_shopping_cart] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_lt_bucks_ShoppingCart

set @insert_date_time = getdate()
insert into dbo.stage_hash_lt_bucks_ShoppingCart (
       bk_hash,
       cart_id,
       cart_session,
       cart_product,
       cart_qty,
       cart_status,
       cart_color,
       cart_size,
       cart_ext_data1,
       cart_logo,
       cart_amount,
       cart_point_amount,
       cart_sku,
       cart_sku2,
       cart_name,
       cart_locked,
       cart_timestamp,
       cart_coupon_amount,
       LastModifiedTimestamp,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cart_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       cart_id,
       cart_session,
       cart_product,
       cart_qty,
       cart_status,
       cart_color,
       cart_size,
       cart_ext_data1,
       cart_logo,
       cart_amount,
       cart_point_amount,
       cart_sku,
       cart_sku2,
       cart_name,
       cart_locked,
       cart_timestamp,
       cart_coupon_amount,
       LastModifiedTimestamp,
       isnull(cast(stage_lt_bucks_ShoppingCart.cart_timestamp as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_lt_bucks_ShoppingCart
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_lt_bucks_shopping_cart @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_lt_bucks_shopping_cart (
       bk_hash,
       cart_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_lt_bucks_ShoppingCart.bk_hash,
       stage_hash_lt_bucks_ShoppingCart.cart_id cart_id,
       isnull(cast(stage_hash_lt_bucks_ShoppingCart.cart_timestamp as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       5,
       @insert_date_time,
       @user
  from stage_hash_lt_bucks_ShoppingCart
  left join h_lt_bucks_shopping_cart
    on stage_hash_lt_bucks_ShoppingCart.bk_hash = h_lt_bucks_shopping_cart.bk_hash
 where h_lt_bucks_shopping_cart_id is null
   and stage_hash_lt_bucks_ShoppingCart.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_lt_bucks_shopping_cart
if object_id('tempdb..#l_lt_bucks_shopping_cart_inserts') is not null drop table #l_lt_bucks_shopping_cart_inserts
create table #l_lt_bucks_shopping_cart_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_lt_bucks_ShoppingCart.bk_hash,
       stage_hash_lt_bucks_ShoppingCart.cart_id cart_id,
       stage_hash_lt_bucks_ShoppingCart.cart_session cart_session,
       stage_hash_lt_bucks_ShoppingCart.cart_product cart_product,
       stage_hash_lt_bucks_ShoppingCart.cart_logo cart_logo,
       stage_hash_lt_bucks_ShoppingCart.cart_timestamp dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ShoppingCart.cart_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ShoppingCart.cart_session as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ShoppingCart.cart_product as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ShoppingCart.cart_logo as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_lt_bucks_ShoppingCart
 where stage_hash_lt_bucks_ShoppingCart.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_lt_bucks_shopping_cart records
set @insert_date_time = getdate()
insert into l_lt_bucks_shopping_cart (
       bk_hash,
       cart_id,
       cart_session,
       cart_product,
       cart_logo,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_lt_bucks_shopping_cart_inserts.bk_hash,
       #l_lt_bucks_shopping_cart_inserts.cart_id,
       #l_lt_bucks_shopping_cart_inserts.cart_session,
       #l_lt_bucks_shopping_cart_inserts.cart_product,
       #l_lt_bucks_shopping_cart_inserts.cart_logo,
       case when l_lt_bucks_shopping_cart.l_lt_bucks_shopping_cart_id is null then isnull(#l_lt_bucks_shopping_cart_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       5,
       #l_lt_bucks_shopping_cart_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_lt_bucks_shopping_cart_inserts
  left join p_lt_bucks_shopping_cart
    on #l_lt_bucks_shopping_cart_inserts.bk_hash = p_lt_bucks_shopping_cart.bk_hash
   and p_lt_bucks_shopping_cart.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_lt_bucks_shopping_cart
    on p_lt_bucks_shopping_cart.bk_hash = l_lt_bucks_shopping_cart.bk_hash
   and p_lt_bucks_shopping_cart.l_lt_bucks_shopping_cart_id = l_lt_bucks_shopping_cart.l_lt_bucks_shopping_cart_id
 where l_lt_bucks_shopping_cart.l_lt_bucks_shopping_cart_id is null
    or (l_lt_bucks_shopping_cart.l_lt_bucks_shopping_cart_id is not null
        and l_lt_bucks_shopping_cart.dv_hash <> #l_lt_bucks_shopping_cart_inserts.source_hash)

--calculate hash and lookup to current s_lt_bucks_shopping_cart
if object_id('tempdb..#s_lt_bucks_shopping_cart_inserts') is not null drop table #s_lt_bucks_shopping_cart_inserts
create table #s_lt_bucks_shopping_cart_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_lt_bucks_ShoppingCart.bk_hash,
       stage_hash_lt_bucks_ShoppingCart.cart_id cart_id,
       stage_hash_lt_bucks_ShoppingCart.cart_qty cart_qty,
       stage_hash_lt_bucks_ShoppingCart.cart_status cart_status,
       stage_hash_lt_bucks_ShoppingCart.cart_color cart_color,
       stage_hash_lt_bucks_ShoppingCart.cart_size cart_size,
       stage_hash_lt_bucks_ShoppingCart.cart_ext_data1 cart_ext_data1,
       stage_hash_lt_bucks_ShoppingCart.cart_amount cart_amount,
       stage_hash_lt_bucks_ShoppingCart.cart_point_amount cart_point_amount,
       stage_hash_lt_bucks_ShoppingCart.cart_sku cart_sku,
       stage_hash_lt_bucks_ShoppingCart.cart_sku2 cart_sku2,
       stage_hash_lt_bucks_ShoppingCart.cart_name cart_name,
       stage_hash_lt_bucks_ShoppingCart.cart_locked cart_locked,
       stage_hash_lt_bucks_ShoppingCart.cart_timestamp cart_timestamp,
       stage_hash_lt_bucks_ShoppingCart.cart_coupon_amount cart_coupon_amount,
       stage_hash_lt_bucks_ShoppingCart.LastModifiedTimestamp last_modified_timestamp,
       stage_hash_lt_bucks_ShoppingCart.cart_timestamp dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ShoppingCart.cart_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ShoppingCart.cart_qty as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ShoppingCart.cart_status as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ShoppingCart.cart_color as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ShoppingCart.cart_size as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ShoppingCart.cart_ext_data1 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ShoppingCart.cart_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ShoppingCart.cart_point_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_ShoppingCart.cart_sku,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_ShoppingCart.cart_sku2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_ShoppingCart.cart_name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ShoppingCart.cart_locked as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_lt_bucks_ShoppingCart.cart_timestamp,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_ShoppingCart.cart_coupon_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_lt_bucks_ShoppingCart.LastModifiedTimestamp,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_lt_bucks_ShoppingCart
 where stage_hash_lt_bucks_ShoppingCart.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_lt_bucks_shopping_cart records
set @insert_date_time = getdate()
insert into s_lt_bucks_shopping_cart (
       bk_hash,
       cart_id,
       cart_qty,
       cart_status,
       cart_color,
       cart_size,
       cart_ext_data1,
       cart_amount,
       cart_point_amount,
       cart_sku,
       cart_sku2,
       cart_name,
       cart_locked,
       cart_timestamp,
       cart_coupon_amount,
       last_modified_timestamp,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_lt_bucks_shopping_cart_inserts.bk_hash,
       #s_lt_bucks_shopping_cart_inserts.cart_id,
       #s_lt_bucks_shopping_cart_inserts.cart_qty,
       #s_lt_bucks_shopping_cart_inserts.cart_status,
       #s_lt_bucks_shopping_cart_inserts.cart_color,
       #s_lt_bucks_shopping_cart_inserts.cart_size,
       #s_lt_bucks_shopping_cart_inserts.cart_ext_data1,
       #s_lt_bucks_shopping_cart_inserts.cart_amount,
       #s_lt_bucks_shopping_cart_inserts.cart_point_amount,
       #s_lt_bucks_shopping_cart_inserts.cart_sku,
       #s_lt_bucks_shopping_cart_inserts.cart_sku2,
       #s_lt_bucks_shopping_cart_inserts.cart_name,
       #s_lt_bucks_shopping_cart_inserts.cart_locked,
       #s_lt_bucks_shopping_cart_inserts.cart_timestamp,
       #s_lt_bucks_shopping_cart_inserts.cart_coupon_amount,
       #s_lt_bucks_shopping_cart_inserts.last_modified_timestamp,
       case when s_lt_bucks_shopping_cart.s_lt_bucks_shopping_cart_id is null then isnull(#s_lt_bucks_shopping_cart_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       5,
       #s_lt_bucks_shopping_cart_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_lt_bucks_shopping_cart_inserts
  left join p_lt_bucks_shopping_cart
    on #s_lt_bucks_shopping_cart_inserts.bk_hash = p_lt_bucks_shopping_cart.bk_hash
   and p_lt_bucks_shopping_cart.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_lt_bucks_shopping_cart
    on p_lt_bucks_shopping_cart.bk_hash = s_lt_bucks_shopping_cart.bk_hash
   and p_lt_bucks_shopping_cart.s_lt_bucks_shopping_cart_id = s_lt_bucks_shopping_cart.s_lt_bucks_shopping_cart_id
 where s_lt_bucks_shopping_cart.s_lt_bucks_shopping_cart_id is null
    or (s_lt_bucks_shopping_cart.s_lt_bucks_shopping_cart_id is not null
        and s_lt_bucks_shopping_cart.dv_hash <> #s_lt_bucks_shopping_cart_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_lt_bucks_shopping_cart @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_lt_bucks_shopping_cart @current_dv_batch_id

end
