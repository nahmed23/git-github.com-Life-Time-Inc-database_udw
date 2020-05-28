CREATE PROC [dbo].[proc_etl_mms_club_product_tax_rate] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_ClubProductTaxRate

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_ClubProductTaxRate (
       bk_hash,
       ClubProductTaxRateID,
       ClubID,
       ProductID,
       TaxRateID,
       StartDate,
       EndDate,
       InsertedDatetime,
       UpdatedDateTime,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ClubProductTaxRateID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ClubProductTaxRateID,
       ClubID,
       ProductID,
       TaxRateID,
       StartDate,
       EndDate,
       InsertedDatetime,
       UpdatedDateTime,
       isnull(cast(stage_mms_ClubProductTaxRate.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_ClubProductTaxRate
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_club_product_tax_rate @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_club_product_tax_rate (
       bk_hash,
       club_product_tax_rate_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_ClubProductTaxRate.bk_hash,
       stage_hash_mms_ClubProductTaxRate.ClubProductTaxRateID club_product_tax_rate_id,
       isnull(cast(stage_hash_mms_ClubProductTaxRate.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_ClubProductTaxRate
  left join h_mms_club_product_tax_rate
    on stage_hash_mms_ClubProductTaxRate.bk_hash = h_mms_club_product_tax_rate.bk_hash
 where h_mms_club_product_tax_rate_id is null
   and stage_hash_mms_ClubProductTaxRate.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_club_product_tax_rate
if object_id('tempdb..#l_mms_club_product_tax_rate_inserts') is not null drop table #l_mms_club_product_tax_rate_inserts
create table #l_mms_club_product_tax_rate_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_ClubProductTaxRate.bk_hash,
       stage_hash_mms_ClubProductTaxRate.ClubProductTaxRateID club_product_tax_rate_id,
       stage_hash_mms_ClubProductTaxRate.ClubID club_id,
       stage_hash_mms_ClubProductTaxRate.ProductID product_id,
       stage_hash_mms_ClubProductTaxRate.TaxRateID tax_rate_id,
       stage_hash_mms_ClubProductTaxRate.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_ClubProductTaxRate.ClubProductTaxRateID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ClubProductTaxRate.ClubID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ClubProductTaxRate.ProductID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_ClubProductTaxRate.TaxRateID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_ClubProductTaxRate
 where stage_hash_mms_ClubProductTaxRate.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_club_product_tax_rate records
set @insert_date_time = getdate()
insert into l_mms_club_product_tax_rate (
       bk_hash,
       club_product_tax_rate_id,
       club_id,
       product_id,
       tax_rate_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_club_product_tax_rate_inserts.bk_hash,
       #l_mms_club_product_tax_rate_inserts.club_product_tax_rate_id,
       #l_mms_club_product_tax_rate_inserts.club_id,
       #l_mms_club_product_tax_rate_inserts.product_id,
       #l_mms_club_product_tax_rate_inserts.tax_rate_id,
       case when l_mms_club_product_tax_rate.l_mms_club_product_tax_rate_id is null then isnull(#l_mms_club_product_tax_rate_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_club_product_tax_rate_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_club_product_tax_rate_inserts
  left join p_mms_club_product_tax_rate
    on #l_mms_club_product_tax_rate_inserts.bk_hash = p_mms_club_product_tax_rate.bk_hash
   and p_mms_club_product_tax_rate.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_club_product_tax_rate
    on p_mms_club_product_tax_rate.bk_hash = l_mms_club_product_tax_rate.bk_hash
   and p_mms_club_product_tax_rate.l_mms_club_product_tax_rate_id = l_mms_club_product_tax_rate.l_mms_club_product_tax_rate_id
 where l_mms_club_product_tax_rate.l_mms_club_product_tax_rate_id is null
    or (l_mms_club_product_tax_rate.l_mms_club_product_tax_rate_id is not null
        and l_mms_club_product_tax_rate.dv_hash <> #l_mms_club_product_tax_rate_inserts.source_hash)

--calculate hash and lookup to current s_mms_club_product_tax_rate
if object_id('tempdb..#s_mms_club_product_tax_rate_inserts') is not null drop table #s_mms_club_product_tax_rate_inserts
create table #s_mms_club_product_tax_rate_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_ClubProductTaxRate.bk_hash,
       stage_hash_mms_ClubProductTaxRate.ClubProductTaxRateID club_product_tax_rate_id,
       stage_hash_mms_ClubProductTaxRate.StartDate start_date,
       stage_hash_mms_ClubProductTaxRate.EndDate end_date,
       stage_hash_mms_ClubProductTaxRate.InsertedDatetime inserted_date_time,
       stage_hash_mms_ClubProductTaxRate.UpdatedDateTime updated_date_time,
       stage_hash_mms_ClubProductTaxRate.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_ClubProductTaxRate.ClubProductTaxRateID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ClubProductTaxRate.StartDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ClubProductTaxRate.EndDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ClubProductTaxRate.InsertedDatetime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_ClubProductTaxRate.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_ClubProductTaxRate
 where stage_hash_mms_ClubProductTaxRate.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_club_product_tax_rate records
set @insert_date_time = getdate()
insert into s_mms_club_product_tax_rate (
       bk_hash,
       club_product_tax_rate_id,
       start_date,
       end_date,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_club_product_tax_rate_inserts.bk_hash,
       #s_mms_club_product_tax_rate_inserts.club_product_tax_rate_id,
       #s_mms_club_product_tax_rate_inserts.start_date,
       #s_mms_club_product_tax_rate_inserts.end_date,
       #s_mms_club_product_tax_rate_inserts.inserted_date_time,
       #s_mms_club_product_tax_rate_inserts.updated_date_time,
       case when s_mms_club_product_tax_rate.s_mms_club_product_tax_rate_id is null then isnull(#s_mms_club_product_tax_rate_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_club_product_tax_rate_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_club_product_tax_rate_inserts
  left join p_mms_club_product_tax_rate
    on #s_mms_club_product_tax_rate_inserts.bk_hash = p_mms_club_product_tax_rate.bk_hash
   and p_mms_club_product_tax_rate.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_club_product_tax_rate
    on p_mms_club_product_tax_rate.bk_hash = s_mms_club_product_tax_rate.bk_hash
   and p_mms_club_product_tax_rate.s_mms_club_product_tax_rate_id = s_mms_club_product_tax_rate.s_mms_club_product_tax_rate_id
 where s_mms_club_product_tax_rate.s_mms_club_product_tax_rate_id is null
    or (s_mms_club_product_tax_rate.s_mms_club_product_tax_rate_id is not null
        and s_mms_club_product_tax_rate.dv_hash <> #s_mms_club_product_tax_rate_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_club_product_tax_rate @current_dv_batch_id

end
