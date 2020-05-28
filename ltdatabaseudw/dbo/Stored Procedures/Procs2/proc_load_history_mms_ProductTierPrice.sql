CREATE PROC [dbo].[proc_load_history_mms_ProductTierPrice] AS
begin

set nocount on
set xact_abort on

/*Select the records from [dbo].[MMSProductTierPrice] to be staged and inserted into the dv tables*/

if object_id('tempdb.dbo.#stage_mms_ProductTierPrice_history') is not null drop table #stage_mms_ProductTierPrice_history
create table dbo.#stage_mms_ProductTierPrice_history with (location=user_db, distribution = hash(ProductTierPriceID)) as
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ProductTierPriceID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ProductTierPriceID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(ProductTierID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(ValCardLevelID as varchar(500)),'z#@$k%&P'))),2)   l_mms_product_tier_price_hash,
        convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ProductTierPriceID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(Price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,UpdatedDateTime,120),'z#@$k%&P'))),2)  s_mms_product_tier_price_hash ,

        row_number() over(partition by ProductTierPriceID order by x.update_insert_date) rank2,
		*
  from (select row_number() over(partition by ProductTierPriceID,
              case when MMSUpdatedDateTime is null then MMSInsertedDateTime
                   when datepart(hh, isnull(MMSUpdatedDateTime,InsertedDateTime)) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, isnull(MMSUpdatedDateTime,InsertedDateTime))))
                   else dateadd(hh, 27, convert(datetime, convert(date, isnull(MMSUpdatedDateTime,InsertedDateTime))))
                   end
                   order by [MMSProductTierPriceKey] desc) rank1,
              case when MMSUpdatedDateTime is null then MMSInsertedDateTime
                   when datepart(hh, isnull(MMSUpdatedDateTime,InsertedDateTime)) in (0, 1) then dateadd(hh, 3, convert(datetime, convert(date, isnull(MMSUpdatedDateTime,InsertedDateTime))))
                   else dateadd(hh, 27, convert(datetime, convert(date, isnull(MMSUpdatedDateTime,InsertedDateTime))))
               end update_insert_date,
               *
          from stage_mms_ProductTierPrice_history) x
 where rank1 = 1					
                              
/* Create the h records.*/
/* dv_load_date_time is the MMSInsertedDateTime or Jan 1, 1980 if MMSInsertedDateTime is null.*/
/* dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS*/
insert into dbo.h_mms_product_tier_price(
	   bk_hash,
       product_tier_price_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select 
       x.*
  from (select bk_hash,
              #stage_mms_ProductTierPrice_history.ProductTierPriceID ,
               isnull(cast(#stage_mms_ProductTierPrice_history.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time, 
               case when MMSInsertedDateTime is null then 19800101000000
                    else replace(replace(replace(convert(varchar, MMSInsertedDateTime,120 ), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user 
          from dbo.#stage_mms_ProductTierPrice_history 
         where rank2 = 1) x
         		
/* Create the l records.*/
/* Calculate dv_load_date_time*/
/* dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS*/
insert into dbo.l_mms_product_tier_price (
	   bk_hash,
       product_tier_price_id,
       product_tier_id,
       val_membership_type_group_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
	   	
select 
       x.*
  from (select #stage_mms_ProductTierPrice_history.bk_hash,
       #stage_mms_ProductTierPrice_history.ProductTierPriceID product_tier_price_id,
       #stage_mms_ProductTierPrice_history.ProductTierID product_tier_id,
       #stage_mms_ProductTierPrice_history.ValMembershipTypeGroupID val_membership_type_group_id,
               case when #stage_mms_ProductTierPrice_history.rank2 = 1 then
                         case when #stage_mms_ProductTierPrice_history.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_ProductTierPrice_history.MMSInsertedDateTime
                          end
                    when datepart(hh, isnull(#stage_mms_ProductTierPrice_history.MMSUpdatedDateTime,#stage_mms_ProductTierPrice_history.InsertedDateTime)) in (0, 1) 
					then dateadd(hh, 3, convert(datetime, convert(date, isnull(#stage_mms_ProductTierPrice_history.MMSUpdatedDateTime,#stage_mms_ProductTierPrice_history.InsertedDateTime))))
                    else dateadd(hh, 27, convert(datetime, convert(date, isnull(#stage_mms_ProductTierPrice_history.MMSUpdatedDateTime,#stage_mms_ProductTierPrice_history.InsertedDateTime))))
                end dv_load_date_time,
               case when #stage_mms_ProductTierPrice_history.rank2 = 1 then
                         case when #stage_mms_ProductTierPrice_history.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_ProductTierPrice_history.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                    when datepart(hh, isnull(#stage_mms_ProductTierPrice_history.MMSUpdatedDateTime,#stage_mms_ProductTierPrice_history.InsertedDateTime)) in (0, 1) then 
					replace(replace(replace(convert(varchar, dateadd(hh, 3, convert(datetime, convert(date, isnull(#stage_mms_ProductTierPrice_history.MMSUpdatedDateTime,#stage_mms_ProductTierPrice_history.InsertedDateTime)))),120), '-', ''),' ', ''), ':', '')
                    else replace(replace(replace(convert(varchar, dateadd(hh, 27, convert(datetime, convert(date, isnull(#stage_mms_ProductTierPrice_history.MMSUpdatedDateTime,#stage_mms_ProductTierPrice_history.InsertedDateTime)))),120), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               #stage_mms_ProductTierPrice_history.l_mms_product_tier_price_hash,
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user 
           from dbo.#stage_mms_ProductTierPrice_history
          left join dbo.#stage_mms_ProductTierPrice_history prior
            on #stage_mms_ProductTierPrice_history.ProductTierPriceID = prior.ProductTierPriceID
           and #stage_mms_ProductTierPrice_history.rank2 = prior.rank2 + 1
         where #stage_mms_ProductTierPrice_history.l_mms_product_tier_price_hash != isnull(prior.l_mms_product_tier_price_hash, ''))x

insert into dbo.l_mms_product_tier_price_1 (
	   bk_hash,
       product_tier_price_id,
       val_card_level_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
	   	
select 
       x.*
  from (select #stage_mms_ProductTierPrice_history.bk_hash,
       #stage_mms_ProductTierPrice_history.ProductTierPriceID product_tier_price_id,
       #stage_mms_ProductTierPrice_history.ValCardLevelID val_card_level_id,
               case when #stage_mms_ProductTierPrice_history.rank2 = 1 then
                         case when #stage_mms_ProductTierPrice_history.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_ProductTierPrice_history.MMSInsertedDateTime
                          end
                    when datepart(hh, isnull(#stage_mms_ProductTierPrice_history.MMSUpdatedDateTime,#stage_mms_ProductTierPrice_history.InsertedDateTime)) in (0, 1) 
					then dateadd(hh, 3, convert(datetime, convert(date, isnull(#stage_mms_ProductTierPrice_history.MMSUpdatedDateTime,#stage_mms_ProductTierPrice_history.InsertedDateTime))))
                    else dateadd(hh, 27, convert(datetime, convert(date, isnull(#stage_mms_ProductTierPrice_history.MMSUpdatedDateTime,#stage_mms_ProductTierPrice_history.InsertedDateTime))))
                end dv_load_date_time,
               case when #stage_mms_ProductTierPrice_history.rank2 = 1 then
                         case when #stage_mms_ProductTierPrice_history.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_ProductTierPrice_history.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                    when datepart(hh, isnull(#stage_mms_ProductTierPrice_history.MMSUpdatedDateTime,#stage_mms_ProductTierPrice_history.InsertedDateTime)) in (0, 1) then 
					replace(replace(replace(convert(varchar, dateadd(hh, 3, convert(datetime, convert(date, isnull(#stage_mms_ProductTierPrice_history.MMSUpdatedDateTime,#stage_mms_ProductTierPrice_history.InsertedDateTime)))),120), '-', ''),' ', ''), ':', '')
                    else replace(replace(replace(convert(varchar, dateadd(hh, 27, convert(datetime, convert(date, isnull(#stage_mms_ProductTierPrice_history.MMSUpdatedDateTime,#stage_mms_ProductTierPrice_history.InsertedDateTime)))),120), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               #stage_mms_ProductTierPrice_history.l_mms_product_tier_price_hash,
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user 
           from dbo.#stage_mms_ProductTierPrice_history
          left join dbo.#stage_mms_ProductTierPrice_history prior
            on #stage_mms_ProductTierPrice_history.ProductTierPriceID = prior.ProductTierPriceID
           and #stage_mms_ProductTierPrice_history.rank2 = prior.rank2 + 1
         where #stage_mms_ProductTierPrice_history.l_mms_product_tier_price_hash != isnull(prior.l_mms_product_tier_price_hash, ''))x

/* Create the s records.*/
/* dv_batch_id is the dv_load_date_time converted to YYYYMMDDHHMISS*/
insert into dbo.s_mms_product_tier_price (
       bk_hash,
       product_tier_price_id,
       price,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select 
       x.*
  from (select #stage_mms_ProductTierPrice_history.bk_hash,
       #stage_mms_ProductTierPrice_history.ProductTierPriceID product_tier_price_id,
       #stage_mms_ProductTierPrice_history.price price,
	   #stage_mms_ProductTierPrice_history.MMSInsertedDateTime inserted_date_time,
       #stage_mms_ProductTierPrice_history.MMSUpdatedDateTime updated_date_time,
               case when #stage_mms_ProductTierPrice_history.rank2 = 1 then
                         case when #stage_mms_ProductTierPrice_history.MMSInsertedDateTime is null then convert(datetime,'jan 1, 1980',107)
                              else #stage_mms_ProductTierPrice_history.MMSInsertedDateTime
                          end
                    when datepart(hh, isnull(#stage_mms_ProductTierPrice_history.MMSUpdatedDateTime,#stage_mms_ProductTierPrice_history.InsertedDateTime)) in (0, 1) 
					then dateadd(hh, 3, convert(datetime, convert(date, isnull(#stage_mms_ProductTierPrice_history.MMSUpdatedDateTime,#stage_mms_ProductTierPrice_history.InsertedDateTime))))
                    else dateadd(hh, 27, convert(datetime, convert(date, isnull(#stage_mms_ProductTierPrice_history.MMSUpdatedDateTime,#stage_mms_ProductTierPrice_history.InsertedDateTime))))
                end dv_load_date_time,
               case when #stage_mms_ProductTierPrice_history.rank2 = 1 then
                         case when #stage_mms_ProductTierPrice_history.MMSInsertedDateTime is null then 19800101000000
                              else replace(replace(replace(convert(varchar, #stage_mms_ProductTierPrice_history.MMSInsertedDateTime,120), '-', ''),' ', ''), ':', '')
                          end
                    when datepart(hh, isnull(#stage_mms_ProductTierPrice_history.MMSUpdatedDateTime,#stage_mms_ProductTierPrice_history.InsertedDateTime)) in (0, 1) 
					then replace(replace(replace(convert(varchar, dateadd(hh, 3, convert(datetime, convert(date, isnull(#stage_mms_ProductTierPrice_history.MMSUpdatedDateTime,#stage_mms_ProductTierPrice_history.InsertedDateTime)))),120), '-', ''),' ', ''), ':', '')
                    else replace(replace(replace(convert(varchar, dateadd(hh, 27, convert(datetime, convert(date, isnull(#stage_mms_ProductTierPrice_history.MMSUpdatedDateTime,#stage_mms_ProductTierPrice_history.InsertedDateTime)))),120), '-', ''),' ', ''), ':', '')
                end dv_batch_id,
               2 dv_r_load_source_id, 
               #stage_mms_ProductTierPrice_history.s_mms_product_tier_price_hash,
               getdate() dv_inserted_date_time, 
               suser_sname() dv_insert_user
                   from dbo.#stage_mms_ProductTierPrice_history
          left join dbo.#stage_mms_ProductTierPrice_history prior
            on #stage_mms_ProductTierPrice_history.ProductTierPriceID = prior.ProductTierPriceID
           and #stage_mms_ProductTierPrice_history.rank2 = prior.rank2 + 1
         where #stage_mms_ProductTierPrice_history.s_mms_product_tier_price_hash != isnull(prior.s_mms_product_tier_price_hash, ''))x

end
