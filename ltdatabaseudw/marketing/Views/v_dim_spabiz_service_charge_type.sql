﻿CREATE VIEW [marketing].[v_dim_spabiz_service_charge_type] AS select d_dim_spabiz_service_charge_type.dim_spabiz_service_charge_type_key dim_spabiz_service_charge_type_key,
       d_dim_spabiz_service_charge_type.service_charge_id service_charge_id,
       d_dim_spabiz_service_charge_type.store_number store_number,
       d_dim_spabiz_service_charge_type.d_dim_spabiz_service_charge_type_id d_dim_spabiz_service_charge_type_id,
       d_dim_spabiz_service_charge_type.deleted_date_time deleted_date_time,
       d_dim_spabiz_service_charge_type.deleted_flag deleted_flag,
       d_dim_spabiz_service_charge_type.dim_spabiz_staff_key dim_spabiz_staff_key,
       d_dim_spabiz_service_charge_type.edit_date_time edit_date_time,
       d_dim_spabiz_service_charge_type.enabled_flag enabled_flag,
       d_dim_spabiz_service_charge_type.pay_commission_flag pay_commission_flag,
       d_dim_spabiz_service_charge_type.quick_id quick_id,
       d_dim_spabiz_service_charge_type.service_charge_amount service_charge_amount,
       d_dim_spabiz_service_charge_type.service_charge_computed_by_percent_flag service_charge_computed_by_percent_flag,
       d_dim_spabiz_service_charge_type.service_charge_display_name service_charge_display_name,
       d_dim_spabiz_service_charge_type.service_charge_name service_charge_name,
       d_dim_spabiz_service_charge_type.service_charge_percent service_charge_percent,
       d_dim_spabiz_service_charge_type.taxable_flag taxable_flag
  from dbo.d_dim_spabiz_service_charge_type;