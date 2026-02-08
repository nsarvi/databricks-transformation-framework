-- Human Nutrition Orders Dashboard
-- IBM open orders detail
-- Change Log
-- 2025-03-26 DLC: Initial version begun (adapted from queries written for HNOD phase 1)
-- 2025-04-02 DLC: Added item_code, renamed previous item_code with legacy_item_code, item_desc to legacy_item_desc
-- 2025-04-08 DLC: Added archive_timestamp, open_pounds logic, updated order_by

select
  'IBM'               as source,
  a.inor_order_number as sales_order_number, --IBM recycles order_numbers, differentiated by order_date
  a.inor_order_date   as sales_order_create_date,
  i.sale_type         as sale_type,
  c.copc_code         as company_code,
  l.vncu_vendor_nbr   as sold_to_customer_number,
  l.vncu_name         as sold_to_customer_name,
  b.prod_inv_recno    as sales_order_line,
  b.prod_id           as item_code,
  j.prdcd             as legacy_item_code,
  j.abbr_desc         as legacy_item_description,
  a.inor_ship_date    as ship_date,
  case
    when b.prod_unit_basis = 'K'
    then b.prod_invoiced_wt * 2.2046
    else b.prod_invoiced_wt
    end as open_pounds, --verified with SME SH 4/7/25 DLC
  case
    --convert the unit price to the effective price, accounting for unit of measure (prod_unit_basis)
    --and unit of price (prod_price_basis) and weight and exchange rate (either exchange rate of the
    --planned ship date when today or in the past, or the latest available exchange rate when in the future.)
    when b.prod_unit_basis = '#' and b.prod_price_basis = '#' then b.prod_price                 * b.prod_invoiced_wt  * coalesce (exch_rt_ship_dt.ukurs/exch_rt_ship_dt.ffact*exch_rt_ship_dt.tfact, exch_rt_latest.latest_rate, 1)
    when b.prod_unit_basis = '#' and b.prod_price_basis = 'K' then b.prod_price / 2.2046        * b.prod_invoiced_wt  * coalesce (exch_rt_ship_dt.ukurs/exch_rt_ship_dt.ffact*exch_rt_ship_dt.tfact, exch_rt_latest.latest_rate, 1)
    when b.prod_unit_basis = '#' and b.prod_price_basis = 'M' then b.prod_price / 2.2046 / 1000 * b.prod_invoiced_wt  * coalesce (exch_rt_ship_dt.ukurs/exch_rt_ship_dt.ffact*exch_rt_ship_dt.tfact, exch_rt_latest.latest_rate, 1)
    when b.prod_unit_basis = '#' and b.prod_price_basis = 'T' then b.prod_price          / 2000 * b.prod_invoiced_wt  * coalesce (exch_rt_ship_dt.ukurs/exch_rt_ship_dt.ffact*exch_rt_ship_dt.tfact, exch_rt_latest.latest_rate, 1)
    when b.prod_unit_basis = '#' and b.prod_price_basis = 'C' then b.prod_price          / 100  * b.prod_invoiced_wt  * coalesce (exch_rt_ship_dt.ukurs/exch_rt_ship_dt.ffact*exch_rt_ship_dt.tfact, exch_rt_latest.latest_rate, 1)
    when b.prod_unit_basis = '#' and b.prod_price_basis = 'E' then b.prod_price                 * b.prod_invoiced_qty * coalesce (exch_rt_ship_dt.ukurs/exch_rt_ship_dt.ffact*exch_rt_ship_dt.tfact, exch_rt_latest.latest_rate, 1)
    when b.prod_unit_basis = 'K' and b.prod_price_basis = '#' then b.prod_price * 2.2046        * b.prod_invoiced_wt  * coalesce (exch_rt_ship_dt.ukurs/exch_rt_ship_dt.ffact*exch_rt_ship_dt.tfact, exch_rt_latest.latest_rate, 1)
    when b.prod_unit_basis = 'K' and b.prod_price_basis = 'K' then b.prod_price                 * b.prod_invoiced_wt  * coalesce (exch_rt_ship_dt.ukurs/exch_rt_ship_dt.ffact*exch_rt_ship_dt.tfact, exch_rt_latest.latest_rate, 1)
    when b.prod_unit_basis = 'K' and b.prod_price_basis = 'M' then b.prod_price          / 1000 * b.prod_invoiced_wt  * coalesce (exch_rt_ship_dt.ukurs/exch_rt_ship_dt.ffact*exch_rt_ship_dt.tfact, exch_rt_latest.latest_rate, 1)
    when b.prod_unit_basis = 'K' and b.prod_price_basis = 'C' then b.prod_price          / 100  * b.prod_invoiced_wt  * coalesce (exch_rt_ship_dt.ukurs/exch_rt_ship_dt.ffact*exch_rt_ship_dt.tfact, exch_rt_latest.latest_rate, 1)
    when b.prod_unit_basis = 'K' and b.prod_price_basis = 'E' then b.prod_price                 * b.prod_invoiced_qty * coalesce (exch_rt_ship_dt.ukurs/exch_rt_ship_dt.ffact*exch_rt_ship_dt.tfact, exch_rt_latest.latest_rate, 1)
    else                                                           b.prod_price                 * b.prod_invoiced_wt  * coalesce (exch_rt_ship_dt.ukurs/exch_rt_ship_dt.ffact*exch_rt_ship_dt.tfact, exch_rt_latest.latest_rate, 1)
    end             as open_usd,
  current_timestamp as archive_timestamp
from
  bronze_dev01.db2_db21_misdb2a.opininor_invoice_order_detail a
    inner join bronze_dev01.db2_db21_misdb2a.opinprod_product b               on a.inor_id = b.prod_inor_id
    inner join bronze_dev01.db2_db21_misdb2a.opencopc_company_profit_center c on a.inor_copc_id = c.copc_id
    left outer join bronze_dev01.db2_db21_misdb2a.opininvc_invoice e          on a.inor_id = e.invc_inor_id
    left outer join bronze_dev01.db2_db21_misdb2a.opencurr_currency f         on a.inor_curr_id = f.curr_id
    --ADM GT DNA standard is to use only one table for currency exchanges, regardless of ERP,
    --so must join in to the SAP S4 currency table: TCURR, rather than using the IBM native exchange rate table.
    left outer join bronze_ingestion_dev01.saps4_ds1_r4sadditionaltables.tcurr exch_rt_ship_dt
      on a.inor_ship_date = to_date(cast(99999999 - cast(exch_rt_ship_dt.gdatu as numeric) as string), "yyyyMMdd")
      and f.curr_code = exch_rt_ship_dt.fcurr
      and exch_rt_ship_dt.tcurr = 'USD'
      and exch_rt_ship_dt.kurst = 'M' --ADM standard "daily exchange rates"
    inner join bronze_dev01.db2_db21_misdb2a.openterm_terms_code h     on a.inor_tc_terms_code = h.term_code
    inner join bronze_dev01.db2_db21_misdb2a.opensale_sale_type i      on h.term_sale_id = i.sale_id
    inner join bronze_dev01.db2_db21_misdb2a.pdpdroot j                on b.prod_product_id = j.product_id
    inner join bronze_dev01.db2_db21_misdb2a.apvmzlog_z_logic k        on a.inor_soldto = k.zlog_vendor_num and k.zlog_vendor_type = 'C'
    inner join bronze_dev01.db2_db21_misdb2a.apvcvncu_vendorcustomer l on k.zlog_vendor_num_z = l.vncu_vendor_nbr
    --When open orders are planned to ship in the future, we won't have an exchange rate on that date,
    --because exchange rates are not calculated in the future, only today and in the past.
    --So for future planned shipments we have to use the latest (typically today's) exchange rate instead.
    left outer join (
      --Get exchange rate of the most recent date per currency.
      select f1.fcurr, max(f1_eff_dat.dna_date) as maxdt, f1.ukurs/f1.ffact*f1.tfact as latest_rate
      from   bronze_ingestion_dev01.saps4_ds1_r4sadditionaltables.tcurr f1
      inner join sandbox.ragamai.dna_date_dim f1_eff_dat
        on f1.gdatu = f1_eff_dat.days_until_99999999
      inner join (
        --Get most recent exchange rate date per currency.
        select   max (f3_eff_dat.dna_date) as max_gdatu, f3.fcurr
        from     bronze_ingestion_dev01.saps4_ds1_r4sadditionaltables.tcurr f3
                 inner join sandbox.ragamai.dna_date_dim f3_eff_dat
                   on f3.gdatu = f3_eff_dat.days_until_99999999
        where    f3.tcurr = 'USD'
                 and f3.kurst = 'M' --ADM standard "daily exchange rates"
        group by f3.fcurr
      ) f2 on f1_eff_dat.dna_date = f2.max_gdatu
      where f1.tcurr = 'USD'
            and f1.kurst = 'M' --ADM standard "daily exchange rates"
      group by f1.fcurr, f1.ukurs/f1.ffact*f1.tfact
    ) as exch_rt_latest on f.curr_code = exch_rt_latest.fcurr
where
  --filter logic provided by business partners
  (
    --Limit to Human Nutrition company codes only
    (c.copc_code in ('1010', '1034', '1035', '1201', '1202', '1203', '1206', '2058', '4208')
     and b.prod_inventory_sw = '1')
    or
    c.copc_code = '1079'
  )
  and a.inor_status_code in ('1', '2', '3', '5') --1. entered, 2. printed, 3. shipped, 4. cancelled, 5. reissued
  --ERP natively splits intercompany and third party sales for us
  and i.sale_type in ('IDT', 'TRADE SALE')
  --"open orders" are identified by the lack of an invoice date
  and e.invc_invoice_date is null
  -- and a.inor_ship_date between '2025-04-01' and '2025-04-30'
  --and a.inor_ship_date between '2025-03-01' and '2025-03-31'
  --and a.inor_ship_date between cast ('{begin_date}' as date) and cast ('{end_date}' as date)
order by
  12, 2, 8
-- limit 100
