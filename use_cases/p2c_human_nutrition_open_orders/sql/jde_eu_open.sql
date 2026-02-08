-- Human Nutrition Orders Dashboard
-- JD Edwards XE Europe open orders details
-- Change Log
-- 2025-04-01 DLC: Initial version complete (adapted from queries written for phase 1)
-- 2025-04-02 DLC: removed case statement for ship_date, shouldn't be needed
-- 2025-04-03 DLC: Added item_code, renamed previous item_code with legacy_item_code, item_desc to legacy_item_desc
-- 2025-04-08 DLC: Added archive_timestamp, updated order_by

select
  'JDE EU'                  as source,
  a.shdoco                  as sales_order_number,
  order_create.date_as_date as sales_order_create_date,
  case
    --this logic to identify intercompany sales was provided by the business partners, and is specific to Europe
    when a.shdcto in ('C2','C6','C7', 'S2', 'S6', 'SU') then 'IDT'
    when a.shdcto in ('C8','CJ','CO', 'CR', 'S5', 'S8', 'SJ', 'SO') then 'TRADE SALE'
    else 'UNKNOWN'
    end                          as sale_type,
  a.shkcoo                       as company_code,
  b.sdan8                        as sold_to_customer_number,
  c.abalph                       as sold_to_customer_name,
  b.sdlnid                       as sales_order_line,
  b.sditm                        as item_code,
  b.sdlitm                       as legacy_item_code,
  b.sddsc1                       as legacy_item_description,
  promised_delivery.date_as_date as ship_date,
  b.sdsoqs                       as open_pounds, --NOT confirmed
  case when b.sdfea <> 0 then b.sdfea  / 100 --sdfea = foreign extended amount
       when b.sdfea =  0 then b.sdaexp / 100 --sdaexp = amount extended price
       end          as open_usd,
  current_timestamp as archive_timestamp
from
  bronze_dev01.jdexeeu_jdeproduction_proddta.f4201 a              --sales order header
    inner join bronze_dev01.jdexeeu_jdeproduction_proddta.f4211 b --sales order line
      on a.shkcoo = b.sdkcoo
        and a.shdoco = b.sddoco
        and a.shdcto = b.sddcto
    inner join bronze_dev01.jdexeeu_jdeproduction_proddta.f0101 c --customer master (address)
      on b.sdan8 = c.aban8
    inner join sandbox.hnod_gold.dna_date_dim order_create
      on a.shtrdj = order_create.julian_date
    inner join sandbox.hnod_gold.dna_date_dim promised_delivery
      on b.sdrsdj = promised_delivery.julian_date
where
  --filter logic provided by business partners
      b.sdkcoo not in ('01840', '01843', '01960',  '01961','03001', '03009', '03010', '03012', '01929') --company_code
  and b.sdsrp4 not in ('106', '157') --excluding divisions for Animal Nutrition
  and a.shdcto not in ('ST')         --excluding order type stock transfer
  and b.sdlitm not in ('Freight')    --exclude item descriptions for freight
  and b.sdcndj = 0                   --date cancelled must be blank
  and b.sddgl  = 0                   --populated GL date would mean it's been invoiced
  --and promised_delivery.date_as_date between '2025-04-01' and '2025-04-30'
  -- and promised_delivery.date_as_date between '2025-03-01' and '2025-03-31'
  --and promised_delivery.date_as_date between cast ('{begin_date}' as date) and cast ('{end_date}' as date)
order by
  12, 2, 8
-- limit 100
