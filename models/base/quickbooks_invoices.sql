{{
  config(
    enabled = var('invoices_enabled', true)
  )
}}

with invoices as (

  select
      {{ dbt_utils.safe_cast('id', dbt_utils.type_int()) }} as id,
      totalamt as total_amt,
      {{ dbt_utils.safe_cast('txndate', 'date') }} as txn_date,
      {{ dbt_utils.safe_cast('duedate', 'date') }} as due_date,
      balance,
      {% if target.type == 'bigquery' %}
      deliveryinfo.deliverytype as delivery_type,
      {% else %}
      deliveryinfo__deliverytype as delivery_type,
      {% endif %}
      emailstatus as email_status,
      docnumber as doc_number,
      {% if target.type == 'bigquery' %}
        {{ dbt_utils.safe_cast('deliveryinfo.deliverytime', 'datetime') }} as delivery_time,
        {{ dbt_utils.safe_cast('customerref.value', dbt_utils.type_int()) }} as customer_id,
        metadata.createtime as created_at,
        metadata.lastupdatedtime as updated_at,
      {% else %}
        {{ dbt_utils.safe_cast('deliveryinfo__deliverytime', 'datetime') }} as delivery_time,
        {{ dbt_utils.safe_cast('customerref__value', dbt_utils.type_int()) }} as customer_id,
        metadata__createtime as created_at,
        metadata__lastupdatedtime as updated_at,
      {% endif %}
      _sdc_received_at as received_at
  from
      {{ var('base.invoices') }}

),

deduplicate as (

    select
        *,
        row_number() over (partition by id order by
            received_at desc) as dedupe
    from invoices

)

select * except (dedupe) from deduplicate
where dedupe = 1
