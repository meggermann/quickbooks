{{
  config(
    enabled = var('payments_enabled', true)
  )
}}

with payments as (

  select
    {{ dbt_utils.safe_cast('id', dbt_utils.type_int()) }} as id,
    totalamt as total,
    {{ dbt_utils.safe_cast('txndate', 'date') }} as txn_date,
    --txnsource as txn_source,
    unappliedamt as unapplied_amt,
    --creditcardpayment__creditchargeinfo__processpayment as cc_pmt_processed,
    processpayment as payment_processed,
    {% if target.type == 'bigquery' %}
      {{ dbt_utils.safe_cast("nullif('deposittoaccountref.value', '')", dbt_utils.type_int()) }} as account_id,
      {{ dbt_utils.safe_cast('customerref.value', dbt_utils.type_int()) }} as customer_id,
      {{ dbt_utils.safe_cast("nullif(paymentmethodref.value, '')", dbt_utils.type_int()) }} as payment_method_id,
      metadata.createtime as created_at,
      metadata.lastupdatedtime as updated_at,
    {% else %}
      {{ dbt_utils.safe_cast("nullif('deposittoaccountref__value', '')", dbt_utils.type_int()) }} as account_id,
      {{ dbt_utils.safe_cast('customerref__value', dbt_utils.type_int()) }} as customer_id,
      {{ dbt_utils.safe_cast('nullif(paymentmethodref__value, '')', dbt_utils.type_int()) }} as payment_method_id,
      metadata__createtime as created_at,
      metadata__lastupdatedtime as updated_at,
    {% endif %}
    _sdc_received_at as received_at
  from
    {{ var('base.payments') }}

),

deduplicate as (

    select
        *,
        row_number() over (partition by id order by
            received_at desc) as dedupe
    from payments

)

select * except (dedupe) from deduplicate
where dedupe = 1
