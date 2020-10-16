with bill_payments as (

  select
    {{ dbt_utils.safe_cast('id', dbt_utils.type_int()) }} as id,
    totalamt as total,
    {{ dbt_utils.safe_cast('txndate', 'date') }} as txn_date,
    {% if target.type == 'bigquery' %}
      {{ dbt_utils.safe_cast('vendorref.value', dbt_utils.type_int()) }} as vendor_id,
      coalesce(
          {% if var('creditcard_payments_for_bills', true) %}
          {{ dbt_utils.safe_cast("nullif(creditcardpayment.ccaccountref.value,'')", dbt_utils.type_int()) }},
          {% endif %}
          {{ dbt_utils.safe_cast("nullif(checkpayment.bankaccountref.value, '')", dbt_utils.type_int()) }}
      ) as payment_account_id,
      metadata.createtime as created_at,
      metadata.lastupdatedtime as updated_at,
    {% else %}
      {{ dbt_utils.safe_cast('vendorref__value', dbt_utils.type_int()) }} as vendor_id,
      coalesce(
          {% if var('creditcard_payments_for_bills', true) %}
          {{ dbt_utils.safe_cast("nullif(creditcardpayment__ccaccountref__value, '')", dbt_utils.type_int()) }},
          {% endif %}
          {{ dbt_utils.safe_cast("nullif(checkpayment__bankaccountref__value, '')", dbt_utils.type_int()) }}
      ) as payment_account_id,
      metadata__createtime as created_at,
      metadata__lastupdatedtime as updated_at,
    {% endif %}
    _sdc_received_at as received_at
  from
    {{ var('base.billpayments') }}

),

deduplicate as (

    select
        *,
        row_number() over (partition by id order by
            received_at desc) as dedupe
    from bill_payments

)

select * except (dedupe) from deduplicate
where dedupe = 1
