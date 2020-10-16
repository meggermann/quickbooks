with purchases as (

  select
    {{ dbt_utils.safe_cast('id', dbt_utils.type_int()) }} as id,
    paymenttype as payment_type,
    totalamt as total,
    {{ dbt_utils.safe_cast('txndate', 'date') }} as txn_date,
    credit,
    {% if target.type == 'bigquery' %}
      {{ dbt_utils.safe_cast('accountref.value', dbt_utils.type_int()) }} as account_id,
      --the following three fields contain information on who was paid for this record.
      --the payment can be made to a vendor, customer, or employee. this is found in entity_type.
      --based on entity_type, the id value should be mapped to the corresponding table to get details.
      {{ dbt_utils.safe_cast('entityref.value', dbt_utils.type_int()) }} as vendor_id,
      entityref.name as entity_name,
      entityref.type as entity_type,
      metadata.createtime as created_at,
      metadata.lastupdatedtime as updated_at,
    {% else %}
      {{ dbt_utils.safe_cast('accountref__value', dbt_utils.type_int()) }} as account_id,
      --the following three fields contain information on who was paid for this record.
      --the payment can be made to a vendor, customer, or employee. this is found in entity_type.
      --based on entity_type, the id value should be mapped to the corresponding table to get details.
      {{ dbt_utils.safe_cast('entityref__value', dbt_utils.type_int()) }} as vendor_id,
      entityref__name as entity_name,
      entityref__type as entity_type,
      metadata__createtime as created_at,
      metadata__lastupdatedtime as updated_at,
    {% endif %}
    _sdc_received_at as received_at
  from
    {{ var('base.purchases') }}

),

deduplicate as (

    select
        *,
        row_number() over (partition by id order by
            received_at desc) as dedupe
    from purchases

)

select * except (dedupe) from deduplicate
where dedupe = 1
