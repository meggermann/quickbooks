with bills as (

  select
    {{ dbt_utils.safe_cast('id', dbt_utils.type_int()) }} as id,
    {{ dbt_utils.safe_cast('txndate', 'date') }} as txn_date,
    totalamt as total,
    {{ dbt_utils.safe_cast('duedate', 'date') }} as due_date,
    balance,
    {% if target.type == 'bigquery' %}
      {{ dbt_utils.safe_cast('apaccountref.value', dbt_utils.type_int()) }} as ap_account_id,
      metadata.createtime as created_at,
      metadata.lastupdatedtime as updated_at,
    {% else %}
      {{ dbt_utils.safe_cast('apaccountref__value', dbt_utils.type_int()) }} as ap_account_id,
      metadata__createtime as created_at,
      metadata__lastupdatedtime as updated_at,
    {% endif %}
    _sdc_received_at as received_at
  from
    {{ var('base.bills') }}

),

deduplicate as (

    select
        *,
        row_number() over (partition by id order by
            received_at desc) as dedupe
    from bills

)

select * except (dedupe) from deduplicate
where dedupe = 1
