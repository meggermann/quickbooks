with accounts as (

  select
    {{ dbt_utils.safe_cast('id', dbt_utils.type_int()) }} as id,
    name,
    fullyqualifiedname as fully_qualified_name,
    active,
    currentbalance as current_balance,
    accounttype as type,
    accountsubtype as subtype,
    subaccount,
    classification,
    acctnum,
    {% if target.type == 'bigquery' %}
      {{ dbt_utils.safe_cast("nullif(parentref.value, '')", dbt_utils.type_int()) }} as parent_account_id,
      metadata.createtime as created_at,
      metadata.lastupdatedtime as updated_at,
    {% else %}
      {{ dbt_utils.safe_cast("nullif(parentref__value, '')", dbt_utils.type_int()) }} as parent_account_id,
      metadata__createtime as created_at,
      metadata__lastupdatedtime as updated_at,
    {% endif %}
    _sdc_received_at as received_at
  from
    {{ var('base.accounts') }}

),

deduplicate as (

    select
        *,
        row_number() over (partition by id order by
            received_at desc) as dedupe
    from accounts

)

select * except (dedupe) from deduplicate
where dedupe = 1
