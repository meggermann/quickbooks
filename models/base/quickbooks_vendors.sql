with vendors as (

  select
    {{ dbt_utils.safe_cast('id', dbt_utils.type_int()) }} as id,
    displayname as name,
    balance,
    {% if target.type == 'bigquery' %}
      metadata.createtime as created_at,
      metadata.lastupdatedtime as updated_at,
    {% else %}
      metadata__createtime as created_at,
      metadata__lastupdatedtime as updated_at,
    {% endif %}
    _sdc_received_at as received_at
  from
    {{ var('base.vendors') }}

),

deduplicate as (

    select
        *,
        row_number() over (partition by id order by
            received_at desc) as dedupe
    from vendors

)

select * except (dedupe) from deduplicate
where dedupe = 1
