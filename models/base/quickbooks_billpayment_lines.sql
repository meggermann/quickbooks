with lines as (

  {% if target.type == 'bigquery' %}

    select *,
       {{ dbt_utils.safe_cast('id', dbt_utils.type_int()) }} as bill_payment_id,
       {{ dbt_utils.surrogate_key('id', 'line.amount') }} as _id
    from {{ var('base.billpayments') }}
    cross join unnest(line) as line
    cross join unnest(line.linkedtxn) as link

  {% else %}

    select *,
       {{ dbt_utils.safe_cast(var('source_key_id_field'), dbt_utils.type_int()) }} as bill_payment_id
       {{ dbt_utils.safe_cast(var('level_0_id_field'), dbt_utils.type_int()) }} as _id
    from {{ var('base.billpayments_line') }}

  {% endif %}

),

{% if target.type != 'bigquery' %}
links as (

  select *,
         {{ dbt_utils.safe_cast(var('source_key_id_field'), dbt_utils.type_int()) }} as bill_payment_id
         {{ dbt_utils.safe_cast(var('level_0_id_field'), dbt_utils.type_int()) }} as _id
  from {{ var('base.billpayments_line__linkedtxn') }}

),
{% endif %}

billpayments_lines as (

  select
    lines._id as id,
    lines.bill_payment_id,
    amount,
    {{ dbt_utils.safe_cast('txnid', dbt_utils.type_int()) }} as bill_id,
    _sdc_received_at as received_at
  from lines
  {% if target.type != 'bigquery' %}
    inner join links on links._id = lines._id and links.bill_payment_id = lines.bill_payment_id
  {% endif %}

),

deduplicate as (

    select
        *,
        row_number() over (partition by id order by
            received_at desc) as dedupe
    from billpayments_lines

)

select * except (dedupe) from deduplicate
where dedupe = 1
