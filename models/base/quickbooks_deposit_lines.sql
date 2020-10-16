with lines as (

  {% if target.type == 'bigquery' %}

    select *,
          {{ dbt_utils.surrogate_key('line.id', 'deposits.id') }} as _id,
          {{ dbt_utils.safe_cast('deposits.id', dbt_utils.type_int()) }} as deposit_id
    from {{ var('base.deposits') }} as deposits
    cross join unnest(line) as line

  {% else %}

    select *,
          {{ var('level_0_id_field') }}::int as _id,
          {{ var('source_key_id_field') }}::int as deposit_id
    from {{ var('base.deposits_line') }}

  {% endif %}

),

{% if target.type != 'bigquery' %}
  links as (

    select *,
          {{ var('level_0_id_field') }}::int as _id,
          {{ var('source_key_id_field') }}::int as deposit_id
    from {{ var('base.deposits_line_linkedtxn') }}

  ),
{% endif %}

deposit_lines as (

  select
    lines._id as id,
    lines.deposit_id as deposit_id,
    lines.amount as amount,
    {% if target.type == 'bigquery' %}
      {% if var('classes_enabled', true) %}
      {{ dbt_utils.safe_cast("nullif(lines.depositlinedetail.classref.value, '')", dbt_utils.type_bigint()) }} as class_id,
      {% endif %}
      {{ dbt_utils.safe_cast("nullif(lines.depositlinedetail.accountref.value, '')", dbt_utils.type_int()) }} as account_id,
    {% else %}
      {% if var('classes_enabled', true) %}
      nullif(lines.depositlinedetail__classref__value::varchar, '')::bigint as class_id,
      {% endif %}
      nullif(lines.depositlinedetail__accountref__value::varchar, '')::int as account_id,
      links.txnid::int as payment_id,
    {% endif %}
    _sdc_received_at as received_at
  from lines
  {% if target.type != 'bigquery' %}
    left outer join links on
      lines._id = links._id and
      lines.deposit_id = links.deposit_id
  {% endif %}

),

deduplicate as (

    select
        *,
        row_number() over (partition by id order by
            received_at desc) as dedupe
    from deposit_lines

)

select * except (dedupe) from deduplicate
where dedupe = 1
