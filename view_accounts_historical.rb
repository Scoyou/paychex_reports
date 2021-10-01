sql(%(
  create or replace view historic_accounts as
  (
    select distinct bridge_instance
      ,display_id
      ,client_name
      ,bridge_domain_id
      ,bridge_url
      ,account_id
      ,account_created_at
      ,account_deleted_at::date

      ,last_value(case when client_type_updated_at::date > date_trunc('quarter','#{@date}'::date)::date
                  then client_type_new_value
                when client_type_updated_at::date <= date_trunc('quarter','#{@date}'::date)::date
                  then client_type_prior_value
                else client_type
                end)
        over ( partition by bridge_instance, account_id order by client_type_updated_at::date nulls first
              rows between unbounded preceding and unbounded following
            ) as client_type_latest

      ,last_value(case when product_code_updated_at::date > date_trunc('quarter','#{@date}'::date)::date
                  then product_code_new_value
                when product_code_updated_at::date <= date_trunc('quarter','#{@date}'::date)::date
                  then product_code_prior_value
                else product_code
                end)
        over ( partition by bridge_instance, account_id order by product_code_updated_at::date nulls first
              rows between unbounded preceding and unbounded following
            ) as product_code_latest

      ,last_value(case when product_code_updated_at::date > date_trunc('quarter','#{@date}'::date)::date
                  then product_code_prior_value
                end)
        over ( partition by bridge_instance, account_id order by product_code_updated_at::date nulls first
              rows between unbounded preceding and unbounded following
            ) as product_code_prior_value_latest

      ,last_value(case when product_code_updated_at::date > date_trunc('quarter','#{@date}'::date)::date
                  then product_code_new_value
                end)
        over ( partition by bridge_instance, account_id order by product_code_updated_at::date nulls first
              rows between unbounded preceding and unbounded following
            ) as product_code_new_value_latest

      ,last_value(case when product_code_updated_at::date > date_trunc('quarter','#{@date}'::date)::date
                  then product_code_updated_at::date
                end)
        over ( partition by bridge_instance, account_id order by product_code_updated_at::date nulls first
              rows between unbounded preceding and unbounded following
            ) as product_code_updated_at_latest

    from view_paychex_accounts_historical
    where ( account_deleted_at::date is null
          or account_deleted_at::date > date_trunc('quarter','#{@date}'::date)::date )
      and account_created_at::date < (
        date_trunc('quarter', '#{@date}'::date) + interval '3 months' - interval '1 day')::date
  )
))