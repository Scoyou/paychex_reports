# frozen_string_literal: true

class PaychexBillingReport
  def sql(statement)
    ActiveRecord::Base.connection.execute(statement)
  end

  def get_quarter(date)
    (date.month / 3.0).ceil
  end

  def object_versions_exists?
    !sql('select * from object_versions limit 1').blank?
  rescue StandardError => e
    puts e.message
  end

  def perform(date)
    @date = date
    @quarter = get_quarter(date)

    create_object_versions_table unless object_versions_exists?
    run_queries
    # write_to_csv
  end

  def create_object_versions_table
    sql('drop table if exists object_versions cascade;')
    sql(
      "create temporary table object_versions (
      item_id integer,
      id integer,
      account_id integer,
      client_type text,
      client_type_from text,
      client_type_to text,
      company_id text,
      display_id text,
      legal_name text,
      product_code text,
      product_code_from text,
      product_code_to text,
      updated_at_to text
      )"
    )

    PaperTrail::Version.where(
      "object_changes LIKE '%product_code%'
      OR object_changes LIKE '%client_type%'
      OR object_changes LIKE '%client_type%'
      AND object_changes LIKE '%product_code%'
      "
    ).find_in_batches do |batch|
      objects = []
      batch.each do |record|
        object_changes = record.object_changes.split(' ')
        product_code_changes = object_changes.grep(/LMS/)
        client_type_changes = object_changes.grep(/PEO|ASO|HRE|BPR/)

        split_object = record.object.split("\n")
        attributes = {}
        split_object.each do |item|
          split = item.split(': ')

          attributes[split.first.to_sym] = split.size > 1 ? split.last : nil
        end

        attributes.each_value { |i| i&.delete!("'") }

        objects << "
          (
            '#{record.item_id}',
            '#{attributes[:client_type]}',
            '#{client_type_changes&.first}',
            '#{client_type_changes&.last}',
            '#{attributes[:company_id]}',
            '#{attributes[:display_id]}',
            '#{attributes[:account_id].to_i}',
            '#{attributes[:legal_name]}',
            '#{attributes[:product_code]}',
            '#{product_code_changes&.first}',
            '#{product_code_changes&.last}',
            '#{record.created_at}'
          )
        ".strip.delete!("\n")
      end
      sql(
        "insert into object_versions (
            item_id,
            client_type,
            client_type_from,
            client_type_to,
            company_id,
            display_id,
            account_id,
            legal_name,
            product_code,
            product_code_from,
            product_code_to,
            updated_at_to
            )
          VALUES
            #{objects.join(',')}
          "
      )
    end
  end

  def run_queries
    # view_paychex_updates
    sql(%(
    drop view if exists view_paychex_updates cascade;

    create or replace view view_paychex_updates AS

    select
      item_id as version_id,
          account_id,
          company_id,
          legal_name,
          'client_type' as change_type,
          client_type_from as prior_value,
          client_type_to as new_value,
          updated_at_to as updated_at
    from object_versions
    where client_type_from != ''

    union all

    select
      item_id as version_id,
          account_id,
          company_id,
          legal_name,
          'product_code' as change_type,
          product_code_from as prior_value,
          product_code_to as new_value,
          updated_at_to as updated_at
    from object_versions
    where product_code_from != ''
    order by updated_at
    ))

    sql(%(

    drop view if exists view_paychex_accounts_historical cascade;

    create or replace view view_paychex_accounts_historical as

    with accounts_with_updates as
    (
      select a.id as account_id
        ,a2.display_id
        ,coalesce(a2.legal_name,a.name) as client_name
        ,case when a2.legal_name ilike '%&&%'
              or a2.legal_name ilike '%polarson%'
              or a.name ilike '%&&%'
              or a.name ilike '%polarson%'
            then true
          else false end as test_account
        ,a.bridge_domain_id
        ,a.bridge_url
        ,pct.name as client_type
        ,pat.name as product_code
        ,a.created_at::date
        ,a.deleted_at::date
        ,a2.primary
        ,case when u.change_type = 'client_type' then u.prior_value end as client_type_prior_value
        ,case when u.change_type = 'client_type' then u.new_value end as client_type_new_value
        ,case when u.change_type = 'client_type' then u.updated_at::date end as client_type_updated_at
        ,case when u.change_type = 'product_code' then u.prior_value end as product_code_prior_value
        ,case when u.change_type = 'product_code' then u.new_value end as product_code_new_value
        ,case when u.change_type = 'product_code' then u.updated_at end as product_code_updated_at
        ,rank () over ( partition by a.id
                    order by (case when a2.legal_name ilike '%&&%'
                                or a2.legal_name ilike '%polarson%'
                                or a.name ilike '%&&%'
                                or a.name ilike '%polarson%'
                              then true
                            else false end)
                      ,(case when a2.primary = true then 1 else 2 end)
                      ,a2.display_id
                  ) as account_priority
      from accounts a
        left join bridge_template_accounts as ta on a.bridge_template_account_id = ta.id
        left join paychex_account_types as pat on ta.paychex_account_type_id = pat.id
        left join paychex_client_types as pct on ta.paychex_client_type_id = pct.id
        left join paychex_accounts a2 on a.id = a2.account_id
        left join view_paychex_updates u on a.id = u.account_id
    )

    select 'paychex' as bridge_instance
      ,display_id
      ,client_name
      ,bridge_domain_id
      ,bridge_url
      ,client_type
      ,product_code
      ,account_id
      ,created_at as account_created_at
      ,deleted_at as account_deleted_at
      ,client_type_prior_value
      ,client_type_new_value
      ,client_type_updated_at
      ,product_code_prior_value
      ,product_code_new_value
      ,product_code_updated_at::date
    from accounts_with_updates
    where test_account = false
      and account_priority = 1
    ;
    ))
    # view_billing_detail_by_user
    sql(%(
      drop view if exists view_billing_detail_by_user cascade;

      create or replace view view_billing_detail_by_user as

      with historic_accounts as
      (
        select distinct bridge_instance
          ,display_id
          ,client_name
          ,bridge_domain_id
          ,bridge_url
          ,account_id
          ,account_created_at
          ,account_deleted_at::date

          ,last_value(case when client_type_updated_at::date < date_trunc('quarter','#{@date}'::date)::date
                      then client_type_new_value
                    when client_type_updated_at::date >= date_trunc('quarter','#{@date}'::date)::date
                      then client_type_prior_value
                    else client_type
                    end)
            over ( partition by bridge_instance, account_id order by client_type_updated_at::date nulls first
                  rows between unbounded preceding and unbounded following
                ) as client_type_latest

          ,last_value(case when product_code_updated_at::date < date_trunc('quarter','#{@date}'::date)::date
                      then product_code_new_value
                    when product_code_updated_at::date >= date_trunc('quarter','#{@date}'::date)::date
                      then product_code_prior_value
                    else product_code
                    end)
            over ( partition by bridge_instance, account_id order by product_code_updated_at::date nulls first
                  rows between unbounded preceding and unbounded following
                ) as product_code_latest

          ,last_value(case when product_code_updated_at::date < date_trunc('quarter','#{@date}'::date)::date
                      then product_code_prior_value
                    end)
            over ( partition by bridge_instance, account_id order by product_code_updated_at::date nulls first
                  rows between unbounded preceding and unbounded following
                ) as product_code_prior_value_latest

          ,last_value(case when product_code_updated_at::date < date_trunc('quarter','#{@date}'::date)::date
                      then product_code_new_value
                    end)
            over ( partition by bridge_instance, account_id order by product_code_updated_at::date nulls first
                  rows between unbounded preceding and unbounded following
                ) as product_code_new_value_latest

          ,last_value(case when product_code_updated_at::date < date_trunc('quarter','#{@date}'::date)::date
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
      ,enhanced_accounts as
      (
        select *
          ,case when product_code_updated_at_latest >= date_trunc('quarter','#{@date}'::date)::date
                  and product_code_updated_at_latest < (
                    date_trunc('quarter', '#{@date}'::date) + interval '3 months' - interval '1 day')::date
                then 'change in period'
              when product_code_latest = 'LMS_ENH'
                then 'enhanced'
              end as enhanced_category
        from historic_accounts
      )

      ,paychex_users as
      (
        select w.account_id
          ,w.created_at
          ,w.deleted_at
          ,w.bridge_user_id
          ,coalesce( case when w.composite_uid like '%:%' then split_part(w.composite_uid,':',2) end
                ,case when w.composite_uid not like '%:%' and pu.paychex_user_id is not null then pu.paychex_user_id end
                ,case when w.composite_uid not like '%:%' and pu.paychex_user_id is null then w.composite_uid end
                ) as paychex_user_id
          ,w.full_name as paychex_worker_full_name
          ,w.email as paychex_worker_email
          ,pu.first_name as paychex_user_first_name
          ,pu.last_name as paychex_user_last_name
          ,pu.email as paychex_user_email
          ,case when nu.paychex_user_id is not null then 'user' else 'worker' end as status_in_middleware
          ,nu.created_at as created_at_middleware_user_table
        from workers as w
          left join accounts as a on a.id = w.account_id
          left join users as pu on w.user_id = pu.id
          left join users as nu on nullif(split_part(w.composite_uid, ':', 2),'') = nu.paychex_user_id
      )
      ,users_in_period as
      (
        select ea.bridge_instance
          ,ea.client_type_latest
          ,ea.display_id
          ,ea.account_id
          ,ea.client_name
          ,ea.bridge_url
          ,ea.bridge_domain_id
          ,ea.account_created_at::date
          ,ea.account_deleted_at::date
          ,ea.product_code_prior_value_latest
          ,ea.product_code_new_value_latest
          ,ea.product_code_updated_at_latest
          ,ea.enhanced_category
          ,p.paychex_user_id as paychex_user_id
          ,p.bridge_user_id as bridge_user_id
          ,p.created_at as user_created_at
          ,p.deleted_at as user_deleted_at
          ,p.paychex_worker_full_name
          ,p.paychex_worker_email
          ,p.paychex_user_first_name
          ,p.paychex_user_last_name
          ,p.paychex_user_email
          ,p.status_in_middleware
          ,p.created_at_middleware_user_table
        from enhanced_accounts as ea
          left join paychex_users as p on ea.bridge_instance = 'paychex' and ea.account_id = p.account_id
        where (case when ea.enhanced_category = 'change in period' and ea.product_code_new_value_latest = 'LMS_ESS'
                    and p.created_at < ea.product_code_updated_at_latest
                    and (p.deleted_at is null
                        or p.deleted_at > date_trunc('quarter','#{@date}'::date)::date
                      )
                  then 1
                when ea.enhanced_category = 'change in period' and ea.product_code_new_value_latest = 'LMS_ENH'
                    and p.created_at < (
                      date_trunc('quarter','#{@date}'::date) + interval '3 months' - interval '1 day')::date
                    and (p.deleted_at is null
                        or p.deleted_at > ea.product_code_updated_at_latest
                      )
                  then 1
                when ea.enhanced_category = 'enhanced'
                    and p.created_at < (
                      date_trunc('quarter','#{@date}'::date) + interval '3 months' - interval '1 day')::date
                    and (p.deleted_at is null
                        or p.deleted_at > date_trunc('quarter','#{@date}'::date)::date
                      )
                  then 1
                end) = 1
      )
      select bridge_instance
        ,client_type_latest as client_type
        ,display_id
        ,account_id
        ,client_name
        ,bridge_url
        ,account_created_at::date
        ,account_deleted_at::date
        ,case when product_code_updated_at_latest is not null then 'True' else 'False' end as product_code_change
        ,product_code_prior_value_latest as prior_product_code
        ,product_code_new_value_latest as new_product_code
        ,product_code_updated_at_latest as product_code_change_date
        ,paychex_user_id
        ,min(user_created_at) as user_created_at
        ,max(user_deleted_at) as user_deleted_at
        ,min(bridge_user_id) as bridge_user_id
        ,min(paychex_worker_full_name) as paychex_worker_full_name
        ,min(paychex_worker_email) as paychex_worker_email
        ,min(paychex_user_first_name) as paychex_user_first_name
        ,min(paychex_user_last_name) as paychex_user_last_name
        ,min(paychex_user_email) as paychex_user_email
        ,max(status_in_middleware) as status_in_middleware
        ,min(created_at_middleware_user_table) as created_at_middleware_user_table
        ,date_trunc('quarter','#{@date}'::date)::date as report_period_start
        ,(date_trunc('quarter', '#{@date}'::date) + interval '3 months' - interval '1 day')::date as report_period_end
      from users_in_period
      group by bridge_instance
        ,client_type_latest
        ,display_id
        ,account_id
        ,client_name
        ,bridge_url
        ,account_created_at::date
        ,account_deleted_at::date
        ,case when product_code_updated_at_latest is not null then 'True' else 'False' end
        ,product_code_prior_value_latest
        ,product_code_new_value_latest
        ,product_code_updated_at_latest
        ,paychex_user_id
      order by bridge_instance
        ,client_type_latest
        ,display_id
        ,account_id
        ,client_name
        ,bridge_url
        ,paychex_user_id
      ;

      ))

    # paychex_enh_qtr_billing_detail
    sql(%(
        drop view if exists paychex_enh_qtr_billing_detail;

    create or replace view paychex_enh_qtr_billing_detail as

    select bridge_instance
      ,client_type
      ,display_id
      ,account_id
      ,client_name
      ,bridge_url
      ,account_created_at
      ,account_deleted_at
      ,product_code_change
      ,prior_product_code
      ,new_product_code
      ,product_code_change_date
      ,count(paychex_user_id) as user_count
      ,report_period_start
      ,report_period_end
    from view_billing_detail_by_user
    group by bridge_instance
      ,client_type
      ,display_id
      ,account_id
      ,client_name
      ,bridge_url
      ,account_created_at
      ,account_deleted_at
      ,product_code_change
      ,prior_product_code
      ,new_product_code
      ,product_code_change_date
      ,report_period_start
      ,report_period_end
    order by bridge_instance
      ,client_type
      ,display_id
      ,account_id
      ,client_name
      ,bridge_url
    ;
      ))
    # paychex_enh_qtr_billing_summary
    sql(%(
    drop view if exists paychex_enh_qtr_billing_summary;

    create view paychex_enh_qtr_billing_summary as

    select bridge_instance
      ,client_type
      ,count(account_id) as accounts
      ,sum(user_count) as user_count
      ,date_trunc('quarter','#{@date}'::date)::date as report_period_start
      ,(date_trunc('quarter', '#{@date}'::date)  + interval '3 months' - interval '1 day')::date as report_period_end
    from paychex_enh_qtr_billing_detail
    group by bridge_instance
      ,client_type
    order by bridge_instance
      ,client_type
    ;
    ))
  end

  def write_to_csv
    path = "./Q#{@quarter}_reports"
    `mkdir #{path}`
    [
      {
        file: "#{path}/paychex_enh_qtr_billing_detail_by_user.csv",
        results: sql('SELECT * FROM view_billing_detail_by_user').to_a
      },
      {
        file: "#{path}/paychex_enh_qtr_billing_detail.csv",
        results: sql('SELECT * FROM paychex_enh_qtr_billing_detail').to_a
      },
      {
        file: "#{path}/paychex_enh_qtr_billing_summary.csv",
        results: sql('SELECT * FROM paychex_enh_qtr_billing_summary').to_a
      }
    ].each do |query|
      `touch #{query[:file]}`
      CSV.open(File.open(query[:file]), 'w') do |csv|
        csv << query[:results].first.keys
        query[:results].each do |row|
          csv << row.values
        end
      end
    end
  end
end

reports = PaychexBillingReport.new
reports.perform(Date.parse('Dec 01 2020'))
