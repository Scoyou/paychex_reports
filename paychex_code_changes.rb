# frozen_string_literal: true

date = Date.parse("sept 15 2021")

def get_quarter(date)
  (date.month / 3.0).ceil
end

account_file = "./accounts_Q#{get_quarter(date)}_#{date.year}.csv"

`touch #{account_file}`

def account_headers
  ['Bridge Instance',
   'Client Type',
   'Display Id',
   'Account Id',
   'Client Name',
   'Bridge Url',
   'Account Created At',
   'Account Deleted At',
   'Product Code Change',
   'Prior Product Code',
   'New Product Code',
   'Product Code Change Date',
   'Product Code Change Year',
   'Product Code Change Quarter',
   'User Count',
   'Account Created Year',
   'Account Created Quarter']
end

def workers_query(account, created, deleted)
  account.workers.with_deleted.where(
    'deleted_at > ?
     OR deleted_at IS NULL
    ', deleted
  ).where(
    'created_at < ?', created
  ).where.not(
    "workers.composite_uid ILIKE '%duplicate%'"
  )
end

CSV.open(open(account_file), 'w') do |csv|
  csv << account_headers

  @all_workers = []

  PaperTrail::Version.where(
    "object_changes LIKE '%product_code%'
    AND created_at < ?", date.end_of_quarter
  ).select(
    'DISTINCT ON ("item_id") *'
  ).order(:item_id, created_at: :desc).each do |record|
    account = PaychexAccount.with_deleted.where(
      id: record.item_id
    ).where(
      'created_at < ?', date.end_of_quarter
    ).where.not(
      "legal_name ILIKE '%&&%'"
    ).first
    next if account.blank?

    codes = record.object_changes.split(' ').grep(/LMS/) 

    next if codes.last == 'LMS_ESS' && !record.created_at.between?(date.beginning_of_quarter, date.end_of_quarter)

    account_created_date = account.created_at
    account_created_quarter = get_quarter(account_created_date)
    product_change_date = record.created_at
    product_change_quarter = get_quarter(product_change_date)

    if codes.first == 'LMS_ESS' && codes.last == 'LMS_ENH'
      highest_date = [product_change_date, date.beginning_of_quarter].max
      workers = workers_query(account, date.end_of_quarter, highest_date)

    elsif codes.first == 'LMS_ENH' && codes.last == 'LMS_ESS'
      workers = workers_query(account, product_change_date, date.beginning_of_quarter)

    else
      workers = workers_query(account, date.end_of_quarter, date.beginning_of_quarter)
    end

    @all_workers << workers

    csv << [
      'paychex',
      account.client_type,
      account.display_id,
      account.account_id,
      account.legal_name,
      "https://#{account.bridge_subdomain}-paychex.bridgeapp.com",
      account_created_date,
      account.deleted_at,
      'TRUE',
      codes.first,
      codes.last,
      product_change_date,
      product_change_date.year,
      "Q#{product_change_quarter}",
      workers.count,
      account_created_date.year,
      "Q#{account_created_quarter}"
    ]
  end

  pt_ids = PaperTrail::Version.where(item_type: 'PaychexAccount').where("object_changes LIKE '%product_code%'").pluck(:item_id).uniq
  account_ids = PaychexAccount.with_deleted.where.not(id: pt_ids).where(product_code: 'LMS_ENH').pluck(:id).uniq

  PaychexAccount.with_deleted.where(
    id: account_ids
  ).where(
    product_code: 'LMS_ENH'
  ).where(
    'created_at < ?', date.end_of_quarter
  ).find_each do |account|
    workers = workers_query(account, date.end_of_quarter, date.beginning_of_quarter)

    @all_workers << workers

    quarter = get_quarter(account.created_at)

    csv << [
      'paychex',
      account.client_type,
      account.display_id,
      account.account_id,
      account.legal_name,
      "https://#{account.bridge_subdomain}-paychex.bridgeapp.com",
      account.created_at,
      account.deleted_at,
      'FALSE',
      nil,
      nil,
      nil,
      nil,
      nil,
      workers.count,
      account.created_at.year,
      "Q#{quarter}"
    ]
  end
end

worker_file = "./workers_Q#{get_quarter(date)}_#{date.year}.csv"

`touch #{worker_file}`

CSV.open(open(worker_file), 'w') do |csv|
  csv << ['Bridge Instance',
          'Client Type',
          'Account Id',
          'Client Name',
          'Paychex User Id',
          'Bridge User Id',
          'User Created At',
          'User Deleted At',
          'Paychex Worker Full Name']

  @all_workers.flatten.each do |worker|
    csv << [
      'paychex',
      worker.paychex_account.client_type,
      worker.paychex_account.account_id,
      worker.paychex_account.legal_name,
      worker.user_id,
      worker.bridge_user_id,
      worker.created_at,
      worker.deleted_at,
      worker.full_name
    ]
  end
end
