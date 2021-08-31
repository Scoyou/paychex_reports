# frozen_string_literal: true

date = Date.parse('jan 2021')

def get_quarter(date)
  (date.month / 3.0).ceil
end

account_file = "./paychex_enhanced_accounts_Q#{get_quarter(date)}_#{date.year}.csv"

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

def get_account_data(date, record)

  account = Account.with_deleted.find(record.account_id)

  codes = record.object_changes.split(' ').grep(/LMS/)
  created_date = record.created_at
  created_quarter = get_quarter(created_date)
  product_change_date = record.versions_created_at
  product_change_quarter = get_quarter(product_change_date)

  workers = account.workers.with_deleted.where('created_at < ?', date.end_of_quarter).where.not('deleted_at < ?', date.beginning_of_quarter).count

  [
    'paychex',
    account.client_type,
    account.display_id,
    account.id,
    account.name,
    "https://#{account.bridge_subdomain}-paychex.bridgeapp.com",
    created_date,
    account.deleted_at,
    'TRUE',
    codes.first,
    codes.last,
    product_change_date,
    product_change_date.year,
    "Q#{product_change_quarter}",
    workers,
    created_date.year,
    "Q#{created_quarter}"
  ]
end

# @all_workers = []

CSV.open(open(account_file), 'w') do |csv|
  csv << account_headers

  found_records = []
  found_account_ids = []

  PaychexAccount.with_deleted.where(
    'paychex_accounts.created_at < ?', date.end_of_quarter
  ).joins(
    'INNER JOIN versions ON versions.item_id = paychex_accounts.id'
  ).where(
    'versions.object_changes LIKE ?', '%product_code%'
  ).select(
    '*,
     versions.created_at AS versions_created_at,
     paychex_accounts.id AS p_account_id
    '
  ).order("versions.created_at DESC").each do |record|
    next if record.blank?
    next if found_records.include? record.p_account_id
    next if found_account_ids.include? record.account_id

    found_records << record.p_account_id
    found_account_ids << record.account_id

    next if record.versions_created_at > date.end_of_quarter

    data = get_account_data(date, record)
    csv << data
  end
 
  Account.with_deleted.where.not(
    id: found_account_ids
  ).where(
    product_code: 'LMS_ENH'
  ).where(
    'created_at < ?', date.end_of_quarter
  ).find_each do |account|

    workers = account.workers.with_deleted.where('created_at < ?', date.end_of_quarter).where.not('deleted_at < ?', date.beginning_of_quarter).count

    quarter = get_quarter(date)
    csv << [
      'paychex',
      account.client_type,
      account.display_id,
      account.id,
      account.name,
      "https://#{account.bridge_subdomain}-paychex.bridgeapp.com",
      account.created_at,
      account.deleted_at,
      'FALSE',
      nil,
      nil,
      nil,
      nil,
      nil,
      workers,
      date.year,
      "Q#{quarter}"
    ]
  end
end
