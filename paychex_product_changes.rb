# frozen_string_literal: true

date = Date.parse('nov 2020')
date_range = date.beginning_of_quarter..date.end_of_quarter

def get_quarter(date)
  (date.month / 3.0).ceil
end

account_file = "./paychex_enhanced_accounts_Q#{get_quarter(date)}_#{date.year}.csv"
worker_file = "./paychex_enhanced_workers_Q#{get_quarter(date)}_#{date.year}.csv"

`touch #{account_file}`
`touch #{worker_file}`

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

def worker_headers
  ['Bridge Instance',
   'Client Type',
   'Account Id',
   'Client Name',
   'Paychex User Id',
   'Bridge User Id',
   'User Created At',
   'User Deleted At',
   'Paychex Worker Full Name']
end

all_workers = []

CSV.open(open(account_file), 'w') do |csv|
  csv << account_headers

  last_record = ''
  found_ids = []

  PaperTrail::Version.where(item_type: 'PaychexAccount', created_at: date_range).where('object_changes ILIKE ?',
                                                                                       '%product_code%').order(:item_id).find_each do |version|
    account = PaychexAccount.with_deleted.find_by(id: version.item_id)

    next if account.blank?

    found_ids << account.id
    codes = version.object_changes.split(' ').grep(/LMS/)

    data = {
      account_id: account.id,
      prior_code: codes.first,
      new_code: codes.last
    }

    next if data == last_record

    last_record = data
    created_date = account.created_at
    created_quarter = get_quarter(created_date)
    product_change_date = version.created_at
    product_change_quarter = get_quarter(product_change_date)

    if codes.first == 'LMS_ESS' && codes.last == 'LMS_ENH'
      workers = account.workers.with_deleted.where.not('deleted_at < ?', product_change_date).where.not(
        'created_at > ?', product_change_date.end_of_quarter
      )
    end

    if codes.first == 'LMS_ENH' && codes.last == 'LMS_ESS'
      workers = account.workers.with_deleted.where.not('created_at > ?', product_change_date).where.not(
        'created_at > ?', product_change_date.end_of_quarter
      )
    end

    all_workers << workers

    csv << [
      'paychex',
      account.client_type,
      account.display_id,
      account.account_id,
      account.legal_name,
      "https://#{account.bridge_subdomain}-paychex.bridgeapp.com",
      created_date,
      account.deleted_at,
      'TRUE',
      codes.first,
      codes.last,
      product_change_date,
      product_change_date.year,
      "Q#{product_change_quarter}",
      workers.count,
      created_date.year,
      "Q#{created_quarter}"
    ]
  end

  PaychexAccount.with_deleted.where.not(id: found_ids).where(product_code: 'LMS_ENH',
                                                             created_at: date_range).find_each do |account|
    date = account.created_at

    workers = account.workers.with_deleted.where.not('created_at > ?', date.end_of_quarter)

    all_workers << workers

    quarter = get_quarter(date)
    csv << [
      'paychex',
      account.client_type,
      account.display_id,
      account.account_id,
      account.legal_name,
      "https://#{account.bridge_subdomain}-paychex.bridgeapp.com",
      date,
      account.deleted_at,
      'FALSE',
      nil,
      nil,
      nil,
      nil,
      nil,
      workers.count,
      date.year,
      "Q#{quarter}"
    ]
  end
end

CSV.open(open(worker_file), 'w') do |csv|
  csv << worker_headers
  all_workers.flatten.each do |worker|
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
