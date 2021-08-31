# frozen_string_literal: true

date = Date.parse('may 2021')

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

CSV.open(open(account_file), 'w') do |csv|
  ids = []
  csv << account_headers

  PaperTrail::Version.where(
    item_type: 'PaychexAccount'
  ).where.not(
    'created_at > ?', date.end_of_quarter
  ).where(
    'object_changes LIKE ?', '%product_code%'
  ).select(
    :item_id, :object_changes, 'MAX(created_at) AS created_at'
  ).group(:item_id, :object_changes).each do |record|
    ids << record.item_id
    account = PaychexAccount.with_deleted.where(id: record.item_id).first
    next if account.blank?

    codes = record.object_changes.split(' ').grep(/LMS/)
    account_created_date = account.created_at
    account_created_quarter = get_quarter(account_created_date)
    product_change_date = record.created_at
    product_change_quarter = get_quarter(product_change_date)

    workers = account.workers.with_deleted.where('created_at < ?', date.end_of_quarter).where.not('deleted_at < ?',
                                                                                                  date.beginning_of_quarter).count

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
      workers,
      account_created_date.year,
      "Q#{account_created_quarter}"
    ]
  end

  PaychexAccount.with_deleted.where.not(
    id: ids
  ).where(
    product_code: 'LMS_ENH'
  ).where(
    'created_at < ?', date.end_of_quarter
  ).find_each do |account|
    workers = account.workers.with_deleted.where('created_at < ?', date.end_of_quarter).where.not('deleted_at < ?',
                                                                                                  date.beginning_of_quarter).count
    quarter = get_quarter(date)
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
      workers,
      date.year,
      "Q#{quarter}"
    ]
  end
end
