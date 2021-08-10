# frozen_string_literal: true

file = open('./Q4_2020_new_accounts.csv')

CSV.open(file, 'w') do |csv|
  csv << [
    'Bridge Instance',
    'Client Type',
    'Display Id',
    'Account Id',
    'Client Name',
    'Bridge Url',
    'Account Created At',
    'Account Deleted At',
    'Product Code',
    'User Count'
  ]

  date_range = Date.parse('oct 1 2020')..Date.parse('dec 31 2020')

  PaychexAccount.with_deleted.where(created_at: date_range).find_each do |account|
    csv << [
      'paychex',
      account.client_type,
      account.display_id,
      account.account_id,
      account.legal_name,
      "https://#{account.bridge_subdomain}-paychex.bridgeapp.com",
      account.created_at,
      account.deleted_at,
      account.product_code,
      account.workers.count
    ]
  end
end
