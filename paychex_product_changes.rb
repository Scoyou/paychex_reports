# frozen_string_literal: true

file = open('./paychex_enhanced_accounts.csv')

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
    'Product Code Change',
    'Prior Product Code',
    'New Product Code',
    'Product Code Change Date',
    'User Count'
  ]

  date_range = Date.parse('jan 1 2021')..Date.parse('jun 30 2021')

  last_record = ''
  
  PaperTrail::Version.where(item_type: 'PaychexAccount', created_at: date_range).where(
    'object_changes ILIKE ?', '%product_code%'
  ).order(:item_id).find_each do |version|
    account = PaychexAccount.with_deleted.find_by(id: version.item_id)

    codes = version.object_changes.split(' ').grep(/LMS/)

    data = {
      account_id: account.id,
      prior_code: codes.first,
      new_code: codes.last
    }

    next if data == last_record

    last_record = data

    csv << [
      "paychex",
      account.client_type,
      account.display_id,
      account.account_id,
      account.legal_name,
      "https://#{account.bridge_subdomain}-paychex.bridgeapp.com",
      account.created_at,
      account.deleted_at,
      'TRUE',
      codes.first,
      codes.last,
      version.created_at,
      account.workers.count
    ]
  end
end
