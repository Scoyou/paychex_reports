# frozen_string_literal: true

role_finder = RoleFinder.new(sub_account: s)

enhanced_roles = role_finder.visible(include_deprecated: false).select do |zuul_role|
  zuul_role.name.downcase.match?(/([^\s]+) enhanced/)
end.map(&:id)
User.joins(:grants).where('grants.role_id IN (?)', enhanced_roles)

file = open('/app-learn-web/tmp/paychex_users.csv')

CSV.open(file, 'w') do |csv|
  csv << %w[sub_account_name sub_account_id user_id user_uid deleted_user user_roles]
  SubAccount.first do |s|
    count = SubAccount.active.count -= 1
    puts count
    role_finder = RoleFinder.new(sub_account: s)
    enhanced_roles = role_finder.visible(include_deprecated: false).select do |r|
                       r.name.downcase.match?(/([^\s]+) enhanced/)
                     end.map { |r| { role_id: r.id, role_name: r.name } }
    User.joins(:grants).where('grants.role_id IN (?)', enhanced_roles.map do |e|
                                                         e[:role_id]
                                                       end).where(created_at: Date.parse('jan 1 2021')..Date.parse('March 31 2021')).uniq.find_each(batch_size: 1000) do |user|
      user_roles = user.roles.pluck(:role_id)
      role_names = []
      user_roles.each do |role|
        role_names << enhanced_roles.find { |e| e[:role_id] == role }[:role_name]
      end
      csv << [
        s.name,
        s.id,
        user.id,
        user.uid,
        user.deleted_at.present?,
        role_names
      ]
    end
  end
end

all_enhanced_roles = []
SubAccount.active.find_each(batch_size: 50) do |sub_account|
  role_finder = RoleFinder.new(sub_account: sub_account)
  count -= 1
  puts count
  all_enhanced_roles << role_finder.visible(include_deprecated: false).select do |r|
                          r.name.downcase.match?(/([^\s]+) enhanced/)
                        end.map { |r| { role_id: r.id, role_name: r.name } }
end
