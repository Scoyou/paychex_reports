# frozen_string_literal: true

def sql(statement)
  ActiveRecord::Base.connection.execute(statement)
end

def create_object_versions_table
  sql('drop table if exists test_object_versions cascade;')
  sql(
    "create temporary table test_object_versions (
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
  ).where(
    created_at: Date.parse("sept 1 2021").midnight.. Date.parse("sept 5 2021").end_of_day
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
      "insert into test_object_versions (
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

create_object_versions_table
