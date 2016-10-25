# Get data from the morph.io api
require 'rest-client'
require 'json'
require 'pry'
require 'scraperwiki'
require 'active_support'
require 'active_support/core_ext'
require 'geokit'

if ENV['MORPH_API_KEY'].blank?
  puts "Must have MORPH_API_KEY set"
  exit(1)
end

# Set an API key if provided
Geokit::Geocoders::GoogleGeocoder.api_key = ENV['MORPH_GOOGLE_API_KEY'] if ENV['MORPH_GOOGLE_API_KEY']

def url
  'https://api.morph.io/disclosurelogs/au-wa-food-offenses/data.json'
end

def morph_api_key
  ENV['MORPH_API_KEY']
end

def fetch
  return @records if @records
  params = { :key => morph_api_key, :query => "select * from 'data'" }
  result = RestClient.get(url, :params => params)
  @records = JSON.parse(result)
end

def upstream_records
  fetch
end

def upstream_record_ids
  upstream_records.map{|r| r['notice_pdf_url']}
end

def geocode(record)
  @addresses ||= {}

  address = record['business_location']

  if @addresses[address]
    puts "Geocoding [cache hit] #{address}"
    location = @addresses[address]
  else
    puts "Geocoding #{address}"
    a = Geokit::Geocoders::GoogleGeocoder.geocode(address)

    record['lat'] = a.lat
    record['lng'] = a.lng
  end
  record
end

begin
  existing_record_ids = ScraperWiki.select('notice_pdf_url from data').map {|r| r['notice_pdf_url']}
rescue SqliteMagic::NoSuchTable
  existing_record_ids = []
end

new_records = (upstream_record_ids - existing_record_ids)

puts "### Geocoding #{new_records.size} new records"

new_records.map! do |id|
  record = upstream_records.find{|r| r['notice_pdf_url'] == id}
  geocode(record)
end

ScraperWiki.save_sqlite(['notice_pdf_url'], new_records)
