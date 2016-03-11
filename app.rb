require 'sinatra'
require "sinatra/json"

require 'og_api'
require 'redis'
require 'csv'
require 'json'
require 'sanitize'
require 'array_stats'

##### Set up
configure { set :server, :puma }

REDIS_HOST = ENV['REDIS_HOST']
REDIS_PORT = ENV['REDIS_PORT']

DATA_DIR = ENV['DATA_DIR'] || 'data'

LIST_OF_ENTITIES = {
  'columbusoh' => 'Transactions',
  'columbianaoh' => 'Checkbook',
  'claytonoh' => 'Checkbook',
  'cuyahogafallsoh' => 'Transactions',
  'brookvilleoh' => 'Checkbook',
  'bexleyoh' => 'Checkbook Bexley'
}

# OG API
OpenGov::Client.default_options = {
  base: 'https://api.ogstaging.us/',
}
OpenGov::Client.authorize! access_token: ENV['OG_ACCESS_TOKEN']

# Get a handle to redis
$redis = Redis.new(host: REDIS_HOST, port: REDIS_PORT)


##### Helper Functions

# Generate vendor search cache key
def cache_key(entity_name, transaction_name, search_query)
  "#{entity_name}.#{transaction_name}.#{search_query}"
end

# Assumes results is a hash
def cache_save(key, results)
  $redis.set(key, results.to_json)
end

# Returns a ruby hash or array
def cache_get(key)
  results = $redis.get(key)
  results && JSON.parse(results)
end

def vendor_search(search_query)
  output = []

  LIST_OF_ENTITIES.each do |entity_name, transaction_name|
    # Check if search is in cache
    ckey = cache_key(entity_name, transaction_name, search_query)
    cached_search = cache_get(ckey)

    #  Skip search if cached
    next output << cached_search if cached_search

    begin
      puts "Beginning with #{entity_name}"
      entity = OpenGov::Entity.find(entity_name)
      info_with_pop = entity.information(embed: 'population')
      population = nil
      if info_with_pop[:metrics]
        population = entity.information(embed: 'population')[:metrics]['population']['years'].first['value']
      end

      name = entity.name
      longitude = entity.information.longitude
      latitude = entity.information.latitude

      entity.reports.each do |report|
        schema = report.schema
        next unless report.grid_report? &&
                    schema.meta['title'] == transaction_name
        query_results = schema.query filter: { all: { contains: search_query } }, limit: 10000000
        results_array = query_results['transactions'].map { |element| element['amount'].to_f }

        total = results_array.total_sum.to_f.round(2)
        count = results_array.count
        max = results_array.max
        seventy_fifth = results_array.percentile(75)
        twenty_fifth = results_array.percentile(25)

        results =  [
          name,
          total,
          population,
          count,
          max,
          seventy_fifth,
          twenty_fifth,
          longitude,
          latitude
        ]

        output << results

        # Save results to Redis cache
        cache_save(ckey, results)
      end
    rescue StandardError => e
      puts "Error: #{entity_name} errored out"
      puts e
    end
  end

  output
end

# Uses Google to perform auto complete
# Request results look something like:
# {"e"=>"OYbiVoDNKoLYjwPj2K_ACg",
#  "c"=>0,
#  "u"=>
#   "https://www.google.com/complete/search?sclient=psy-ab&site=&source=hp&q=united&oq=&gs_l=&pbx=1&bav=on.2,or.&bvm=bv.116636494,d.cGc&fp=1&biw=1536&bih=740&dpr=1.25&pf=p&gs_rn=64&gs_ri=psy-ab&cp=3&gs_id=c&xhr=t&tch=1&ech=3&psi=DnviVpxgjIKPA-31t9AM.1457683214351.1",
#  "p"=>true,
#  "d"=>
#  "[\"united\",[[\"united\\u003cb\\u003e airlines\\u003c\\/b\\u003e\",0,[131]],[\"united\\u003cb\\u003e healthcare\\u003c\\/b\\u003e\",0,[131]],[\"united\\u003cb\\u003e mileage plus\\u003c\\/b\\u003e\",0],[\"united\\u003cb\\u003e flight status\\u003c\\/b\\u003e\",0]],{\"j\":\"c\",\"q\":\"z2hPE9uyMfcs4IAy1YMdvZB7Cy4\",\"t\":{\"bpc\":false,\"phi\":0,\"tlw\":false}}]"
# }
# Where the suggestions are in the key "d", which is a JSON string embedded with HTML that needs to get sanitized.
# The resulting array is the suggestions in order of greatest to least match
#
# Returns a hash of the requested query and its associated suggestions
def fetch_matches(query)
  raw_results = `curl -s 'https://www.google.com/complete/search?sclient=psy-ab&site=&source=hp&q=#{query}&oq=&gs_l=&pbx=1&bav=on.2,or.&bvm=bv.116636494,d.cGc&fp=1&biw=1536&bih=740&dpr=1.25&pf=p&gs_rn=64&gs_ri=psy-ab&cp=3&gs_id=c&xhr=t&tch=1&ech=3&psi=DnviVpxgjIKPA-31t9AM.1457683214351.1' -H 'Referer: https://www.google.com/' -H 'User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/48.0.2564.116 Chrome/48.0.2564.116 Safari/537.36'`
  parsed_results= JSON.parse(raw_results)

  raw_suggestions = JSON.parse(parsed_results['d'])
  puts raw_suggestions.inspect

  parsed_suggestions = raw_suggestions[1].map(&:first).map do |html|
    Sanitize.fragment(html)
  end

  {
    query: query,
    suggestions: parsed_suggestions
  }
end

##### Routes
get '/' do
  erb :index
end

get '/autocomplete' do
  json(fetch_matches(params[:query]))
end

get '/search' do
  @output = vendor_search(params[:query])
  erb :search
end
