require 'sinatra'
require "sinatra/json"

require 'og_api'
require 'redis'
require 'csv'
require 'json'
require 'sanitize'
require 'array_stats'
require 'rgeo/geo_json'

##### Set up
configure { set :server, :puma }

REDIS_HOST = ENV['REDIS_HOST']
REDIS_PORT = ENV['REDIS_PORT']

DATA_DIR = ENV['DATA_DIR'] || 'data'

LIST_OF_ENTITIES = {
  'columbusoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'vendor_name',
    'longitude' => '-83.2102799',
    'latitude' => '39.808631'
  },
  'columbianaoh' => {
    'report_name' => 'Checkbook',
    'vendor_name' => 'vendor_name',
    'longitude' => '-80.69396429999999',
    'latitude' => '40.88839309999999',
    'population' => '6323'
  },
  'claytonoh' => {
    'report_name' => 'Checkbook',
    'vendor_name' => 'vendor_name',
    'longitude' => '-84.3605022',
    'latitude' => '39.8631101',
    'population' => '13213'
  },
  'cuyahogafallsoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'Name',
    'longitude' => '-81.48455849999999',
    'latitude' => '41.1339449',
    'population' => '49267'
  },
  'brookvilleoh' => {
    'report_name' => 'Checkbook',
    'vendor_name' => 'vendor_name',
    'longitude' => '-84.4113366',
    'latitude' => '39.8367207',
    'population' => '5884'
  },
  'bexleyoh' => {
    'report_name' => 'Checkbook Bexley',
    'vendor_name' => 'vendor_name',
    'longitude' => '-82.93768039999999',
    'latitude' => '39.9689532'
  },
  'stowoh' => {
    'report_name' => 'Checkbook',
    'vendor_name' => 'vendor_name',
    'longitude' => '-81.44038979999999',
    'latitude' => '41.1595005'
  },
  'streetsborooh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'vendor_name',
    'longitude' => '-81.3459405',
    'latitude' => '41.2392227'
  },
  'wellstonoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'vendor_name',
    'longitude' => '-82.53293769999999',
    'latitude' => '39.1234054'
  },
  'worthingtonoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'Name',
    'longitude' => '-83.017962',
    'latitude' => '40.0931191'
  },
  'toledooh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'vendor_name',
    'longitude' => '-83.55521200000001',
    'latitude' => '41.6639383'
  },
  'ketteringoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'vendor_name',
    'longitude' => '-84.1688274',
    'latitude' => '39.68950359999999'
  },
  'pataskalaoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'vendor_name',
    'longitude' => '-82.6743341',
    'latitude' => '39.9956193'
  },
  'columbusoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'vendor_name',
    'longitude' => '-82.99879419999999',
    'latitude' => '39.9611755'
  },
  'tallmadgeoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'name',
    'longitude' => '-81.441779',
    'latitude' => '41.1014451'
  },
  'northcantonoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'vendor_name',
    'longitude' => '-81.4023356',
    'latitude' => '40.875891'
  },
  'wapakonetaoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'name',
    'longitude' => '-84.1935594',
    'latitude' => '40.5678265'
  },
  'belpreoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'vendor_name',
    'longitude' => '-81.5729029',
    'latitude' => '39.27396390000001'
  },
  'grandviewheightsoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'vendor_name',
    'longitude' => '-83.0407403',
    'latitude' => '39.9797863'
  },
  'chardonoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'vendor_name',
    'longitude' => '-81.2133262',
    'latitude' => '41.5786639'
  },
  'lakewoodoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'vendor',
    'longitude' => '-81.8388992',
    'latitude' => '41.4819932'
  },
  'allianceoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'vendor_name',
    'longitude' => '-81.1571478',
    'latitude' => '40.9117945'
  },
  'ashlandoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'vendor_name',
    'longitude' => '-82.3791289',
    'latitude' => '40.8671542'
  },
  'barbertonoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'vendor_name',
    'longitude' => '-81.6420968',
    'latitude' => '41.008796'
  },
  'bellefontaineoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'name',
    'longitude' => '-83.8458485',
    'latitude' => '40.3559636'
  },
  'bucyrusoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'vendor_name',
    'longitude' => '-83.0052713',
    'latitude' => '40.8077682'
  },
  'delawareoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'name',
    'longitude' => '-83.1056248',
    'latitude' => '40.2828612'
  },
  'eastlakeoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'name',
    'longitude' => '-81.4663042',
    'latitude' => '41.6593455'
  },
  # No public reports
  # 'hamiltonoh' => {
  #   'report_name' => 'Hamilton_OH_2015.06.09',
  #   'longitude' => '-84.6305353',
  #   'latitude' => '39.3959952'
  # },
  'hillsborooh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'name',
    'longitude' => '-83.6471168',
    'latitude' => '39.2146192'
  },
  'hubbardoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'name',
    'longitude' => '-80.6001207',
    'latitude' => '41.1595698'
  },
  'huberheightsoh' => {
    'report_name' => 'Transactions V2',
    'vendor_name' => 'name',
    'longitude' => '-84.1832889',
    'latitude' => '39.8588637'
  },
  'mansfieldoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'vendor',
    'longitude' => '-82.5936789',
    'latitude' => '40.7662998'
  },
  'massillonoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'vendor_name',
    'longitude' => '-81.6318358',
    'latitude' => '40.7743768'
  },
  'middletownoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'vendor_name',
    'longitude' => '-81.6318358',
    'latitude' => '40.7743768'
  },
  'montgomeryoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'name',
    'longitude' => '-84.3751818',
    'latitude' => '39.2472501'
  },
  'newalbanyoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'name',
    'longitude' => '-82.8678377',
    'latitude' => '40.0827607'
  },
  'newfranklinoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'vendor_name',
    'longitude' => '-81.6247023',
    'latitude' => '40.9495915'
  },
  # 'northroyaltonoh' => {
  #   'report_name' => 'North Royalton FY2013-FY2014 2015-06-17',
  #   'longitude' => '-81.7799248',
  #   'latitude' => '41.3137551'
  # },
  'portclintonoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'vendor_name',
    'longitude' => '-82.953299',
    'latitude' => '41.5095992'
  },
  'salemoh' => {
    'report_name' => 'Transactions2',
    'vendor_name' => 'vendor_name',
    'longitude' => '-80.8860602',
    'latitude' => '40.902369'
  },
  'sanduskyoh' => {
    'report_name' => 'Transactions',
    'vendor_name' => 'vendor_name',
    'longitude' => '-82.7721684',
    'latitude' => '41.4550543'
  }
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

def percentile(values, percentile)
  return 0 unless values && values.size > 0
  return values[0] if values.size == 1
  values_sorted = values.sort
  k = (percentile*(values_sorted.length-1)+1).floor - 1
  f = (percentile*(values_sorted.length-1)+1).modulo(1)

  return values_sorted[k] + (f * (values_sorted[k+1] - values_sorted[k]))
end

def vendor_search(search_query)
  output = []

  LIST_OF_ENTITIES.each do |entity_name, data|
    # Check if search is in cache
    ckey = cache_key(entity_name, data['report_name'], search_query)
    cached_search = cache_get(ckey)

    #  Skip search if cached
    next output << cached_search if cached_search

    begin
      puts "Beginning with #{entity_name}"
      entity = OpenGov::Entity.find(entity_name)
      info_with_pop = entity.information(embed: 'population')
      population = data['population']
      if info_with_pop[:metrics]
        population ||= entity.information(embed: 'population')[:metrics]['population']['years'].first['value']
      end

      name = entity.name
      longitude = data['longitude']
      latitude = data['latitude']

      entity.reports.each do |report|
        schema = report.schema
        next unless report.grid_report? &&
                    schema.meta['title'] == data['report_name']
        query_results = schema.query filter: { data['vendor_name'].to_sym => { contains: search_query } }, limit: 10000000
        results_array = query_results['transactions'].map { |element| element['amount'].to_f }

        total = results_array.total_sum.to_f.round(2)
        count = results_array.count
        max = results_array.max
        seventy_fifth = percentile(results_array, 0.75)
        twenty_fifth = percentile(results_array, 0.25)

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

  output.sort_by{ |k| k[1] }.reverse
end

def convert_to_geojson(results)
  output_hash = {
    'type' => 'FeatureCollection',
    'features' => []
  }

  results.each do |line|
    output_hash['features'] << {
      'type' => 'Feature',
      'geometry' => {
        'type' => 'Point',
        'coordinates' => [line[7], line[8]]
      },
      'properties' => {
        'entity' => line[0],
        'total' => line[1],
        'population' => line[2],
        'count' => line[3],
        'max' => line[4],
        '75th' => line[5],
        '25th' => line[6]
      }
    }
  end

  output_hash.to_json
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
  @output = convert_to_geojson(vendor_search(params[:query]))
  erb :search
end
