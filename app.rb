require 'sinatra'
#require 'sinatra/reloader' if development?
require 'tilt/erb'

require 'og_api'
require 'redis'
require 'array_stats'
require 'csv'
require 'json'
require 'rgeo/geo_json'

REDIS_HOST = ENV['REDIS_HOST']
REDIS_PORT = ENV['REDIS_PORT']

LIST_OF_ENTITIES = {
  'columbusoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-83.2102799',
    'latitude' => '39.808631'
  },
  'columbianaoh' => {
    'report_name' => 'Checkbook',
    'longitude' => '-80.69396429999999',
    'latitude' => '40.88839309999999',
    'population' => '6323'
  },
  'claytonoh' => {
    'report_name' => 'Checkbook',
    'longitude' => '-84.3605022',
    'latitude' => '39.8631101',
    'population' => '13213'
  },
  'cuyahogafallsoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-81.48455849999999',
    'latitude' => '41.1339449',
    'population' => '49267'
  },
  'brookvilleoh' => {
    'report_name' => 'Checkbook',
    'longitude' => '-84.4113366',
    'latitude' => '39.8367207',
    'population' => '5884'
  },
  'bexleyoh' => {
    'report_name' => 'Checkbook Bexley',
    'longitude' => '-82.93768039999999',
    'latitude' => '39.9689532'
  },
  'stowoh' => {
    'report_name' => 'Checkbook',
    'longitude' => '-81.44038979999999',
    'latitude' => '41.1595005'
  },
  'streetsborooh' => {
    'report_name' => 'Transactions',
    'longitude' => '-81.3459405',
    'latitude' => '41.2392227'
  },
  'wellstonoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-82.53293769999999',
    'latitude' => '39.1234054'
  },
  'worthingtonoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-83.017962',
    'latitude' => '40.0931191'
  },
  'toledooh' => {
    'report_name' => 'Transactions',
    'longitude' => '-83.55521200000001',
    'latitude' => '41.6639383'
  },
  'ketteringoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-84.1688274',
    'latitude' => '39.68950359999999'
  },
  'pataskalaoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-82.6743341',
    'latitude' => '39.9956193'
  },
  'columbusoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-82.99879419999999',
    'latitude' => '39.9611755'
  },
  'tallmadgeoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-81.441779',
    'latitude' => '41.1014451'
  },
  'northcantonoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-81.4023356',
    'latitude' => '40.875891'
  },
  'wapakonetaoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-84.1935594',
    'latitude' => '40.5678265'
  },
  'belpreoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-81.5729029',
    'latitude' => '39.27396390000001'
  },
  'grandviewheightsoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-83.0407403',
    'latitude' => '39.9797863'
  },
  'chardonoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-81.2133262',
    'latitude' => '41.5786639'
  },
  'lakewoodoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-81.8388992',
    'latitude' => '41.4819932'
  },
  'allianceoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-81.1571478',
    'latitude' => '40.9117945'
  },
  'ashlandoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-82.3791289',
    'latitude' => '40.8671542'
  },
  'barbertonoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-81.6420968',
    'latitude' => '41.008796'
  },
  'bellefontaineoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-83.8458485',
    'latitude' => '40.3559636'
  },
  'bucyrusoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-83.0052713',
    'latitude' => '40.8077682'
  },
  'delawareoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-83.1056248',
    'latitude' => '40.2828612'
  },
  'eastlakeoh' => {
    'report_name' => 'Transactions',
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
    'longitude' => '-83.6471168',
    'latitude' => '39.2146192'
  },
  'hubbardoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-80.6001207',
    'latitude' => '41.1595698'
  },
  'huberheightsoh' => {
    'report_name' => 'Transactions V2',
    'longitude' => '-84.1832889',
    'latitude' => '39.8588637'
  },
  'mansfieldoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-82.5936789',
    'latitude' => '40.7662998'
  },
  'massillonoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-81.6318358',
    'latitude' => '40.7743768'
  },
  'middletownoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-81.6318358',
    'latitude' => '40.7743768'
  },
  'montgomeryoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-84.3751818',
    'latitude' => '39.2472501'
  },
  'newalbanyoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-82.8678377',
    'latitude' => '40.0827607'
  },
  'newfranklinoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-81.6247023',
    'latitude' => '40.9495915'
  },
  'northroyaltonoh' => {
    'report_name' => 'North Royalton FY2013-FY2014 2015-06-17',
    'longitude' => '-81.7799248',
    'latitude' => '41.3137551'
  },
  'portclintonoh' => {
    'report_name' => 'Transactions',
    'longitude' => '-82.953299',
    'latitude' => '41.5095992'
  },
  'salemoh' => {
    'report_name' => 'Transactions2',
    'longitude' => '-80.8860602',
    'latitude' => '40.902369'
  },
  'sanduskyoh' => {
    'report_name' => 'Transactions',
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
        query_results = schema.query filter: { all: { contains: search_query } }, limit: 10000000
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

get '/' do
  erb :index
end

get '/search' do
  @output = convert_to_geojson(vendor_search(params[:query]))
  erb :search
end
