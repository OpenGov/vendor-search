require 'sinatra'
require 'og_api'
require 'array_stats'
require 'csv'

OpenGov::Client.authorize! access_token: ENV['OG_ACCESS_TOKEN']
OpenGov::Client.default_options = { base: 'https://api.ogstaging.us/' }

LIST_OF_ENTITIES = {
  'columbusoh' => 'Transactions',
  'columbianaoh' => 'Checkbook',
  'claytonoh' => 'Checkbook',
  'cuyahogafallsoh' => 'Transactions',
  'brookvilleoh' => 'Checkbook',
  'bexleyoh' => 'Checkbook Bexley'
}

def vendor_search(search_query)
  output = []

  LIST_OF_ENTITIES.each do |entity_name, transaction_name|
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

        output << [
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
      end
    rescue StandardError => e
      puts "Error: #{entity_name} errored out"
      puts e
    end
  end

  output
end

get '/' do
  erb :index
end

get '/search' do
  @output = vendor_search(params[:query])
  erb :search
end
