# encoding: utf-8

require "logstash/filters/base"
require "logstash/namespace"
require "json"
require "time"
require "dalli"
require_relative "utils/dimensions"
require_relative "mobility/location_data"

module ConfigVariables
  attr_accessor :consolidated_time, :expired_time,:max_dwell_time,:expired_repetitions_time
  def self.setConsolidatedTime(value)
    @@consolidated_time = value
  end

  def self.consolidated_time
    @@consolidated_time
  end

  def self.setExpiredTime(value)
    @@expired_time = value
  end
 
  def self.expired_time
    @@expired_time
  end

  def self.setMaxDwellTime(value)
    @@max_dwell_time = value
  end

  def self.max_dwell_time
    @@max_dwell_time
  end

  def self.setExpiredRepetitionsTime(value)
    @@expired_repetitions_time = value
  end

  def self.expired_repetitions_time
    @@expired_repetitions_time
  end
end
class LogStash::Filters::Mobility < LogStash::Filters::Base

  config_name "mobility"

  config :consolidated_time,       :validate => :number, :default => 180,   :required => false
  config :expired_time,            :validate => :number, :default => 1200,  :required => false
  config :max_dwell_time,           :validate => :number, :default => 1440,  :required => false
  config :expired_repetitions_time, :validate => :number, :default => 10080, :required => false
  
  public
  def register
    ConfigVariables.setConsolidatedTime(@consolidated_time)
    ConfigVariables.setExpiredTime(@expired_time)
    ConfigVariables.setMaxDwellTime(@max_dwell_time)
    ConfigVariables.setExpiredRepetitionsTime(@expired_repetitions_time)

    @store = {}
    @dim_to_enrich = [MARKET_UUID, ORGANIZATION_UUID, ZONE_UUID, NAMESPACE_UUID,
                   DEPLOYMENT_UUID, SENSOR_UUID, NAMESPACE, SERVICE_PROVIDER_UUID, 
                   BUILDING_UUID, CAMPUS_UUID, FLOOR_UUID,
                   STATUS, CLIENT_PROFILE, CLIENT_RSSI_NUM]
    @memcached = Dalli::Client.new("localhost:11211", {:expires_in => 0})
    @store = @memcached.get("location") || {}
    puts "Im mobility!"
  end

  def filter(event)
     client = event.get(CLIENT).to_s
     namespace = (event.get(NAMESPACE)) ? event.get(NAMESPACE) : ""
     id = client + namespace

     if client
       events = []
       current_location = LocationData.location_from_message(event,id)
       cache_data = @store[id]
       puts "CacheData is: "
       puts  cache_data
       puts "------ current_location is : "
       puts current_location
       if cache_data
         cache_location = LocationData.location_from_cache(cache_data, id)
         events += cache_location.update_with_new_location_data(current_location)
         puts "cache_location.campus  is: "
         puts cache_location.campus
         location_map = cache_location.to_map
         puts "location_map is: "
         puts location_map
         puts "id is: "
         puts id
         @store[id] = location_map
         puts "Updating client ID[{#{id}] with [{#{location_map}]"
       else
         location_map = current_location.to_map
         @store[id] = location_map
         puts "Creating client ID[{#{id}] with [{#{location_map}]"
       end
       puts "the events are: "
       puts events
       puts "---"
       events.each do |e|
         # enrich |e| with extra info
         e.set(CLIENT,client)
         @dim_to_enrich.each { |d| e.set(d, event.get(d)) if event.get(d) }
         #Prepare Event object to send it to the pipeline
         #enrichmentEvent = LogStash::Event.new
         #e.each {|k,v| enrichmentEvent.set(k,v)}
         #yield enrichmentEvent
         yield e
       end
       event.cancel
     end    
 
  end  # def filter
end    # class Logstash::Filter::Mobility
