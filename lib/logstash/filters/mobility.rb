# encoding: utf-8

require "logstash/filters/base"
require "logstash/namespace"
require "json"
require "time"
require "dalli"
require_relative "utils/mobility_constant"
require_relative "utils/memcached_config"
require_relative "mobility/location_data"

module ConfigVariables
  attr_accessor :consolidated_time, :expired_time,:max_dwell_time,:expired_repetitions_time, :max_keys_mobility_to_clean
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

  def self.setMaxKeysMobilityToClean(value)
    @@max_keys_mobility_to_clean = value
  end

  def self.max_keys_mobility_to_clean
    @@max_keys_mobility_to_clean
  end

end

class LogStash::Filters::Mobility < LogStash::Filters::Base
  include MobilityConstant

  config_name "mobility"

  config :consolidated_time,          :validate => :number, :default => 180,   :required => false
  config :expired_time,               :validate => :number, :default => 1200,  :required => false
  config :max_dwell_time,             :validate => :number, :default => 1440,  :required => false
  config :expired_repetitions_time,   :validate => :number, :default => 10080, :required => false
  config :memcached_server,           :validate => :string, :default => "",    :required => false
  config :max_keys_mobility_to_clean, :validate => :number, :default => 300,   :required => false
  
  public
  def register
    
    ConfigVariables.setConsolidatedTime(@consolidated_time)
    ConfigVariables.setExpiredTime(@expired_time)
    ConfigVariables.setMaxDwellTime(@max_dwell_time)
    ConfigVariables.setExpiredRepetitionsTime(@expired_repetitions_time)
    ConfigVariables.setMaxKeysMobilityToClean(@max_keys_mobility_to_clean)
    @stores = {}
    @dim_to_enrich = [MARKET_UUID, ORGANIZATION_UUID, ZONE_UUID, NAMESPACE_UUID,
                   DEPLOYMENT_UUID, SENSOR_UUID, NAMESPACE, SERVICE_PROVIDER_UUID, 
                   BUILDING_UUID, CAMPUS_UUID, FLOOR_UUID,
                   STATUS, CLIENT_PROFILE, CLIENT_RSSI_NUM]
    @not_empty_dims = [ZONE_UUID, ZONE, BUILDING_UUID, BUILDING, FLOOR, FLOOR_UUID, CAMPUS, CAMPUS_UUID]
    @memcached_server = MemcachedConfig::servers if @memcached_server.empty?
    @memcached = Dalli::Client.new(@memcached_server, {:expires_in => 0, :value_max_bytes => 4000000})
  end

  def save_store
      @memcached.set("mobility",@store)
  end

  # Get all the stores of mobility
  # and store it on @stores
  def stores_from_memcache
    @stores = @memcached.get_multi("mobility","mobility-historical") || {}
  end

  def find_data_from_stores(key)
    data = nil
    @stores.values.each do |store|
      data = store[key]
      break if data
    end
    data
  end

  def filter(event)
     client = event.get(CLIENT).to_s
     namespace = (event.get(NAMESPACE_UUID)) ? event.get(NAMESPACE_UUID) : ""
     id = client + namespace
     if client
       @store = stores_from_memcache["mobility"] || {}
       events = []
       current_location = LocationData.location_from_message(event,id)
       cache_data = find_data_from_stores(id)
       @store.delete(id) if @store.key? (id)
       if cache_data
         cache_location = LocationData.location_from_cache(cache_data, id)
         events += cache_location.update_with_new_location_data(current_location)
         location_map = cache_location.to_map
         @store[id] = location_map 
         @logger.debug? && @logger.debug("Updating client ID[{#{id}] with [{#{location_map}]")
         #puts "updating.."
       else
         location_map = current_location.to_map
         @store[id] = location_map
         @logger.debug? && @logger.debug("Creating client ID[{#{id}] with [{#{location_map}]")
         #puts "creating.."
       end
       @store = @store.map{|h| h}[(-@max_keys_mobility_to_clean+50)..-1].to_h if @store.keys.count > @max_keys_mobility_to_clean
       events.each do |e|
         e.set(CLIENT,client)
         @dim_to_enrich.each { |d| e.set(d, event.get(d)) if event.get(d) }
         e.to_hash.each { |k,v| e.set("discard", true) if @not_empty_dims.include? k and (v.nil? or v == "") }
         yield e
       end
       event.cancel
       save_store
     end    
 
  end  # def filter
end    # class Logstash::Filter::Mobility
