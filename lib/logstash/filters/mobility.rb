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
  include MobilityConstant

  config_name "mobility"

  config :consolidated_time,          :validate => :number, :default => 180,    :required => false
  config :expired_time,               :validate => :number, :default => 1200,   :required => false
  config :max_dwell_time,             :validate => :number, :default => 1440,   :required => false
  config :expired_repetitions_time,   :validate => :number, :default => 10080,  :required => false
  config :memcached_server,           :validate => :string, :default => "",     :required => false
  config :clean_store_time,           :validate => :number, :default => 600,    :required => false
  config :number_of_stores,           :validate => :number, :default => 10,     :required => false
  
  public
  def register
    
    ConfigVariables.setConsolidatedTime(@consolidated_time)
    ConfigVariables.setExpiredTime(@expired_time)
    ConfigVariables.setMaxDwellTime(@max_dwell_time)
    ConfigVariables.setExpiredRepetitionsTime(@expired_repetitions_time)

    @stores = {}
    @dim_to_enrich = [MARKET_UUID, ORGANIZATION_UUID, ZONE_UUID, NAMESPACE_UUID,
                   DEPLOYMENT_UUID, SENSOR_UUID, NAMESPACE, SERVICE_PROVIDER_UUID, 
                   BUILDING_UUID, CAMPUS_UUID, FLOOR_UUID,
                   STATUS, CLIENT_PROFILE, CLIENT_RSSI_NUM]
    @not_empty_dims = [ZONE_UUID, ZONE, BUILDING_UUID, BUILDING, FLOOR, FLOOR_UUID, CAMPUS, CAMPUS_UUID]
    @memcached_server = MemcachedConfig::servers if @memcached_server.empty?
    @memcached = Dalli::Client.new(@memcached_server, {:expires_in => 0, :value_max_bytes => 4000000})
    @last_clean_time = Time.now.to_i
  end

  # clean the store based on last_seen
  def clean_stores(number_of_stores)
    events = []
    if Time.now.to_i > (@last_clean_time + @clean_store_time)
      puts "cleaning mobility stores....#{Time.now}"
      for store_id in 0 .. number_of_stores-1
        @store = stores_from_memcache(store_id)["mobility#{store_id}"] || {}
        ids_to_delete = []
        @store.each do |client| 
          next unless client[1] && client[1]["campus_uuid"] && client[1]["campus_uuid"]["t_last_seen"]

          if Time.now.to_i > (client[1]["campus_uuid"]["t_last_seen"] + (@expired_time + 60))
            puts "cleaning client with id #{client[0]}"
            id = client[0]
            ids_to_delete.push(id)
            # Make events to "outside" with Campus, Building, Floor and Zone
            cache_location = LocationData.location_from_cache(client[1], id)
            outside_location = LocationData.location_to_outside(client[1], id, @clean_store_time)
            events += cache_location.update_with_new_location_data(outside_location) 
          end
        end
        ids_to_delete.each{ |id| @store.delete(id) if @store.key? (id) }
        save_store(store_id)
      end
      @last_clean_time = Time.now.to_i
    end
    return events
  end

  def save_store(id)
      @memcached.set("mobility#{id}",@store)
  end

  # Get all the stores of mobility
  # and store it on @stores
  def stores_from_memcache(store_id)
    @stores = @memcached.get_multi("mobility#{store_id}","mobility-historical#{store_id}") || {}
  end

  def find_data_from_stores(key)
    data = nil
    @stores.values.each do |store|
      data = store[key]
      break if data
    end
    data
  end

  def hash_mac(mac_address)
    Digest::SHA256.hexdigest(mac_address).to_i(16)
  end
  
  def assign_store(mac_address, number_of_stores)
    hash_value = hash_mac(mac_address)
    hash_value % number_of_stores
  end

  def filter(event)
     client = event.get(CLIENT).to_s
     namespace = (event.get(NAMESPACE_UUID)) ? event.get(NAMESPACE_UUID) : ""
     id = client + namespace
     if client
       events = clean_stores(@number_of_stores)
       store_id = assign_store(client, @number_of_stores)
       @store = stores_from_memcache(store_id)["mobility#{store_id}"] || {}
       current_location = LocationData.location_from_message(event,id)
       cache_data = find_data_from_stores(id)
       if @store.key? (id)
         @store.delete(id)
       end
       if cache_data
         cache_location = LocationData.location_from_cache(cache_data, id)
         events += cache_location.update_with_new_location_data(current_location)
         location_map = cache_location.to_map
         @store[id] = location_map 
         @logger.debug? && @logger.debug("Updating client ID[{#{id}] with [{#{location_map}]")
       else
         location_map = current_location.to_map
         @store[id] = location_map
         @logger.debug? && @logger.debug("Creating client ID[{#{id}] with [{#{location_map}]")
       end

       events.each do |e|
         e.set(CLIENT,client)
         @dim_to_enrich.each { |d| e.set(d, event.get(d)) if event.get(d) }
         e.to_hash.each { |k,v| e.set("discard", true) if @not_empty_dims.include? k and (v.nil? or v == "") }
         yield e
       end
       event.cancel
       save_store(store_id)
     end    
 
  end  # def filter
end    # class Logstash::Filter::Mobility
