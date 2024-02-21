# encoding: utf-8

require "logstash/filters/base"
require "logstash/namespace"
require "json"
require "time"
require "dalli"
require_relative "util/mobility_constant"
require_relative "util/memcached_config"
require_relative "store/store_manager"
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
  # expired_time should be smaller than expired_repetitions_time
  config :expired_time,               :validate => :number, :default => 1200,   :required => false
  config :max_dwell_time,             :validate => :number, :default => 1440,   :required => false
  config :expired_repetitions_time,   :validate => :number, :default => 10080,  :required => false
  config :memcached_server,           :validate => :string, :default => "",     :required => false
  config :clean_store_time,           :validate => :number, :default => 600,    :required => false
  config :number_of_stores,           :validate => :number, :default => 10,     :required => false
  config :update_stores_rate,         :validate => :number,  :default => 60,      :required => false

  public
  def register
    
    ConfigVariables.setConsolidatedTime(@consolidated_time)
    ConfigVariables.setExpiredTime(@expired_time)
    ConfigVariables.setMaxDwellTime(@max_dwell_time)
    ConfigVariables.setExpiredRepetitionsTime(@expired_repetitions_time)

    @dim_to_enrich = [MARKET_UUID, ORGANIZATION_UUID, ZONE_UUID, NAMESPACE_UUID,
                   DEPLOYMENT_UUID, SENSOR_UUID, NAMESPACE, SERVICE_PROVIDER_UUID, 
                   BUILDING_UUID, CAMPUS_UUID, FLOOR_UUID,
                   STATUS, CLIENT_PROFILE, CLIENT_RSSI_NUM]
    @not_empty_dims = [ZONE_UUID, ZONE, BUILDING_UUID, BUILDING, FLOOR, FLOOR_UUID, CAMPUS, CAMPUS_UUID]
    @memcached_server = MemcachedConfig::servers if @memcached_server.empty?
    @memcached = Dalli::Client.new(@memcached_server, {:expires_in => 0, :value_max_bytes => 4000000})

    @store_manager = StoreManager.new(@memcached, @update_stores_rate)
    @last_clean_time = Time.now.to_i
  end

  def time_to_remove_expired_clients?
    Time.now.to_i > (@last_clean_time + @clean_store_time)
  end

  def client_expired?(last_seen)
    Time.now.to_i > (last_seen + (@expired_repetitions_time + 60))
  end
 
  # This function search for expired clients
  # deleting them from the cache and 
  # generate its movements to ouside
  # return Array of events
  def expired_events_from_memcached
    @logger.debug? && @logger.debug("[mobility] removing expired clients at #{Time.now}")
    expired_events  = []

    @number_of_stores.times do |store_id|
      expired_clients = []
      store = @memcached.get("mobility#{store_id}") || {}

      store.each do |client| 
        next unless client[1] && client[1]["campus_uuid"] && client[1]["campus_uuid"]["t_last_seen"]

        client_id             = client[0]

        if client[1]["campus_uuid"]["consolidated"] == "outside"
            # Client was already ouside the campus so we dont need to calculate
            # outside movements, just delete it from the cache
            @logger.debug? && @logger.debug("[mobility] client with id #{client_id} was already outside so we only need to delete it from cache")
            expired_clients.push(client_id)
        else
          client_campus_last_seen = client[1]["campus_uuid"]["t_last_seen"]

          next unless client_expired?(client_campus_last_seen)

          client_mac            = client_id[0..16]
          client_namespace_uuid = client_id[17..-1]
          client_cache_data     = client[1]

          next unless valid_mac_address?(client_mac)


          # Get old client location from the cache 
          old_client_location  = LocationData.location_from_cache(client_cache_data, client_id) 
          @logger.debug? && @logger.debug("[mobility] Old client location was #{old_client_location.to_map}")

          # Create the new client location as he moved to ouside
          new_client_location = LocationData.location_to_outside(client_cache_data, client_id)
          @logger.debug? && @logger.debug("[mobility] new client location is #{new_client_location.to_map}")

          expired_events_client = old_client_location.update_with_new_location_data(new_client_location)
          @logger.debug? && @logger.debug("[mobility] number of generated expired events is #{expired_events_client.count}")

          if expired_events_client.count > 0
            @logger.debug? && @logger.debug("[mobility] Expiring client with id #{client_id}")
            expired_clients.push(client_id)
          else
            @logger.debug? && @logger.debug("[mobility] Could not expire client with id #{client_id} because no movements to outside were generated")
          end

          # Enrich expired events with client_mac and namespace_uuid
          # to use it later to enrich the @dim_to_enrich
          expired_events_client.each do |expired_event_client|
            expired_event_client.set("expiration_track", 1)
            expired_event_client.set(CLIENT, client_mac)
            expired_event_client.set(NAMESPACE_UUID, client_namespace_uuid)

            expired_events.push(expired_event_client)
          end
        end
      end

      # Remove expired clients from the store
      expired_clients.each{ |client_id| store.delete(client_id) if store.key? (client_id) }
      save_store(store_id, store)
    end

    @last_clean_time = Time.now.to_i

    return expired_events
  end

  def save_store(id, store)
      @memcached.set("mobility#{id}", store)
  end

  def valid_mac_address?(mac)
    !!(/^(?:[0-9A-Fa-f]{2}[:-]){5}(?:[0-9A-Fa-f]{2})$/ === mac)
  end

  def find_client_data_from_cache(store_id, client_id)
    stores = @memcached.get_multi("mobility#{store_id}","mobility-historical#{store_id}") || {}

    stores.values.each do |store|
      data = store[client_id]

      return data if data
    end

    return nil
  end

  def hash_mac(mac_address)
    Digest::SHA256.hexdigest(mac_address).to_i(16)
  end
  
  def assign_store(mac_address)
    hash_value = hash_mac(mac_address)
    hash_value % @number_of_stores
  end

  def client_mac_from_event(e)
    e.get(CLIENT).to_s
  end
  def namespace_from_event(e)
    (e.get(NAMESPACE_UUID)) ? e.get(NAMESPACE_UUID) : ""
  end

  def filter(event)
     client_mac = client_mac_from_event(event)
     namespace  = namespace_from_event(event)
     client_id  = client_mac + namespace

     return unless client_mac

     client_events = []
     
     # Get store of the client
     store_id = assign_store(client_mac)
     store = @memcached.get("mobility#{store_id}") || {}

     new_client_location = LocationData.location_from_message(event,client_id)

     client_cache_data = find_client_data_from_cache(store_id, client_id)

     if store.key? (client_id)
       store.delete(client_id)
     end
     
     unless client_cache_data
       @logger.debug? && @logger.debug("[mobility] Creating client ID[{#{client_id}] with [{#{new_client_location.to_map}]")
 
       # Create the client_id in the store with the new location
       store[client_id] = new_client_location.to_map
     else
       @logger.debug? && @logger.debug("[mobility] Updating client ID[{#{client_id}] with [{#{new_client_location.to_map}]")

       old_client_location = LocationData.location_from_cache(client_cache_data, client_id)

       # Update the old location with the new and get the generated events into client_events
       client_events += old_client_location.update_with_new_location_data(new_client_location)

       # Update the client in the store with the updated location
       store[client_id] = old_client_location.to_map 

       # Enrich the generated events with data from the received event
       client_events.each do |client_event|
         client_event.set(CLIENT,client_mac)
         @dim_to_enrich.each { |d| client_event.set(d, event.get(d)) if event.get(d) }
         client_event.to_hash.each { |k,v| client_event.set("discard", true) if @not_empty_dims.include? k and (v.nil? or v == "") }
         yield client_event
       end
     end

     save_store(store_id, store)

     # Also check for expired clients and generated it movements to outside
     expired_events = time_to_remove_expired_clients? ? expired_events_from_memcached : []
     @logger.debug? && @logger.debug("[mobility] expired_events are #{expired_events.count}")

     # Enrich expired_events with @dim_to_enrich and yield
     expired_events.each do |expired_event|

       @logger.debug? && @logger.debug("[mobility] Enriching expired_event: #{expired_event.to_hash}")
       enrich_data = @store_manager.enrich(expired_event.to_hash)
       @logger.debug? && @logger.debug("[mobility] enrich data is: #{enrich_data}")

       @dim_to_enrich.each { |d| expired_event.set(d, enrich_data[d]) if enrich_data[d] }
       expired_event.to_hash.each { |k,v| e.set("discard", true) if @not_empty_dims.include? k and (v.nil? or v == "") }

       yield expired_event
     end

     event.cancel
  end  # def filter
end    # class Logstash::Filter::Mobility
