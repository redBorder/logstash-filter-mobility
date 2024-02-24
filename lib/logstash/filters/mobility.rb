# encoding: utf-8

require 'logstash/filters/base'
require 'logstash/namespace'
require 'json'
require 'time'
require 'dalli'

require_relative 'util/mobility_constant'
require_relative 'util/memcached_config'
require_relative 'util/configuration'
require_relative 'store/store_manager'
require_relative 'mobility/client'
require_relative 'mobility/cache'

module Configuration
  class << self
    attr_accessor :config
  end
end

class LogStash::Filters::Mobility < LogStash::Filters::Base
  include MobilityConstant

  config_name "mobility"

  # config :consolidated_time,          :validate => :number, :default => 180,    :required => false
  config :consolidated_time,          :validate => :number, :default => 10,    :required => false
  # expired_time should be smaller than expired_repetitions_time
  # config :expired_time,               :validate => :number, :default => 1200,   :required => false
  config :expired_time,               :validate => :number, :default => 180,   :required => false
  config :max_dwell_time,             :validate => :number, :default => 1440,   :required => false
  # config :expired_repetitions_time,   :validate => :number, :default => 10080,  :required => false
  config :expired_repetitions_time,   :validate => :number, :default => 180,  :required => false
  config :memcached_server,           :validate => :string, :default => "",     :required => false
  # config :clean_store_time,           :validate => :number, :default => 600,    :required => false
  config :clean_store_time,           :validate => :number, :default => 10,    :required => false
  config :number_of_stores,           :validate => :number, :default => 10,     :required => false
  config :update_stores_rate,         :validate => :number, :default => 60,     :required => false

  public
  def register
    @config.each{ |key, value| Configuration.set_config("#{key}", value) }

    @dimensions_to_enrich = [MARKET_UUID, ORGANIZATION_UUID, ZONE_UUID, NAMESPACE_UUID,
                             DEPLOYMENT_UUID, SENSOR_UUID, NAMESPACE, SERVICE_PROVIDER_UUID, 
                             BUILDING_UUID, CAMPUS_UUID, FLOOR_UUID,
                             STATUS, CLIENT_PROFILE, CLIENT_RSSI_NUM]
    @not_empty_dimensions = [ZONE_UUID, ZONE, BUILDING_UUID, BUILDING, FLOOR, FLOOR_UUID, CAMPUS, CAMPUS_UUID]

    @memcached_server = MemcachedConfig::servers if @memcached_server.empty?
    @memcached = Dalli::Client.new(@memcached_server, {:expires_in => 0, :value_max_bytes => 4000000})

    @store_manager = StoreManager.new(@memcached, @update_stores_rate)
    @last_clean_time = Time.now.to_i - @clean_store_time
  end

  def time_to_clean_cache?
    Time.now.to_i > (@last_clean_time + @clean_store_time)
  end

  def process_client(event, cache)
    events = []

    namespace_uuid = event.get(NAMESPACE_UUID) || ""
    client_mac = event.get(CLIENT).to_s
    
    client = cache.load_client(client_mac, namespace_uuid) || Client.create_from_event(event)
    client_events = client.update_location_from_event(event)

    cache.save_client(client) 

    # Enrich client events
    client_events.each do |client_event|
      client_event.set(CLIENT,client_mac)
      @dimensions_to_enrich.each { |d| client_event.set(d, event.get(d)) if event.get(d) }
      client_event.to_hash.each { |k,v| client_event.set("discard", true) if @not_empty_dimensions.include? k and (v.nil? or v == "") }

      events.push(client_event)
    end
  
    events
  end

  # Check for expired clients and generated its to outside events
  def process_expired_events(cache)
    events = []

    expired_events = cache.clean_expired_clients
    @last_clean_time = Time.now.to_i

    # Enrich expired_events
    expired_events.each do |expired_event|
      enrich_data = @store_manager.enrich(expired_event.to_hash)
      @dimensions_to_enrich.each { |d| expired_event.set(d, enrich_data[d]) if enrich_data[d] }
      expired_event.to_hash.each { |k,v| e.set("discard", true) if @not_empty_dimensions.include? k and (v.nil? or v == "") }

      events.push(expired_event)
    end

    events
  end

  def filter(event)
    if event.get(CLIENT).nil?
      @logger.error("[mobility] No client_mac detected")
      event.cancel
      return
    end 
    
    events = []

    cache = Cache.new(@memcached)

    events += process_client(event, cache)
    events += process_expired_events(cache) if time_to_clean_cache?

    events.each{|e| yield e }

    event.cancel
  end  # def filter
end    # class Logstash::Filter::Mobility
