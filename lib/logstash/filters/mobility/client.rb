# encoding: utf-8

require "logstash/util/loggable"

require_relative '../util/mobility_constant'
require_relative '../util/utils'
require_relative 'location_data'

class Client
  include LogStash::Util::Loggable
  include MobilityConstant
  
  attr_accessor :client_mac, :namespace_uuid, :location

  def initialize_from_event(event)
    @client_mac = event.get(CLIENT).to_s
    @namespace_uuid = event.get(NAMESPACE_UUID) || ""
    @location = LocationData.create_from_event(event, id)
    logger.info("[mobility] Created client with ID #{id} and location #{location.to_map}")
  end

  def initialize_from_cache(client_mac, namespace_uuid, data)
    @client_mac = client_mac
    @namespace_uuid = namespace_uuid
    @location = LocationData.create_from_cache(data, id)
  end

  def self.create_from_event(event)
    new(event)
  end

  def self.create_from_cache(client_mac, namespace_uuid, data)
    new(client_mac, namespace_uuid, data)
  end

  def initialize(*args)
    if args.count == 1
      initialize_from_event(*args)
    else
      initialize_from_cache(*args)
    end
  end

  def id
    client_mac + namespace_uuid
  end
 
  def update_location(new_location)
    logger.info("[mobility] Updating location for client with ID #{id}, 
                 \nFROM: #{location.to_map} \nTO: #{new_location.to_map}")

    events = @location.update_location!(new_location) 
  
    events
  end

  def update_location_from_event(event)
    new_location = LocationData.create_from_event(event, id)
    update_location(new_location)
  end

  def update_location_to_outside
    new_location = LocationData.create_from_data_to_outside(location.to_map, id)
    update_location(new_location)
  end
end
