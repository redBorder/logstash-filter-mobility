# encoding: utf-8
require 'logstash/util/loggable'

require_relative '../util/mobility_constant'
require_relative '../util/utils'
require_relative 'location'

class LocationData
  include LogStash::Util::Loggable
  include MobilityConstant
 
  attr_accessor :t_global_last_seen, :campus, :building, :floor, :zone, :wireless_station

  def initialize(timestamp = nil, campus = nil, building = nil, floor = nil, zone = nil, wireless_station = nil) 
    @t_global_last_seen = timestamp
    @campus = campus
    @building = building
    @floor = floor
    @zone = zone 
    @wireless_station = wireless_station 
  end

  def update_location!(new_location)
    @t_global_last_seen = new_location.t_global_last_seen

    events = [] 
    locations = {campus: @campus, building: @building, floor: @floor, zone: @zone}

    locations.each do |type, location|
      if location && new_location.send(type)
        events += location.update_location!(new_location.send(type), type.to_s)
      elsif new_location.send(type)
        instance_variable_set("@#{type}", new_location.send(type))
      end
    end

    @wireless_station = new_location.wireless_station 

    # Enrich all events with wireles_station
    events.each do |event|
      event.set(WIRELESS_STATION, @wireless_station)
    end

    return events
  end

  def to_map
    map = Hash.new
    map[T_GLOBAL_LAST_SEEN] = @t_global_last_seen
    map[WIRELESS_STATION] = @wireless_station if @wireless_station 
    map[CAMPUS_UUID] = @campus.to_map if @campus
    map[BUILDING_UUID] = @building.to_map if @building
    map[FLOOR_UUID] = @floor.to_map if @floor
    map[ZONE_UUID] = @zone.to_map if @zone
    return map
  end

  def locations
    locations = Array.new
    locations.push(@campus) if @campus
    locations.push(@building) if @building
    locations.push(@floor) if @floor
    locations.push(@zone) if @zone
    
    return locations
  end

  def self.create_from_cache(data, uuid_prefix)
    t_global_last_seen = Utils.timestamp_to_long(data[T_GLOBAL_LAST_SEEN])

    campus = create_location_from_data(data[CAMPUS_UUID], uuid_prefix)
    building = create_location_from_data(data[BUILDING_UUID], uuid_prefix)
    floor = create_location_from_data(data[FLOOR_UUID], uuid_prefix)
    zone = create_location_from_data(data[ZONE_UUID], uuid_prefix)

    wireless_station = data[WIRELESS_STATION]

    new(t_global_last_seen, campus, building, floor, zone, wireless_station)
  end

  # Used when cleaning clients from memcached
  def self.create_from_data_to_outside(data,uuid_prefix)
    t_global_last_seen = Time.now.to_i

    campus = create_location_from_data_to_outside(data[CAMPUS_UUID], uuid_prefix, t_global_last_seen)
    building = create_location_from_data_to_outside(data[BUILDING_UUID], uuid_prefix, t_global_last_seen)
    floor = create_location_from_data_to_outside(data[FLOOR_UUID], uuid_prefix, t_global_last_seen)
    zone = create_location_from_data_to_outside(data[ZONE_UUID], uuid_prefix, t_global_last_seen)

    wireless_station = data[WIRELESS_STATION]

    new(t_global_last_seen, campus, building, floor, zone, wireless_station)
  end

  def self.create_from_event(event, uuid_prefix)
    t_global_last_seen = Utils.timestamp_to_long(event.get(TIMESTAMP))
    lat_long = event.get(LATLONG).to_s
    old_loc = consolidated = "outside"

    campus = create_location_from_event(event, uuid_prefix, CAMPUS_UUID, t_global_last_seen, lat_long, old_loc, consolidated)
    building = create_location_from_event(event, uuid_prefix, BUILDING_UUID, t_global_last_seen, lat_long, old_loc, consolidated)
    floor = create_location_from_event(event, uuid_prefix, FLOOR_UUID, t_global_last_seen, lat_long, old_loc, consolidated)
    zone = create_location_from_event(event, uuid_prefix, ZONE_UUID, t_global_last_seen, lat_long, old_loc, consolidated)

    wireless_station = event.get(WIRELESS_STATION)

    new(t_global_last_seen, campus, building, floor, zone, wireless_station)
  end

  private

  def self.create_location_from_data(data, uuid_prefix)
    Location.create_from_data(data, uuid_prefix) if data
  end

  def self.create_location_from_data_to_outside(data, uuid_prefix, timestamp)
    if data
      data[NEW_LOC] = "outside"
      data[ENTRANCE] = "outside"
      data[CONSOLIDATED] = "outside"
      data[T_GLOBAL] = timestamp
      data[T_LAST_SEEN] = timestamp
      Location.create_from_data(data, uuid_prefix)
    end
  end
 
  def self.create_location_from_event(event, uuid_prefix, key, timestamp, lat_long, old_loc, consolidated)
    new_loc = entrance = event.get(key).to_s
    return nil if new_loc.empty?
  
    Location.create_from_params(timestamp, timestamp, timestamp, old_loc, new_loc, consolidated, entrance, lat_long, uuid_prefix)
  end
end
