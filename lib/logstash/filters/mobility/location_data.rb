# encoding: utf-8

require_relative "../utils/dimensions"
require_relative "../utils/utils"
require_relative "building"
require_relative "campus"
require_relative "floor"
require_relative "zone"

class LocationData 
  attr_accessor :t_global_last_seen, :campus, :building, :floor, :zone

  def initialize(timestamp = nil, campus = nil, building = nil, floor = nil, zone = nil) 
    @t_global_last_seen = timestamp
    @campus = campus
    @building = building
    @floor = floor
    @zone = zone  
  end

  def update_with_new_location_data(location_data)
    to_send = [] 
    @t_global_last_seen = location_data.t_global_last_seen

    if @campus && location_data.campus
      puts "im in update_with_new_location_data and campus is: "
      puts @campus
      to_send += @campus.update_with_new_location(location_data.campus, "campus")
    elsif location_data.campus
        puts "im in updateWihtNewLocationData campus was nill but location_data.campus is: "
        puts location_data.campus
	@campus = location_data.campus
    end

    if @building && location_data.building
        to_send += building.update_with_new_location(location_data.building, "building")
    elsif location_data.building
        @building = location_data.building
    end
    
    if @floor && location_data.floor
        to_send += @floor.update_with_new_location(location_data.floor, "floor")
    elsif location_data.floor
        @floor = location_data.floor
    end
    
    if @zone && location_data.zone
        to_send += @zone.update_with_new_location(location_data.zone, "zone")
    elsif location_data.zone
        @zone = location_data.zone
    end
    puts "in update_with_new_location_data to_send is: "
    puts to_send
    return to_send
  end

  def to_map
    map = Hash.new
    map[T_GLOBAL_LAST_SEEN] = @t_global_last_seen
    map[CAMPUS] = @campus.to_map if @campus
    map[BUILDING] = @building.to_map if @building
    map[FLOOR] = @floor.to_map if @floor
    map[ZONE] = @zone.to_map if @zone
    
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

  def self.location_from_cache(raw_data, uuid_prefix)
    puts "Im in location_from_cache"
    builder = LocationData.new
    puts "!!!!!!!!!!! raw_data[T_GLOBAL_LAST_SEEN] es: " 
    puts raw_data[T_GLOBAL_LAST_SEEN]
    builder.timestamp = Utils.timestamp_to_long(raw_data[T_GLOBAL_LAST_SEEN])
   
    campus_data = raw_data[CAMPUS]
    builder.campus = Campus.new(campus_data,uuid_prefix) if campus_data

    building_data = raw_data[BUILDING]
    builder.building = Campus.new(building_data,uuid_prefix) if building_data

    floor_data = raw_data[FLOOR]
    builder.floor = Floor.new(floor_data,uuid_prefix) if floor_data

    zone_data = raw_data[ZONE]
    builder.zone = Campus.new(zone_data,uuid_prefix) if zone_data

    puts "builder in location_from_cache is: "
    puts builder    
    
    return builder
  end

  def timestamp=(timestamp)
    @t_global_last_seen = timestamp
  end

  def self.location_from_message(raw_data, uuid_prefix)
    puts "Im in location_from_message"
    puts "!!!!!!!!!!! raw_data.get[TIMESTAMP] es: " 
    puts raw_data.get(TIMESTAMP)
    timestamp = Utils.timestamp_to_long(raw_data.get(TIMESTAMP))
    lat_long = raw_data.get(LATLONG).to_s
    builder = LocationData.new
    builder.timestamp = timestamp
    
    campus = raw_data.get(CAMPUS).to_s
    builder.campus = Campus.new(timestamp, timestamp, timestamp, "outside", campus, "outside", campus, lat_long, uuid_prefix) if campus
    
    building = raw_data.get(BUILDING).to_s
    builder.building = Building.new(timestamp, timestamp, timestamp, "outside", building, "outside", building, lat_long, uuid_prefix) if building

    floor = raw_data.get(FLOOR).to_s
    builder.floor = Floor.new(timestamp, timestamp, timestamp, "outside", floor, "outside", floor, lat_long, uuid_prefix) if floor

    zone = raw_data.get(ZONE).to_s
    builder.zone = Floor.new(timestamp, timestamp, timestamp, "outside", zone, "outside", zone, lat_long, uuid_prefix) if zone

    return builder
  end

 
end
