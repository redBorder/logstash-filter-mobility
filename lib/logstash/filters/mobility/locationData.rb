# encoding: utf-8

require_relative "../utils/dimensions"
require_relative "../utils/utils"
require_relative "building"
require_relative "campus"
require_relative "floor"
require_relative "zone"

class LocationData 
  attr_accessor :tGlobalLastSeen, :campus, :building, :floor, :zone

  def initialize(timestamp = nil, campus = nil, building = nil, floor = nil, zone = nil) 
    self.tGlobalLastSeen = timestamp
    self.campus = campus
    self.building = building
    self.floor = floor
    self.zone = zone  
  end

  def updateWithNewLocationData(locationData)
    toSend = [] 
    tGlobalLastSeen = locationData.tGlobalLastSeen

    if campus and locationData.campus
      puts "im in updateWithNewLocationData and campus is: "
      puts campus
      toSend += campus.updateWithNewLocation(locationData.campus, "campus")
    elsif locationData.campus
        puts "im in updateWihtNewLocationData campus was nill but locationData.campus is: "
        puts locationData.campus
	self.campus = locationData.campus
    end

    if building and locationData.building
        toSend += building.updateWithNewLocation(locationData.building, "building")
    elsif locationData.building
        self.building = locationData.building
    end
    
    if floor and locationData.floor
        toSend += floor.updateWithNewLocation(locationData.floor, "floor")
    elsif locationData.floor
        self.floor = locationData.floor
    end
    
    if zone and locationData.zone
        toSend += zone.updateWithNewLocation(locationData.zone, "zone")
    elsif locationData.zone
        self.zone = locationData.zone
    end
    puts "in updateWithNewLocationData toSend is: "
    puts toSend
    return toSend
  end

  def toMap
    map = Hash.new
    map[T_GLOBAL_LAST_SEEN] = tGlobalLastSeen
    map[CAMPUS] = campus.toMap() if campus
    map[BUILDING] = building.toMap() if building
    map[FLOOR] = floor.toMap() if floor
    map[ZONE] = zone.toMap() if zone
    
    return map
  end

  def locations
    locations = Array.new
    locations.push(campus) if campus
    locations.push(building) if building
    locations.push(floor) if floor
    locations.push(zone) if zone
    
    return locations
  end

  def self.locationFromCache(rawData, uuidPrefix)
    puts "Im in locationFromCache"
    builder = LocationData.new
    builder.timestamp = Utils.timestamp2Long(rawData[T_GLOBAL_LAST_SEEN])
   
    campusData = rawData[CAMPUS]
    builder.campus = Campus.new(campusData,uuidPrefix) if campusData

    buildingData = rawData[BUILDING]
    builder.building = Campus.new(buildingData,uuidPrefix) if buildingData

    floorData = rawData[FLOOR]
    builder.floor = Floor.new(floorData,uuidPrefix) if floorData

    zoneData = rawData[ZONE]
    builder.zone = Campus.new(zoneData,uuidPrefix) if zoneData

    puts "builder in locationFromCache is: "
    puts builder    
    
    return builder
  end

  def timestamp=(timestamp)
    self.tGlobalLastSeen = timestamp
  end

  def self.locationFromMessage(rawData, uuidPrefix)
    puts "Im in locationFromMessage"
    timestamp = Utils.timestamp2Long(rawData.get(TIMESTAMP))
    latLong = rawData.get(LATLONG).to_s
    builder = LocationData.new
    builder.timestamp = timestamp
    
    campus = rawData.get(CAMPUS).to_s
    builder.campus = Campus.new(timestamp, timestamp, timestamp, "outside", campus, "outside", campus, latLong, uuidPrefix) if campus
    
    building = rawData.get(BUILDING).to_s
    builder.building = Building.new(timestamp, timestamp, timestamp, "outside", building, "outside", building, latLong, uuidPrefix) if building

    floor = rawData.get(FLOOR).to_s
    builder.floor = Floor.new(timestamp, timestamp, timestamp, "outside", floor, "outside", floor, latLong, uuidPrefix) if floor

    zone = rawData.get(ZONE).to_s
    builder.zone = Floor.new(timestamp, timestamp, timestamp, "outside", zone, "outside", zone, latLong, uuidPrefix) if zone

    return builder
  end

 
end
