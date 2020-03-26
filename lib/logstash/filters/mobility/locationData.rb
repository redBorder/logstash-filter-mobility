# encoding: utf-8

require_relative "dimensions"

class LocationData 
  attr_accessor :tGlobalLastSeen, :campus, :building, :floor, :zone

  def initialize(timestamp, campus, building, floor, zone) 
    self.tGlobalLastSeen = timestamp
    self.campus = campus
    self.building = building
    self.floor = floor
    self.zone = zone  
  end

  def updateWithNewLocationData(locationData)


  end

  def toMap

  end

  def locations

  end

  def self.locationFromCache(rawData, uuidPrefix)
    timestamp = rawData.get(TIMESTAMP) ? Time.parse(rawData.get(TIMESTAMP).to_i / 1000) : (Time.now.to_i / 1000)
    latLong = rawData.get(LATLONG).to_s
    builder = LocationData.new
    builder.timestamp = timestamp
    
    campus = rawData.get(CAMPUS).to_s
    builder.withCampus(Campus.new(timestamp, timestamp, timestamp, "outside", campus, "outside", campus, latLong, uuidPrefix))    
    
  end

  def locationFromMessage(rawData, uuidPrefix)

  end

 
end
