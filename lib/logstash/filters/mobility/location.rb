# encoding: utf-8

require_relative "dimensions"

class Location
  attr_accessor :tGlobal, :tLastSee, :tTransition, :dWellTime, :oldLoc, :newLoc, 
                :consolidated, :entrance, :latLong, :uuidPrefix, :uuid, :repeatLocations
 
  def initialize_all(tGlobal, tLastSeen, tTransition, oldLoc, newLoc, consolidated, entrance, latLong, uuidPrefix)
    self.tGlobal = tGlobal - tGlobal % 60
    self.tLastSeen = tLastSeen - tLastSeen % 60
    self.tTransition = tTransition - tTransition % 60
    self.oldLoc = oldLoc
    self.newLoc = newLoc
    self.consolidated = consolidated
    self.entrance = entrance
    self.dWellTime = 1
    self.latLong = latLong
    self.uuidPrefix = uuidPrefix
    self.uuid = 0
    self.repeatLocations = Hash.new
  end 

  def initialize_location(rawLocation, uuidPrefix)
    self.tGlobal = rawLocation.get(T_GLOBAL) ? Time.parse(rawLocation.get(T_GLOBAL).to_i / 1000) : (Time.now.to_i / 1000)
    self.tLastSeen = rawLocation.get(T_LAST_SEEN) ? Time.parse(rawLocation.get(T_LAST_SEEN).to_i / 1000) : (Time.now.to_i / 1000)
    self.tTransition = rawLocation.get(T_TRANSITION) ? Time.parse(rawLocation.get(T_TRANSITION).to_i / 1000) : (Time.now.to_i / 1000)
    self.dWellTime = rawLocation.get(DWELL_TIME).to_i
    self.oldLoc = rawLocation.get(OLD_LOC).to_s
    self.newLoc = rawLocation.get(NEW_LOC).to_s
    self.consolidated = rawLocation.get(CONSOLIDATED).to_s
    self.entrance = rawLocation.get(ENTRANCE).to_s
    self.latLong = rawLocation.get(LATLONG).to_s
    self.uuid = rawLocation.get(UUID).to_i # TODO: check if this integer is a long?
    self.uuidPrefix = uuidPrefix
    self.repeatLocations = rawLocation.get(REPEAT_LOCATION) # TODO: check if this is a Map or not

    if (repeatLocations == null) 
        repeatLocations = Hash.new
    end
  end
  def initialize(*args)
    if args.count == 9
      initialize_all(args[0],args[1],args[2],args[3],args[4],args[5],args[6],args[7],args[8])
    elsif
      initalize_location(args[0],args[2])
    end
  end


  def updateWithNewLocation(location, locationType) 
    toSend = Array.new
    newRepetitions = repeatLocations[location.newLoc] || 0
    oldRepetitions = repeatLocations[newLoc] || 0
   
    ((location.tLastSeen - tLastSeen) > @expiredRepetitionsTime) and newRepetitions = 0 and oldRepetitions = 0

    if (location.tLastSeen - tLastSeen >= @expiredTime) 
      e = LogStash::Event.new
      e.set(TIMESTAMP, tLastSeen + MINUTE)
      e.set(OLD_LOC, newLoc)
      e.set(NEW_LOC, "outside")
      e.set(DWELL_TIME, dWellTime)
      e.set(TRANSITION, 1)
      e.set(REPETITIONS, oldRepetitions)
      e.set(POPULARITY, popularity)
      e.set(SESSION, "#{uuidPrefix}-#{uuid}")
      e.set(locationWithUuid(locationType), newLoc)
      e.set(TYPE, locationType.type) 
      toSend.push(e)

      tGlobal = location.tGlobal
      tLastSeen = location.tLastSeen
      tTransition = location.tTransition
      oldLoc = location.oldLoc
      newLoc = location.newLoc
      consolidated = location.consolidated
      entrance = location.entrance
      dWellTime = location.dWellTime
      latLong = location.latLong
      uuid += 1  
    end

    if newLoc.eql?location.newLoc
    
    else
      puts "Moving from [{#{newLoc}}] to [{#{location.newLoc}}]"
      tLastSeen = location.tLastSeen
      oldLoc = newLoc
      newLoc = location.newLoc
      # Leaving consolidated location.
      tTransition = location.tLastSeen if oldLoc.equals(consolidated)
    end
  end

end


