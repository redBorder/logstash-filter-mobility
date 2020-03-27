# encoding: utf-8

require_relative "../utils/dimensions"
require_relative "../utils/utils"

class Location
  attr_accessor :tGlobal, :tLastSeen, :tTransition, :dWellTime, :oldLoc, :newLoc, 
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
    self.tGlobal = Utils.timestamp2Long(rawLocation[T_GLOBAL]) 
    self.tLastSeen = Utils.timestamp2Long(rawLocation[T_LAST_SEEN]) 
    self.tTransition = Utils.timestamp2Long(rawLocation[T_TRANSITION])
    self.dWellTime = rawLocation[DWELL_TIME].to_i
    self.oldLoc = rawLocation[OLD_LOC].to_s
    self.newLoc = rawLocation[NEW_LOC].to_s
    self.consolidated = rawLocation[CONSOLIDATED].to_s
    self.entrance = rawLocation[ENTRANCE].to_s
    self.latLong = rawLocation[LATLONG].to_s
    self.uuid = rawLocation[UUID].to_i # TODO: check if this integer is a long?
    self.uuidPrefix = uuidPrefix
    self.repeatLocations = rawLocation[REPEAT_LOCATION] # TODO: check if this is a Map or not

    repeatLocations = Hash.new if repeatLocations.nil?
  end
  def initialize(*args)
    if args.count == 9
      initialize_all(args[0],args[1],args[2],args[3],args[4],args[5],args[6],args[7],args[8])
    elsif
      initialize_location(args[0],args[2])
    end
  end


  def updateWithNewLocation(location, locationType) 
    toSend = Array.new
    newRepetitions = repeatLocations[location.newLoc] || 0
    oldRepetitions = repeatLocations[newLoc] || 0
    puts "expiredRepetitionsTime is: "
    puts ConfigVariables.expiredRepetitionsTime 
    ((location.tLastSeen - tLastSeen) > ConfigVariables.expiredRepetitionsTime) and newRepetitions = 0 and oldRepetitions = 0

    if (location.tLastSeen - tLastSeen >= ConfigVariables.expiredTime) 
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
      if (consolidated.eql?(location.newLoc))
        if (!isTheSameMinute(tLastSeen, location.tLastSeen))
          for t in ((tLastSeen + MINUTE)..location.tLastSeen).step(MINUTE) do
            if (dWellTime <= SamzaLocationTask.maxDwellTime)
              e =  LogStash::Event.new
              e.set(TIMESTAMP, t)
              e.set(OLD_LOC, location.newLoc)
              e.set(NEW_LOC, location.newLoc)
              e.set(TRANSITION, 0)
              e.set(REPETITIONS, newRepetitions)
              e.set(POPULARITY, popularity)
              e.set(DWELL_TIME, dWellTime)
              e.set(SESSION, "#{uuidPrefix}-#{uuid}")
              e.set(locationWithUuid(locationType), location.newLoc)
              e.set(TYPE, locationType.type)
              e.set(LATLONG, location.latLong) if location.latLong

              toSend.push(e)
            end
            dWellTime += 1
          end
        end
        puts "Consolidated state, sending [{#{toSend.size()}}] events"
        tLastSeen = location.tLastSeen
      else
        if (location.tLastSeen - tLastSeen >= ConfigVariables.consolidatedTime)
          #repeatLocations[location.newLoc]
          if consolidated.eql?"outside"
            e =  LogStash::Event.new
            e.set(TIMESTAMP, tGlobal)
            e.set(OLD_LOC, consolidated)
            e.set(NEW_LOC, entrance)
            e.set(SESSION, "#{uuidPrefix}-#{uuid}")
            e.set(locationWithUuid(locationType), entrance)
            e.set(TRANSITION, 1)
            e.set(REPETITIONS, 0)
            e.set(POPULARITY, popularity)
            e.set(DWELL_TIME, 1)
            e.set(TYPE, locationType.type)
            e.set(LATLONG, location.latLong) if location.latLong
            
            toSend.push(e)
            consolidated = entrance
            tTransition += MINUTE
          else
            # // Last Consolidated location
            for t in (tGlobal + MINUTE)..(tTransition - MINUTE).step(MINUTE)
              break if !isTheSameMinute(t, (tTransition - MINUTE))
              e = LogStash::Event.new
              e.set(TIMESTAMP, t)
              e.set(OLD_LOC, consolidated)
              e.set(NEW_LOC, consolidated)
              e.set(SESSION, "#{uuidPrefix}-#{uuid}")
              e.set(DWELL_TIME, dWellTime)
              e.set(locationWithUuid(locationType), consolidated)
              e.set(TRANSITION, 0)
              e.set(REPETITIONS, oldRepetitions)
              e.set(POPULARITY, popularity)
              e.set(TYPE, locationType.type)
              e.set(LATLONG, location.latLong) if location.latLong

              toSend.push(e)
              dWellTime += 1
            end
            # // Increasing the session uuid because this is new session
            uuid += 1
            popularity = (((newRepetitions + 1) / (Float(uuid) + 1)*100.0).to_i/100.0)
          end

          if (isTheSameMinute(tTransition, tGlobal))
            tTransition += MINUTE
            tLastSeen += MINUTE
          end
          dWellTime = 1
          # // Transition
          for t in tTransition..tLastSeen.step(MINUTE)
            e = LogStash::Event.new
            e.set(TIMESTAMP, t)
            e.set(OLD_LOC, consolidated)
            e.set(NEW_LOC, location.newLoc)
            e.set(SESSION, "#{uuidPrefix}-#{uuid}")
            e.set(locationWithUuid(locationType), location.newLoc)
            e.set(DWELL_TIME, dWellTime)
            e.set(TRANSITION, 1)
            e.set(REPETITIONS, 0)
            e.set(POPULARITY, popularity)
            e.set(TYPE, locationType.type)
            e.set(LATLONG, location.latLong) if location.latLong

            toSend.push(e)
            dWellTime += 1
          end
          dWellTime = 1
          for t in (tLastSeen + MINUTE)..location.tLastSeen.step(MINUTE)
            e = LogStash::Event.new
            e.set(TIMESTAMP, t)
            e.set(OLD_LOC, location.newLoc)
            e.set(NEW_LOC, location.newLoc)
            e.set(SESSION, "#{uuidPrefix}-#{uuid}")
            e.set(locationWithUuid(locationType), location.newLoc)
            e.set(DWELL_TIME, dWellTime)
            e.set(TRANSITION, 0)
            e.set(REPETITIONS, newRepetitions)
            e.set(POPULARITY, popularity)
            e.set(TYPE, locationType.type)
            e.set(LATLONG, location.latLong) if location.latLong
            toSend.push(e)
            dWellTime += 1
          end
          puts "Consolidating state, sending [{#{toSend.count}}] events"
          newRepetitions += 1
          repeatLocations[location.newLoc] = newRepetitions
          tGlobal = location.tLastSeen
          tLastSeen = location.tLastSeen
          tTransition = location.tTransition
          oldLoc = location.newLoc
          newLoc = location.newLoc
          consolidated = location.newLoc
          latLong = location.latLong
        else
          puts "Trying to consolidate state, but {location.tLastSeen[#{location.tLastSeen.to_s}] - tLastSeen[#{tLastSeen.to_s}] < consolidatedTime[#{ConfigVariables.consolidatedTime.to_s}"
        end

      end
    else
      puts "Moving from [{#{newLoc}}] to [{#{location.newLoc}}]"
      tLastSeen = location.tLastSeen
      oldLoc = newLoc
      newLoc = location.newLoc
      # Leaving consolidated location.
      tTransition = location.tLastSeen if oldLoc.eql?(consolidated)
    end
    return toSend
  end

  def toMap
    map = Hash.new
    map[T_GLOBAL] = tGlobal
    map[T_LAST_SEEN] =  tLastSeen
    map[T_TRANSITION] = tTransition
    map[DWELL_TIME] = dWellTime
    map[OLD_LOC] =  oldLoc
    map[UUID] = uuid
    map[NEW_LOC] = newLoc
    map[CONSOLIDATED] = consolidated
    map[ENTRANCE] = entrance
    map[REPEAT_LOCATION] = repeatLocations
    
    map[LATLONG] = latLong if latLong

    return map
  end

  def isTheSameMinute(time1, time2)
        return (time1 - time1 % MINUTE) == (time2 - time2 % MINUTE);
  end

end


