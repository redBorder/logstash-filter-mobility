# encoding: utf-8

require "logstash/util/loggable"

require_relative "../utils/dimensions"
require_relative "../utils/utils"

class Location
  attr_accessor :t_global, :t_last_seen, :t_transition, :dwell_time, :old_loc, :new_loc, 
                :consolidated, :entrance, :lat_long, :uuid_prefix, :uuid, :repeat_locations

  include LogStash::Util::Loggable

  def initialize_all(t_global, t_last_seen, t_transition, old_loc, new_loc, consolidated, entrance, lat_long, uuid_prefix)
    @t_global = t_global - t_global % 60
    @t_last_seen = t_last_seen - t_last_seen % 60
    @t_transition = t_transition - t_transition % 60
    @old_loc = old_loc
    @new_loc = new_loc
    @consolidated = consolidated
    @entrance = entrance
    @dwell_time = 1
    @lat_long = lat_long
    @uuid_prefix = uuid_prefix
    @uuid = 0
    @repeat_locations = Hash.new
  end 

  def initialize_location(raw_location, uuid_prefix)
    @t_global = Utils.timestamp_to_long(raw_location[T_GLOBAL]) 
    @t_last_seen = Utils.timestamp_to_long(raw_location[T_LAST_SEEN]) 
    @t_transition = Utils.timestamp_to_long(raw_location[T_TRANSITION])
    @dwell_time = raw_location[DWELL_TIME].to_i
    @old_loc = raw_location[OLD_LOC].to_s
    @new_loc = raw_location[NEW_LOC].to_s
    @consolidated = raw_location[CONSOLIDATED].to_s
    @entrance = raw_location[ENTRANCE].to_s
    @lat_long = raw_location[LATLONG].to_s
    @uuid = raw_location[UUID].to_i # TODO: check if this integer is a long?
    @uuid_prefix = uuid_prefix
    @repeat_locations = raw_location[REPEAT_LOCATION] || {} # TODO: check if this is a Map or not

  end
  def initialize(*args)
    if args.count == 9
      initialize_all(args[0],args[1],args[2],args[3],args[4],args[5],args[6],args[7],args[8])
    elsif
      initialize_location(args[0],args[1])
    end
  end


  def update_with_new_location(location, location_type) 
    to_send = Array.new
    new_repetitions = @repeat_locations[location.new_loc] || 0
    old_repetitions = @repeat_locations[@new_loc] || 0
    ((location.t_last_seen - @t_last_seen) > ConfigVariables.expired_repetitions_time) and new_repetitions = 0 and old_repetitions = 0

    popularity = (Float((new_repetitions + 1).to_i / ((Float(@uuid) + 1) * 100.0) / 100.0)).round(1)

    if (location.t_last_seen - @t_last_seen >= ConfigVariables.expired_time) 
      e = LogStash::Event.new
      e.set(TIMESTAMP, @t_last_seen + MINUTE)
      e.set(OLD_LOC, @new_loc)
      e.set(NEW_LOC, "outside")
      e.set(DWELL_TIME, @dwell_time)
      e.set(TRANSITION, 1)
      e.set(REPETITIONS, @old_repetitions)
      e.set(POPULARITY, popularity)
      e.set(SESSION, "#{@uuid_prefix}-#{@uuid}")
      e.set(location_with_uuid(location_type), @new_loc)
      e.set(TYPE, location_type) 
      to_send.push(e)

      @t_global = location.t_global
      @t_last_seen = location.t_last_seen
      @t_transition = location.t_transition
      @old_loc = location.old_loc
      @new_loc = location.new_loc
      @consolidated = location.consolidated
      @entrance = location.entrance
      @dwell_time = location.dwell_time
      @lat_long = location.lat_long
      @uuid += 1  
    end

    if @new_loc == location.new_loc
      if (@consolidated == (location.new_loc))
        if (!same_minute?(@t_last_seen, location.t_last_seen))
          t = @t_last_seen + MINUTE
          #for t in ((@t_last_seen + MINUTE)..location.t_last_seen).step(MINUTE) do
          while t <= location.t_last_seen
            if (@dwell_time <= ConfigVariables.max_dwell_time)
              e =  LogStash::Event.new
              e.set(TIMESTAMP, t)
              e.set(OLD_LOC, location.new_loc)
              e.set(NEW_LOC, location.new_loc)
              e.set(TRANSITION, 0)
              e.set(REPETITIONS, new_repetitions)
              e.set(POPULARITY, popularity)
              e.set(DWELL_TIME, @dwell_time)
              e.set(SESSION, "#{@uuid_prefix}-#{@uuid}")
              e.set(location_with_uuid(location_type), location.new_loc)
              e.set(TYPE, location_type)
              e.set(LATLONG, location.lat_long) if location.lat_long

              to_send.push(e)
            end
            @dwell_time += 1
            t += MINUTE
          end # end while
        end
        logger.debug? && logger.debug("Consolidated state, sending [{#{to_send.size()}}] events")
        @t_last_seen = location.t_last_seen
      else
        if (location.t_last_seen - @t_last_seen >= ConfigVariables.consolidated_time)
          #repeat_locations[location.new_loc]
          if consolidated == "outside"
            e =  LogStash::Event.new
            e.set(TIMESTAMP, @t_global)
            e.set(OLD_LOC, @consolidated)
            e.set(NEW_LOC, @entrance)
            e.set(SESSION, "#{@uuid_prefix}-#{@uuid}")
            e.set(location_with_uuid(location_type), @entrance)
            e.set(TRANSITION, 1)
            e.set(REPETITIONS, 0)
            e.set(POPULARITY, popularity)
            e.set(DWELL_TIME, 1)
            e.set(TYPE, location_type)
            e.set(LATLONG, location.lat_long) if location.lat_long
            
            to_send.push(e)
            @consolidated = @entrance
            @t_transition += MINUTE
          else
            # // Last Consolidated location
            if (@t_global + MINUTE) <= (@t_transition - MINUTE)
              t = (@t_global + MINUTE)
              #for t in (@t_global + MINUTE)..(@t_transition - MINUTE).step(MINUTE)
              while t <= (@t_transition - MINUTE)
                break if !same_minute?(t, (@t_transition - MINUTE))
                e = LogStash::Event.new
                e.set(TIMESTAMP, t)
                e.set(OLD_LOC, @consolidated)
                e.set(NEW_LOC, @consolidated)
                e.set(SESSION, "#{@uuid_prefix}-#{@uuid}")
                e.set(DWELL_TIME, @dwell_time)
                e.set(location_with_uuid(location_type), @consolidated)
                e.set(TRANSITION, 0)
                e.set(REPETITIONS, old_repetitions)
                e.set(POPULARITY, popularity)
                e.set(TYPE, location_type)
                e.set(LATLONG, location.lat_long) if location.lat_long
  
                to_send.push(e)
                @dwell_time += 1
                t += MINUTE
              end
            else
              logger.error("ERROR: (@t_global + MINUTE) > (@t_transition - MINUTE) => #{@t_global + MINUTE} > #{@t_transition - MINUTE}")
            end
            # // Increasing the session uuid because this is new session
            @uuid += 1
            popularity = (((new_repetitions + 1) / (Float(@uuid) + 1)*100.0).to_i/100.0).round(1)
          end

          if (same_minute?(@t_transition, @t_global))
            @t_transition += MINUTE
            @t_last_seen += MINUTE
          end
          @dwell_time = 1
          # // Transition
          logger.debug? && logger.debug("@t_transition..@t_last_seen => #{@t_transition} .. #{@t_last_seen}")
          if @t_transition <= @t_last_seen
            t = @t_transition
            #for t in @t_transition..@t_last_seen.step(MINUTE)
            while t <= (@t_last_seen)
              e = LogStash::Event.new
              e.set(TIMESTAMP, t)
              e.set(OLD_LOC, @consolidated)
              e.set(NEW_LOC, location.new_loc)
              e.set(SESSION, "#{@uuid_prefix}-#{@uuid}")
              e.set(location_with_uuid(location_type), location.new_loc)
              e.set(DWELL_TIME, @dwell_time)
              e.set(TRANSITION, 1)
              e.set(REPETITIONS, 0)
              e.set(POPULARITY, popularity)
              e.set(TYPE, location_type)
              e.set(LATLONG, location.lat_long) if location.lat_long

              to_send.push(e)
              @dwell_time += 1
              t += MINUTE
            end
          else
            logger.error("ERROR: @t_transition > @t_last_seen => #{@t_transition} .. #{@t_last_seen}")
          end
          @dwell_time = 1
          if (@t_last_seen + MINUTE) <= location.t_last_seen
            t = (@t_last_seen + MINUTE)
            #for t in (@t_last_seen + MINUTE)..location.t_last_seen.step(MINUTE)
            while t <= location.t_last_seen
              e = LogStash::Event.new
              e.set(TIMESTAMP, t)
              e.set(OLD_LOC, location.new_loc)
              e.set(NEW_LOC, location.new_loc)
              e.set(SESSION, "#{@uuid_prefix}-#{@uuid}")
              e.set(location_with_uuid(location_type), location.new_loc)
              e.set(DWELL_TIME, @dwell_time)
              e.set(TRANSITION, 0)
              e.set(REPETITIONS, new_repetitions)
              e.set(POPULARITY, popularity)
              e.set(TYPE, location_type)
              e.set(LATLONG, location.lat_long) if location.lat_long
              to_send.push(e)
              @dwell_time += 1
              t += MINUTE
            end
          else
            logger.error("ERROR: (@t_last_seen + MINUTE) > location => #{@t_last_seen + MINUTE} > #{location.t_last_seen}")
          end
          logger.debug? && logger.debug("Consolidating state, sending [{#{to_send.count}}] events")
          new_repetitions += 1
          @repeat_locations[location.new_loc] = new_repetitions
          @t_global = location.t_last_seen
          @t_last_seen = location.t_last_seen
          @t_transition = location.t_transition
          @old_loc = location.new_loc
          @new_loc = location.new_loc
          @consolidated = location.new_loc
          @lat_long = location.lat_long
        else
          logger.debug? && logger.debug("Trying to consolidate state, but {location.t_last_seen[#{location.t_last_seen.to_s}] - t_last_seen[#{@t_last_seen.to_s}] < consolidated_time[#{ConfigVariables.consolidated_time.to_s}")
        end

      end
    else
      logger.debug? && logger.debug("Moving from [{#{@new_loc}}] to [{#{location.new_loc}}]")
      @t_last_seen = location.t_last_seen
      @old_loc = @new_loc
      @new_loc = location.new_loc
      # Leaving consolidated location.
      @t_transition = location.t_last_seen if @old_loc == @consolidated
    end
    return to_send
  end

  def to_map
    map = Hash.new
    map[T_GLOBAL] = @t_global
    map[T_LAST_SEEN] =  @t_last_seen
    map[T_TRANSITION] = @t_transition
    map[DWELL_TIME] = @dwell_time
    map[OLD_LOC] =  @old_loc
    map[UUID] = @uuid
    map[NEW_LOC] = @new_loc
    map[CONSOLIDATED] = @consolidated
    map[ENTRANCE] = @entrance
    map[REPEAT_LOCATION] = @repeat_locations
    
    map[LATLONG] = @lat_long if @lat_long

    return map
  end

  def location_with_uuid(type)
    return type + "_uuid"
  end

  def same_minute?(time1, time2)
        return (time1 - time1 % MINUTE) == (time2 - time2 % MINUTE);
  end

end


