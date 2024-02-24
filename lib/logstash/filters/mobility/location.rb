# encoding: utf-8

require "logstash/util/loggable"

require_relative "../util/mobility_constant"
require_relative "../util/utils"

class Location
  include LogStash::Util::Loggable
  include MobilityConstant
  
  attr_accessor :t_global, :t_last_seen, :t_transition, :dwell_time, :old_loc, :new_loc, 
                :consolidated, :entrance, :lat_long, :uuid_prefix, :uuid, :repeat_locations

  def initialize_from_params(t_global, t_last_seen, t_transition, old_loc, new_loc, consolidated, entrance, lat_long, uuid_prefix)
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
    @repeat_locations = {}
  end 

  def initialize_from_data(data, uuid_prefix)
    @t_global = Utils.timestamp_to_long(data[T_GLOBAL]) 
    @t_last_seen = Utils.timestamp_to_long(data[T_LAST_SEEN]) 
    @t_transition = Utils.timestamp_to_long(data[T_TRANSITION])
    @old_loc = data[OLD_LOC].to_s
    @new_loc = data[NEW_LOC].to_s
    @consolidated = data[CONSOLIDATED].to_s
    @entrance = data[ENTRANCE].to_s
    @dwell_time = data[DWELL_TIME].to_i
    @lat_long = data[LATLONG].to_s
    @uuid_prefix = uuid_prefix
    @uuid = data[UUID].to_i # TODO: check if this integer is a long?
    @repeat_locations = data[REPEAT_LOCATION] || {} # TODO: check if this is a Map or not
  end

  def initialize(*args)
    if args.count == 2
      initialize_from_data(*args)
    else
      initialize_from_params(*args)
    end
  end

  def self.create_from_params(t_global, t_last_seen, t_transition, old_loc, new_loc, consolidated, entrance, lat_long, uuid_prefix)
    new(t_global, t_last_seen, t_transition, old_loc, new_loc, consolidated, entrance, lat_long, uuid_prefix)
  end

  def self.create_from_data(data, uuid_prefix)
    new(data, uuid_prefix)
  end

  def update_location!(new_location, new_location_type) 
    events = []

    new_location_repetitions = @repeat_locations[new_location.new_loc] || 0
    old_location_repetitions = @repeat_locations[@new_loc] || 0

    # Reset repetitions if we reach expired_repetitions_time
    if (new_location.t_last_seen - @t_last_seen) > Configuration.expired_repetitions_time
      new_location_repetitions = 0
      old_location_repetitions = 0
    end

    popularity = (Float((new_location_repetitions + 1).to_i / ((Float(@uuid) + 1) * 100.0) / 100.0)).round(1)

    if location_time_expired?(new_location)
      logger.info("[mobility] (#{new_location_type}) Move to outside because time expired")

      @t_global = new_location.t_global
      @t_last_seen = new_location.t_last_seen

      events += move_to_outside_events(new_location, new_location_type, old_location_repetitions, popularity)

      @t_transition = new_location.t_transition
      @old_loc = new_location.old_loc
      @new_loc = new_location.new_loc
      @consolidated = new_location.consolidated
      @entrance = new_location.entrance
      @dwell_time = new_location.dwell_time
      @lat_long = new_location.lat_long
      # And start new session by increasing the session uuid
      @uuid += 1
    end

    # In case it is a new location -> we update location and wait
    # for another update to consolidate it
    if new_location_change?(new_location)
      logger.info("[mobility] (#{new_location_type}) Moving from [{#{@new_loc}}] to [{#{new_location.new_loc}}]")

      @t_last_seen = new_location.t_last_seen
      @old_loc = @new_loc
      @new_loc = new_location.new_loc

      # Leaving consolidated location.
      if @old_loc == @consolidated
        @t_transition = new_location.t_last_seen
      end
     
      return events
    end

    # When not a new location..
    
    # If we were already in same location and the location was consolidated
    # we generate the events needed to calculate the dwell time 
    if @consolidated == new_location.new_loc
      if (!same_minute?(@t_last_seen, new_location.t_last_seen))
        events += consolidated_location_events(new_location, new_location_type, new_location_repetitions, popularity)
      end
      @t_last_seen = new_location.t_last_seen
      logger.info("[mobility] (#{new_location_type}) Consolidated state, sending [{#{events.size()}}] events")
     
      return events
    end

    # Otherwise time to check if we need to consolidate the location
   
    # In case we dont consolidate yet we do nothing
    if !time_to_consolidate?(new_location)
      logger.info("[mobility] (#{new_location_type}) Not consolidated yet")

      return events
    end

    # Start the consolidation process

    if @consolidated == "outside"
      logger.info("[mobility] (#{new_location_type}) Transitioning from outside")
      events += transition_from_outside_events(new_location, new_location_type, popularity)
      @consolidated = @entrance
      @t_transition += MINUTE
    else
      if (@t_global + MINUTE) <= (@t_transition - MINUTE)
        logger.info("[mobility] (#{new_location_type}) Calculating dwell time events from last session")
        events += from_last_session_events(new_location, new_location_type, old_location_repetitions, popularity)
      end
      # And start new session by increasing the session uuid
      @uuid += 1
      popularity = (((new_location_repetitions + 1) / (Float(@uuid) + 1)*100.0).to_i/100.0).round(1)
    end

    if (same_minute?(@t_transition, @t_global))
      @t_transition += MINUTE
      @t_last_seen += MINUTE
    end

    @dwell_time = 1
    # Transition
    if @t_transition <= @t_last_seen
      logger.info("[mobility] (#{new_location_type}) Transitoning to new location")
      events += transition_to_new_location_events(new_location, new_location_type, popularity)
    end

    @dwell_time = 1
    if (@t_last_seen + MINUTE) <= new_location.t_last_seen
      logger.info("[mobility] (#{new_location_type}) Calculating dwell time events for this location")
      events += new_location_static_events(new_location, new_location_type, new_location_repetitions, popularity)
    end
 
    # Consolidating location
    new_location_repetitions += 1
    @repeat_locations[new_location.new_loc] = new_location_repetitions
    @t_global = new_location.t_last_seen
    @t_last_seen = new_location.t_last_seen
    @t_transition = new_location.t_transition
    @old_loc = new_location.new_loc
    @new_loc = new_location.new_loc
    @consolidated = new_location.new_loc
    @lat_long = new_location.lat_long

    logger.info("[mobility] (#{new_location_type}) Location was consolidated, sending [{#{events.count}}] events")

    return events
  end

  def to_map
    map = {}
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

  private

  def same_minute?(time1, time2)
    return (time1 - time1 % MINUTE) == (time2 - time2 % MINUTE);
  end

  def location_time_expired?(new_location)
    new_location.t_last_seen - @t_last_seen >= Configuration.expired_time
  end

  def new_location_change?(new_location)
    @new_loc != new_location.new_loc
  end

  def time_to_consolidate?(new_location)
    new_location.t_last_seen - @t_last_seen >= Configuration.consolidated_time
  end

  def move_to_outside_events(new_location, new_location_type, old_location_repetitions, popularity)
    events = []

    e = LogStash::Event.new
    e.set(TIMESTAMP, @t_last_seen - MINUTE)
    e.set(OLD_LOC, @new_loc)
    e.set(NEW_LOC, "outside")
    e.set(DWELL_TIME, @dwell_time)
    e.set(TRANSITION, 1)
    e.set(REPETITIONS, old_location_repetitions)
    e.set(POPULARITY, popularity)
    e.set(SESSION, "#{@uuid_prefix}-#{@uuid}")
    e.set("#{new_location_type}_uuid", @new_loc)
    e.set(TYPE, new_location_type) 
    events.push(e)

    events
  end

  def consolidated_location_events(new_location, new_location_type, new_location_repetitions, popularity)
    events = []
    t = @t_last_seen + MINUTE
    while t <= new_location.t_last_seen
      if (@dwell_time <= Configuration.max_dwell_time)
        e =  LogStash::Event.new
        e.set(TIMESTAMP, t)
        e.set(OLD_LOC, new_location.new_loc)
        e.set(NEW_LOC, new_location.new_loc)
        e.set(TRANSITION, 0)
        e.set(REPETITIONS, new_location_repetitions)
        e.set(POPULARITY, popularity)
        e.set(DWELL_TIME, @dwell_time)
        e.set(SESSION, "#{@uuid_prefix}-#{@uuid}")
        e.set("#{new_location_type}_uuid", new_location.new_loc)
        e.set(TYPE, new_location_type)
        e.set(LATLONG, new_location.lat_long) if new_location.lat_long

        events.push(e)
      end
      @dwell_time += 1
      t += MINUTE
    end # end while

    events
  end

  def from_last_session_events(new_location, new_location_type, old_location_repetitions, popularity)
    events = []
    # // Last Consolidated location
    t = (@t_global + MINUTE)
    while t <= (@t_transition - MINUTE)
      break if !same_minute?(t, (@t_transition - MINUTE))
      e = LogStash::Event.new
      e.set(TIMESTAMP, t)
      e.set(OLD_LOC, @consolidated)
      e.set(NEW_LOC, @consolidated)
      e.set(SESSION, "#{@uuid_prefix}-#{@uuid}")
      e.set(DWELL_TIME, @dwell_time)
      e.set("#{new_location_type}_uuid", @consolidated)
      e.set(TRANSITION, 0)
      e.set(REPETITIONS, old_location_repetitions)
      e.set(POPULARITY, popularity)
      e.set(TYPE, new_location_type)
      e.set(LATLONG, new_location.lat_long) if new_location.lat_long
  
      events.push(e)
      @dwell_time += 1
      t += MINUTE
    end

    events
  end

  def transition_to_new_location_events(new_location, new_location_type, popularity)
    events = []
    t = @t_transition
    #for t in @t_transition..@t_last_seen.step(MINUTE)
    while t <= (@t_last_seen)
      e = LogStash::Event.new
      e.set(TIMESTAMP, t)
      e.set(OLD_LOC, @consolidated)
      e.set(NEW_LOC, new_location.new_loc)
      e.set(SESSION, "#{@uuid_prefix}-#{@uuid}")
      e.set("#{new_location_type}_uuid", new_location.new_loc)
      e.set(DWELL_TIME, @dwell_time)
      e.set(TRANSITION, 1)
      e.set(REPETITIONS, 0)
      e.set(POPULARITY, popularity)
      e.set(TYPE, new_location_type)
      e.set(LATLONG, new_location.lat_long) if new_location.lat_long

      events.push(e)
      @dwell_time += 1
      t += MINUTE
    end

    events
  end

  def transition_from_outside_events(new_location, new_location_type, popularity)
    events = []
    e =  LogStash::Event.new
    e.set(TIMESTAMP, @t_global)
    e.set(OLD_LOC, @consolidated)
    e.set(NEW_LOC, @entrance)
    e.set(SESSION, "#{@uuid_prefix}-#{@uuid}")
    e.set("#{new_location_type}_uuid", @entrance)
    e.set(TRANSITION, 1)
    e.set(REPETITIONS, 0)
    e.set(POPULARITY, popularity)
    e.set(DWELL_TIME, 1)
    e.set(TYPE, new_location_type)
    e.set(LATLONG, new_location.lat_long) if new_location.lat_long
    
    events.push(e)

    events
  end

  def new_location_static_events(new_location, new_location_type, new_location_repetitions, popularity)
    events = []
    t = (@t_last_seen + MINUTE)
    while t <= new_location.t_last_seen
      e = LogStash::Event.new
      e.set(TIMESTAMP, t)
      e.set(OLD_LOC, new_location.new_loc)
      e.set(NEW_LOC, new_location.new_loc)
      e.set(SESSION, "#{@uuid_prefix}-#{@uuid}")
      e.set("#{new_location_type}_uuid", new_location.new_loc)
      e.set(DWELL_TIME, @dwell_time)
      e.set(TRANSITION, 0)
      e.set(REPETITIONS, new_location_repetitions)
      e.set(POPULARITY, popularity)
      e.set(TYPE, new_location_type)
      e.set(LATLONG, new_location.lat_long) if new_location.lat_long
      events.push(e)
      @dwell_time += 1
      t += MINUTE
    end

    events
  end

end


