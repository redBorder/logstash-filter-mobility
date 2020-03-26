# encoding: utf-8

require "logstash/filters/base"
require "logstash/namespace"
require "json"
require "time"
require "dalli"
require_relative "mobility/dimensions"
require_relative "mobility/locationData"

class LogStash::Filters::Mobility < LogStash::Filters::Base

  config_name "mobility"

  #config :path, :validate => :path, :default => " ", :required => false
  config :consolidatedTime, :validate => :number, :default => 180, :required => false
  config :expiredTime, :validate => :number, :default => 1200, :required => false
  config :maxDwellTime, :validate => :number, :default => 1440, :required => false
  config :expiredRepetitionsTime, :validate => :number, :default => 10080, :required => false
  
  public
  def set_stores
    @store = @memcached.get("location")
    @store = Hash.new if @store.nil?
  end

  def register
    @store = {}
    @dimToDruid = [ MARKET_UUID, ORGANIZATION_UUID, ZONE_UUID, NAMESPACE_UUID,
                    DEPLOYMENT_UUID, SENSOR_UUID, NAMESPACE, SERVICE_PROVIDER_UUID, BUILDING_UUID, CAMPUS_UUID, FLOOR_UUID,
                    STATUS, CLIENT_PROFILE, CLIENT_RSSI_NUM ]
    options = {:expires_in => 0}
    @memcached = Dalli::Client.new("localhost:11211", options)
    set_stores
    puts "Im mobility!"
  end

#  def locv89(event)
#    generatedEvents = []
#    namespace_id = (event.get(NAMESPACE_UUID)) ? event.get(NAMESPACE_UUID) : ""
#    mseEventContent = event.get(LOC_STREAMING_NOTIFICATION)
#    if (mseEventContent)
#      location = mseEventContent[LOC_LOCATION]
#      toCache = {}
#      toDruid = {}
#      
#      if (location) 
#        geoCoordinate = (location[LOC_GEOCOORDINATEv8]) ? location[LOC_GEOCOORDINATEv8] : location[LOC_GEOCOORDINATEv9]
#        mapInfo = (location[LOC_MAPINFOv8]) ? location[LOC_MAPINFOv8] : location[LOC_MAPINFOv9]
#        macAddress = String(location[LOC_MACADDR])
#        toDruid[CLIENT_MAC] = macAddress
#        mapHierarchy = String(mapInfo[LOC_MAP_HIERARCHY])
#
#        if mapHierarchy
#          zone = mapHierarchy.split(">")
#          toCache[CAMPUS]   = zone[0] if (zone.length >= 1)
#          toCache[BUILDING] = zone[1] if (zone.length >= 2)
#          toCache[FLOOR]    = zone[2] if (zone.length >= 3)
#        end
#        
#        state = String(location[LOC_DOT11STATUS])
#        
#        if state
#          toDruid[DOT11STATUS] = state
#          toCache[DOT11STATUS] = state
#        end
#
#        if state and state.eql?LOC_ASSOCIATED
#          ip = location[LOC_IPADDR].to_a
#          toCache[WIRELESS_ID]      = location[LOC_SSID]       if location[LOC_SSID]
#          toCache[WIRELESS_STATION] = location[LOC_AP_MACADDR] if location[LOC_AP_MACADDR]
#          toDruid[LAN_IP] = ip.first if ip and ip.first
#        end
#      end
#      
#      if geoCoordinate
#        latitude = Float( (geoCoordinate[LOC_LATITUDEv8]) ? geoCoordinate[LOC_LATITUDEv8] : geoCoordinate[LOC_LATITUDEv9] )
#        latitude = Float ( (latitude * 100000 ).round / 100000 )
# 
#        longitude = Float(geoCoordinate[LOC_LONGITUDE])
#        longitude = Float ( (longitude * 100000 ).round / 100000 )
#
#        locationFormat = latitude.to_s + "," + longitude.to_s
#        toCache[CLIENT_LATLNG] = locationFormat 
#      end 
#      
#      dateString = String(mseEventContent[TIMESTAMP]) 
#      sensorName = String(mseEventContent[LOC_SUBSCRIPTION_NAME])
#     
#      toDruid[SENSOR_NAME] = sensorName if sensorName
#      
#      @dimToDruid.each { |dimension| toDruid[dimension] =  mseEventContent[dimension] if mseEventContent[dimension] }
#      @dimToDruid.each { |dimension| toDruid[dimension] =  event.get(dimension) if event.get(dimension) }
#
#      toDruid.merge!(toCache)
#      toDruid[CLIENT_RSSI] = "unknown"
#      toDruid[CLIENT_SNR] = "unknown"
#      toDruid[NAMESPACE_UUID] = namespace_id if !namespace_id.eql?"" 
#      toDruid[TYPE] = "mse" 
#      toDruid[TIMESTAMP] = (dateString) ? (Time.parse(dateString).to_i / 1000) : (Time.now.to_i / 1000)
#
#      if macAddress
#        @store[macAddress + namespace_id] = toCache
#        @memcached.set(LOCATION_STORE,@store)
#      end
#      
#      toDruid[CLIENT_PROFILE] = "hard"
#
#      namespace = event.get(NAMESPACE_UUID)
#      datasource = (namespace) ? DATASOURCE + "_" + namespace : DATASOURCE
#
#      counterStore = @memcached.get(COUNTER_STORE)
#      counterStore = Hash.new if counterStore.nil?
#      counterStore[datasource] = counterStore[datasource].nil? ? 0 : (counterStore[datasource] + 1)
#      @memcached.set(COUNTER_STORE,counterStore)
#      
#      flowsNumber = @memcached.get(FLOWS_NUMBER)
#      flowsNumber = Hash.new if flowsNumber.nil?
#      toDruid["flows_count"] = flowsNumber[datasource] if flowsNumber[datasource] 
#
#      #clean the event
#      enrichmentEvent = LogStash::Event.new
#      #event.to_hash.each{|k,v| event.remove(k) }
#      toDruid.each {|k,v| enrichmentEvent.set(k,v)}
#      generatedEvents.push(enrichmentEvent)
#      return generatedEvents
#    end
#  end
#
#  def locv10(event) 
#    messages = event.get("notifications")
#    generatedEvents = []
#    if messages
#      messages.each do |msg|
#        toCache = {}
#        toDruid = {}
#        
#        clientMac = String(msg[LOC_DEVICEID])
#        namespace_id = msg[NAMESPACE_UUID] ? msg[NAMESPACE_UUID] : ""
#
#        toCache[WIRELESS_ID] = msg[LOC_SSID] if msg[LOC_SSID]
#        toCache[NMSP_DOT11PROTOCOL] = msg[LOC_BAND] if msg[LOC_BAND]
#        toCache[DOT11STATUS] = msg[LOC_STATUS].to_i if msg[LOC_STATUS]
#        toCache[WIRELESS_STATION] = msg[LOC_AP_MACADDR] if msg[LOC_AP_MACADDR]
#        toCache[CLIENT_ID] = msg[LOC_USERNAME] if msg[LOC_USERNAME]
#  
#        toDruid.merge!(toCache)
#        
#        toDruid[SENSOR_NAME] = msg[LOC_SUBSCRIPTION_NAME]
#        toDruid[CLIENT_MAC] = clientMac
#        toDruid[TIMESTAMP] = msg[TIMESTAMP].to_i / 1000
#        toDruid[TYPE] = "mse10-association"
#        toDruid[LOC_SUBSCRIPTION_NAME] = msg[LOC_SUBSCRIPTION_NAME]
#        
#        toDruid[MARKET] = msg[MARKET] if msg[MARKET]
#        toDruid[MARKET_UUID] = msg[MARKET_UUID] if msg[MARKET]
#        toDruid[ORGANIZATION] = msg[ORGANIZATION] if msg[ORGANIZATION]
#        toDruid[ORGANIZATION_UUID] = msg[ORGANIZATION_UUID] if msg[ORGANIZATION_UUID]
#        toDruid[DEPLOYMENT] = msg[DEPLOYMENT] if msg[DEPLOYMENT]
#        todruid[DEPLOYMENT_UUID] = msg[DEPLOYMENT_UUID] if msg[DEPLOYMENT_UUID]
#        toDruid[SENSOR_NAME] = msg[SENSOR_NAME] if msg[SENSOR_NAME]
#        toDruid[SENSOR_UUID] = msg[SENSOR_UUID] if msg[SENSOR_UUID]
#        
#        @store[clientMac + namespace_id] = toCache
#        @memcached.set(LOCATION_STORE,@store)
#      
#        toDruid[CLIENT_PROFILE] = "hard"
#        
#        namespace = event.get(NAMESPACE_UUID)
#        datasource = (namespace) ? DATASOURCE + "_" + namespace : DATASOURCE
#
#        counterStore = @memcached.get(COUNTER_STORE)
#        counterStore = Hash.new if counterStore.nil?
#        counterStore[datasource] = counterStore[datasource].nil? ? 0 : (counterStore[datasource] + 1)
#        @memcached.set(COUNTER_STORE,counterStore)
#
#        flowsNumber = @memcached.get(FLOWS_NUMBER)
#        flowsNumber = Hash.new if flowsNumber.nil?
#        toDruid["flows_count"] = flowsNumber[datasource] if flowsNumber[datasource]
#
#        #clean the event
#        enrichmentEvent = LogStash::Event.new
#        toDruid.each {|k,v| enrichmentEvent.set(k,v)}
#        generatedEvents.push(enrichmentEvent)
#      end
#    end
#    return generatedEvents
#  end

  def filter(event)
     client = event.get(CLIENT).to_s
     namespace = (event.get(NAMESPACE)) ? event.get(NAMESPACE) : ""
     id = client + namespace

     if client
       events = {}
       currentLocation = LocationData.locationFromMessage(event,id)
       cacheData = @store[id]

       if cacheData
        # cacheLocation = LocationData.locationFromCache(cacheData, id)
        # events.merge!(cacheLocation.updateWithNewLocationData(currentLocation)
        # locationMap = cacheLocation.toMap()
        # @store[id] = locationMap
         puts "Updating client ID[{#{id}] with [{#{locationMap}]"
       else
        # locationMap = currentLocation.toMap()
        # @store[id] = locationMap
         puts "Creating client ID[{#{id}] with [{#{locationMap}]"
       end
      
       events.each do |e|
         # enrich |e| with extra info
         e[CLIENT] = client 
         @dimToEnrich.each { |d| e[d] = event.get(d) if event.get(d) }
         #Prepare Event object to send it to the pipeline
         enrichmentEvent = LogStash::Event.new
         e.each {|k,v| enrichmentEvent.set(k,v)}
         yield enrichmentEvent
       end
       event.cancel
     end    
 
#    generatedEvents = []
#    if (event.get(LOC_STREAMING_NOTIFICATION))
#      generatedEvents = locv89(event)
#    elsif (event.get(LOC_NOTIFICATIONS))
#      generatedEvents = locv10(event)
#    else
#      puts "WARN: Unknow location message: {#{event}}"
#    end
#
#    generatedEvents.each do |e|
#      yield e
#    end
#    event.cancel
  end  # def filter
end    # class Logstash::Filter::Mobility
