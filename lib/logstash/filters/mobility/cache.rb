# encoding: utf-8

require 'logstash/util/loggable'
require 'dalli'
require 'time'
require 'json'

require_relative '../util/mobility_constant'
require_relative '../util/utils'
require_relative 'client'

class Cache
  include LogStash::Util::Loggable
  include MobilityConstant
  
  def initialize(memcached)
    @memcached = memcached
  end

  public

  def load_client(client_mac, namespace_uuid)
    client_id = client_mac + namespace_uuid
    data = client_data(client_mac, client_id)
    client = data ? Client.create_from_cache(client_mac, namespace_uuid, data) : nil
  end

  def save_client(client)
    store = load_client_store(client)
    store[client.id] = client.location.to_map
    save_client_store(client, store)
  end

  def delete_client(client)
    store = load_client_store(client)
    store.delete(client.id)
  end

  # This function search for expired clients
  # deleting them from the cache and 
  # generate its movements to ouside
  # return Array of events
  def clean_expired_clients
    logger.debug("[mobility] Calculating expired clients at #{Time.now}")
    expired_events  = []
    expired_clients_counter = 0

    stores.each do |store_id, store|
      expired_clients = []
      
      logger.debug("[mobility] Looping over store (#{store_id}): #{store}")

      store.each do |client_data|
        id = client_data[0]
        client_mac = id[0..16]
        namespace_uuid = id[17..-1]
        data = client_data[1]
        logger.debug("[mobility] Checking if client (#{id}) is expired..")
        next unless data && data["campus_uuid"] && data["campus_uuid"]["t_last_seen"] && client_expired?(data["campus_uuid"]["t_last_seen"]) && valid_mac_address?(client_mac)
  

        client = Client.create_from_cache(client_mac, namespace_uuid, data)
        expired_events_client = []

        if data["campus_uuid"]["consolidated"] == "outside"
          expired_clients.push(client) 
          expired_clients_counter += 1
          logger.debug("[mobility] Client (#{client.id}) expired but was already outside, deleting from cache only")
        else
          expired_events_client += client.update_location_to_outside!
          if expired_events_client.count <= 0
            logger.debug("[mobility] Could not expire client (#{client.id}): no events to ouside were generated")
          else
            logger.debug("[mobility] Adding client (#{client.id}) expired with #{expired_events_client.count} events to outside")
            expired_clients.push(client)
            expired_clients_counter += 1
          end
        end
        # Enrich expired events with client_mac and namespace_uuid
        # to use it later to enrich the @dim_to_enrich
        expired_events_client.each do |expired_event_client|
          expired_event_client.set("expiration_track", 1)
          expired_event_client.set(CLIENT, client_mac)
          expired_event_client.set(NAMESPACE_UUID, namespace_uuid)

          expired_events.push(expired_event_client)
        end # expired_events_client.each
      end # store.values.each

      # Remove expired clients from the store
      expired_clients.each{ |client| store.delete(client.id) }
      save_store(store_id, store) if expired_clients.count > 0
    end # stores.each_with_index

    logger.debug("[mobility] Number of expired clients #{expired_clients_counter}")
    logger.debug("[mobility] Number of expired events #{expired_events.count}")
    expired_events 
  end # client_expired_clients

  private

  def stores
    mobility_stores = []
    Configuration.number_of_stores.times do |id|
      mobility_stores.push("mobility#{id}")
    end
    logger.debug("[mobility] mobility_stores: #{mobility_stores}")
    @memcached.get_multi(mobility_stores) || {}
  end

  def client_expired?(last_seen)
    Time.now.to_i > (last_seen + (Configuration.max_time_without_movement))
  end

  def hash_mac(mac_address)
    Digest::SHA256.hexdigest(mac_address).to_i(16)
  end

  def assign_store(mac_address)
    hash_value = hash_mac(mac_address)
    hash_value % Configuration.number_of_stores
  end

  def client_data(client_mac, client_id)
    data = nil

    id = assign_store(client_mac)
    stores = @memcached.get_multi("mobility#{id}","mobility-historical#{id}") || {}
    stores.values.each do |store|
      if store[client_id]
        data = store[client_id]
        break
      end
    end # stores.values.each

    data
  end # client_data

  def load_client_store(client)
    store_id = assign_store(client.client_mac)
    @memcached.get("mobility#{store_id}") || {}
  end

  def save_client_store(client, store)
    id = assign_store(client.client_mac)
    @memcached.set("mobility#{id}", store)
  end

  def save_store(store_name, store)
    logger.debug("[mobility] Saving store #{store_name} with #{store}")
    @memcached.set(store_name, store)
  end

  def valid_mac_address?(mac)
    !!(/^(?:[0-9A-Fa-f]{2}[:-]){5}(?:[0-9A-Fa-f]{2})$/ === mac)
  end
end # Class cache
