# encoding: utf-8
require "time"

class Utils
  def self.timestamp_to_long(timestamp)
    result = nil
    if timestamp
      result = (timestamp.kind_of? Integer) ? timestamp.to_i : Time.now.to_i 
    else
      result = Time.now.to_i 
    end
    puts "**************************"
    puts "me ha llegado: "
    puts timestamp
    puts "el resutl es: "
    puts result
    puts " y lo que devuelvo es:"
    puts result - result % 60
    puts "**************************"
    return result - result % 60
  end

  def self.current_timestamp() 
    self.timestamp_to_long(nil)
  end
end
