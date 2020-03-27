# encoding: utf-8
require "time"

class Utils
  def self.timestamp2Long(timestamp)
    result = nil
    if timestamp
      result = (timestamp.kind_of? Integer) ? timestamp.to_i : Time.now.to_i / 1000
    else
      result = Time.now.to_i / 1000
    end
    return result - result % 60
  end

  def self.currentTimestamp() 
    self.timestamp2Long(nil)
  end
end
