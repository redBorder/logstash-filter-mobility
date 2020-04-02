# encoding: utf-8

require_relative "location"

class Campus < Location

 def initialize(*args)
    if (args.count == 9)
        super(args[0],args[1],args[2],args[3],args[4],args[5],args[6],args[7],args[8])
    elsif (args.count == 2)
        super(args[0],args[1])
    end
 end
 
end
