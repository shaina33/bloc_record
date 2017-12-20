module BlocRecord
   def self.connect_to(filename, type)
     @database_filename = filename
     @database_type = type
     puts "Database filename & type configured."
   end
 
   def self.database_filename
     @database_filename
   end
   
   def self.database_type
      @database_type
   end
end