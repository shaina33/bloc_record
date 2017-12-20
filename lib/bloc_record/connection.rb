require 'sqlite3'
require 'pg'

module Connection
    def connection
        if BlocRecord.database_type == :sqlite3
            @connection ||= SQLite3::Database.new(BlocRecord.database_filename)
        elsif BlocRecord.database_type == :pg
            @connection ||= PG::Connection.new(dbname: BlocRecord.database_filename, host: "localhost", user: "postgres", password: "password")
        else
            puts "Database connection error."
            puts "Database type is #{BlocRecord.database_type}."
        end
    end
end