require 'sqlite3'
require 'bloc_record/schema'

module BlocRecord
    class Collection < Array
        
        def update_all(updates)
            ids = self.map(&:id)
            self.any? ? self.first.class.update(ids, updates) : false
        end
        
        # copied from selection.rb private methods, added parameter for class
        # def rows_to_array(rows, class_for_columns)
        #     columns = class_for_columns.columns
        #     collection = BlocRecord::Collection.new
        #     rows.each { |row| collection << new(Hash[columns.zip(row)]) }
        #     collection
        # end
        
        def take 
            self[0]
        end
        
        def where(args_hash, exclude=false) 
            ids = self.map(&:id)
            if self.any?
                #id_clause = exclude ? ("id IN (#{ids.join(",")}) AND NOT ") : (id_clause = "id IN (#{ids.join(",")}) AND ")
                id_clause = "id IN (#{ids.join(",")}) AND "
                id_clause << "NOT " if exclude
                arg_clause = args_hash.map { |key, value| "#{key}=#{BlocRecord::Utility.sql_strings(value)}"}.join(" and ")
                where_clause = id_clause + arg_clause
                
                the_class = self.first.class # typically, the_class is Entry
                rows = the_class.connection.execute <<-SQL
                    SELECT #{the_class.columns.join ","} FROM #{the_class.table}
                    WHERE #{where_clause};
                SQL
                the_class.rows_to_array(rows)
            else
                false
            end
        end
        
        def not(args_hash)
            where(args_hash, true)
        end
        
    end
end
