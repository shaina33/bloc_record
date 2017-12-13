require 'sqlite3'

module Selection
    def find_one(id)
        if check_integer(id, 1)
            row = connection.get_first_row <<-SQL
                SELECT #{columns.join ","} FROM #{table}
                WHERE id = #{id};
            SQL
            init_object_from_row(row)
        else
            puts "ID is invalid. Please provide a positive integer."
            return nil
        end
    end
    
    def find(*ids)
        ids.each do |id|
            if !check_integer(id, 1)
                puts "At least one invalid ID provided. Ensure all IDs are positive integers."
                return nil
            end
        end
        if ids.length == 1
            find_one(ids.first)
        else
            rows = connection.execute <<-SQL
                SELECT #{columns.join ","} FROM #{table}
                WHERE id IN (#{ids.join(",")});
            SQL
            rows_to_array(rows)
        end
    end
    
    def find_by(attribute, value)
        row = connection.get_first_row <<-SQL
            SELECT #{columns.join ","} FROM #{table}
            WHERE #{attribute} = #{BlocRecord::Utility.sql_strings(value)};
        SQL
        init_object_from_row(row)
    end
    
    # assignment 2
    def find_many_by(attribute, value)
        rows = connection.execute <<-SQL
            SELECT #{columns.join ","} FROM #{table}
            WHERE #{attribute} = #{BlocRecord::Utility.sql_strings(value)};
        SQL
        rows_to_array(rows)
    end
    
    def take(num=1)
        if !check_integer(num, 1)
            puts "Invalid number provided. Parameter must be a positive integer."
            return nil
        end
        if num > 1
            rows = connection.execute <<-SQL
                SELECT #{columns.join ","} FROM #{table}
                ORDER BY random()
                LIMIT #{num};
            SQL
            rows_to_array(rows)
        else
            take_one
        end
    end
    
    def take_one
        row = connection.get_first_row <<-SQL
            SELECT #{columns.join ","} FROM #{table}
            ORDER BY random()
            LIMIT 1;
        SQL
        init_object_from_row(row)
    end
    
    def first
        row = connection.get_first_row <<-SQL
            SELECT #{columns.join ","} FROM #{table}
            ORDER BY id
            ASC LIMIT 1;
        SQL
        init_object_from_row(row)
    end
    
    def last
        row = connection.get_first_row <<-SQL
            SELECT #{columns.join ","} FROM #{table}
            ORDER BY id
            DESC LIMIT 1;
        SQL
        init_object_from_row(row)
    end
    
    def all
        rows = connection.execute <<-SQL
            SELECT #{columns.join ","} FROM #{table};
        SQL
        rows_to_array(rows)
    end
    
    # written for assignment #3
    # returns True for integer of at least min, False otherwise
    # to be used for input validation
    def check_integer(input, min=0)
        if input.is_a?(Integer) and input >= min
            return true
        end
        return false
    end
    # allows function calls such as Entry.find_by_name("Jerome")
    def method_missing(m, *args)
        words = m.to_s.split('_')
        if words[0] === 'find' && words[1] === 'by'
            attribute = words[2]
            value = args[0]
            find_by(attribute, value)
        else
            puts "Method not found."
            return nil
        end
    end
    # iterate through entry records in 1 batch of entries
    # just chose a practical max batch size as the default
    # when invoking with parameters, must use keyword names, ex. find_each(start: 2, batch_size: 10)
    def find_each(start: 1, batch_size: 10000)
        if !(check_integer(start,1) && check_integer(batch_size,1))
            puts "Invalid arguments. Start and batch_size must be positive integers."
            return nil
        end
        if batch_size > 10000
           puts "Maximum batch size is 10,000 records. Use find_in_batches() with smaller batches."
           return nil
        end
        if start >= 0 && batch_size > 0
            rows = connection.execute <<-SQL
                SELECT #{columns.join ","} FROM #{table}
                ORDER BY id
                LIMIT #{batch_size}
                OFFSET #{start};
            SQL
            entries = rows_to_array(rows)
        end
        entries.each{ |e| yield e }
    end
    # retrieve a batch of entry records, to be used in find_in_batches()
    def find_batch(start, batch_size)
        if start >= 0 && batch_size > 0
            rows = connection.execute <<-SQL
                SELECT #{columns.join ","} FROM #{table}
                ORDER BY id
                LIMIT #{batch_size}
                OFFSET #{start};
            SQL
            return rows_to_array(rows)
        end
        
    end
    # iterate through batches of entry records
    def find_in_batches(start: 1, batch_size: 10000)
        if !(check_integer(start,1) && check_integer(batch_size,1))
            puts "Invalid arguments. Start and batch_size must be positive integers."
            return nil
        end
        batch_start = start
        row = connection.execute <<-SQL
            SELECT count(id) FROM #{table};
        SQL
        max = row[0][0]
        batch_num = 1
        while batch_start < max
            batch = find_batch(batch_start, batch_size)
            yield batch, batch_num
            batch_start += batch_size
            batch_num += 1
        end
    end
    # end Assignment #3
    
    def where(*args)
        if args.count > 1
            expression = args.shift
            params = args
        else
            case args.first
            when String
                expression = args.first
            when Hash
                expression_hash = BlocRecord::Utility.convert_keys(args.first)
                expression = expression_hash.map {|key, value| "#{key}=#{BlocRecord::Utility.sql_strings(value)}"}.join(" and ")
            end
        end
        # puts "table is: #{table}"
        sql = <<-SQL
            SELECT #{columns.join ","} FROM #{table}
            WHERE #{expression};
        SQL
        
        rows = connection.execute(sql, params)
        rows_to_array(rows)
    end
    
    # function given in checkpoint, then expanded and refactored in assignment #4
    def order(*args)
        order = []
        args.each do |arg|
            if arg.is_a? Hash
                term = arg.map{ |key, value| "#{key} #{value}" }.join(',')
            else
                term = arg 
            end
            order << term
        end
        order = order.join(',')

        rows = connection.execute <<-SQL
            SELECT * FROM #{table}
            ORDER BY #{order};
        SQL
        rows_to_array(rows)
    end
    
    def join(*args)
        if args.count > 1
            joins = args.map { |arg| "INNER JOIN #{arg} ON #{arg}.#{table}_id = #{table}.id"}.join(" ")
            rows = connection.execute <<-SQL
                SELECT * FROM #{table} #{joins}
            SQL
        else
            case args.first
            when String
                rows = connection.execute <<-SQL
                    SELECT * FROM #{table} #{BlocRecord::Utility.sql_strings(arg)};
                SQL
            when Symbol
                rows = connection.execute <<-SQL
                    SELECT * FROM #{table}
                    INNER JOIN #{args.first} ON #{args.first}.#{table}_id = #{table}.id
                SQL
            when Hash # assignment 4, does not support irregular plurals, such as 'entries'
                joins_hash = BlocRecord::Utility.convert_keys(args.first)
                # note: key is chopped in mapping because must be adjusted for plural, and hash keys are immutable
                joins_clause = joins_hash.map {|key, value| "INNER JOIN #{joins_hash.keys[0].chop} ON #{joins_hash.keys[0].chop}.#{table}_id = #{table}.id INNER JOIN #{value} ON #{value}.#{joins_hash.keys[0].chop}_id = #{joins_hash.keys[0].chop}.id" }.join
                sql_query = "SELECT * FROM #{table} #{joins_clause}"
                puts sql_query
                rows = connection.execute <<-SQL
                    #{sql_query}
                SQL
            end
        end
        rows_to_array(rows)
    end
    
    def rows_to_array(rows)
        collection = BlocRecord::Collection.new
        rows.each { |row| collection << new(Hash[columns.zip(row)]) }
        collection
    end
    
    private
    def init_object_from_row(row)
        if row
            data = Hash[columns.zip(row)]
            new(data)
        end
    end
    

    
end