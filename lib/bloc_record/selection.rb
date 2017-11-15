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
            SELECT #{column.join ","} FROM #{table}
            WHERE #{attribute} = #{BlocRecord::Utility.sql_strings(value)};
        SQL
        init_object_from_row(row)
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
    
    # written for assignment #2
    def find_by(attribute, value)
        output = []
        connection.execute <<-SQL do |row|
            SELECT #{columns.join ","} FROM #{table}
            WHERE #{attribute} = #{value};
        SQL
            data = Hash[columns.zip(row)]
            data_obj = new(data)
            output << data_obj
        end
        output
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
            value = *args[0]
            find_by(attribute, value)
        else
            puts "Method not found."
            return nil
        end
    end
    # iterate through entry records in 1 batch of entries
    # just chose a practical max batch size as the default
    def find_each(start=1, batch_size=10000)
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
                SELECT #{columns.push(count(id)).join ","} FROM #{table}
                ORDER BY id
                OFFSET BY #{start}
                LIMIT #{batch_size};
            SQL
            entries = rows_to_array(rows)
        end
        if count(id) > 10000
            puts "Only first 10,000 records returned. To access additional records, use find_in_batches()."
        end
        entries.each{ |e| yield e }
    end
    # retrieve a batch of entry records, to be used in find_in_batches()
    def find_batch(start, batch_size)
        if start >= 0 && batch_size > 0
            rows = connection.execute <<-SQL
                SELECT #{columns.join ","} FROM #{table}
                ORDER BY id
                OFFSET BY #{start}
                LIMIT #{batch_size};
            SQL
            return rows_to_array(rows)
        end
        
    end
    # iterate through batches of entry records
    def find_in_batches(start=1, batch_size=10000)
        if !(check_integer(start,1) && check_integer(batch_size,1))
            puts "Invalid arguments. Start and batch_size must be positive integers."
            return nil
        end
        batch_start = start
        max = connection.execute <<-SQL
            SELECT count(id) FROM #{table};
        SQL
        while batch_start < max
            batch = find_batch(batch_start, batch_size)
            yield batch
            batch_start += batch_size
        end
    end
    
    private
    def init_object_from_row(row)
        if row
            data = Hash[columns.zip(row)]
            new(data)
        end
    end
    
    def rows_to_array(rows)
        rows.map { |row| new(Hash[columns.zip(row)]) }
    end
    
end