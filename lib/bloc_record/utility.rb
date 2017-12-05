module BlocRecord
    module Utility
        extend self
        
        def underscore(camel_cased_word)
            string = camel_cased_word.gsub(/::/, '/')
            string.gsub!(/([A-Z]+)([A-Z][a-z])/,'\1_\2')
            string.gsub!(/([a-z\d])([A-Z])/,'\1_\2')
            string.tr!("-", "_")
            string.downcase
        end
        
        # written for assignment #2
        # example: bloc_record/snake_case => BlocRecord::SnakeCase
        def camelCase(snake_cased_word)
            string = snake_cased_word.gsub(/\//, '::')
            string.gsub!(/\A[a-z]|[_|:][a-z]/) {|match| match.upcase}
            string.gsub!(/([_])([A-Z])/, '\2')
        end
        
        def sql_strings(value)
            case value
            when String
                "'#{value}'"
            when Numeric
                value.to_s
            else
                "null"
            end
        end
        
        # takes an options hash and converts all the keys to string keys
        # allows usage of strings or symbols as hash keys
        def convert_keys(options)
            options.keys.each { |k| options[k.to_s] = options.delete(k) if k.kind_of?(Symbol) }
            options
        end
        
        def instance_variables_to_hash(obj)
            Hash[obj.instance_variables.map{ |var| ["#{var.to_s.delete('@')}", obj.instance_variable_get(var.to_s)]}]
        end
        
        def reload_obj(dirty_obj)
            persisted_obj = dirty_obj.class.find_one(dirty_obj.id)
            dirty_obj.instance_variables.each do |instance_variable|
                dirty_object.instance_variable_set(instance_variable, persisted_obj.instance_variable_get(instance_variable))
            end
        end
    end
end
