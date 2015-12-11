
module TabajaraReplicator
	class Column
		attr_reader :table, :name, :type, :data_type, :default_value

		def initialize(table, name, type, is_nullable, default_value)
			@table = table
			@name = name
			@type = type
			@data_type = data_type
			@is_nullable = is_nullable
			@default_value = default_value
		end

		def nullable?
			@is_nullable
		end
	end
end

