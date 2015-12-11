
module TabajaraReplicator
	class Index
		attr_accessor :table, :name, :type, :columns

		def initialize(table, name, type, columns = [], is_unique = true)
			@table = table
			@name = name
			@type = type
			@columns = columns
			@is_unique = is_unique
		end

		def unique?
			@is_unique
		end
	end
end

