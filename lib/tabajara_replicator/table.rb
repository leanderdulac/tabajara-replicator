
module TabajaraReplicator
	class Table
		attr_accessor :schema, :name, :columns, :indices

		def initialize(schema, name, columns = [], indices = [])
			@schema = schema
			@name = name
			@columns = columns
			@indices = indices
		end

		def column_by_name(name)
			columns.find do |c|
				c.name == name
			end
		end
	end
end

