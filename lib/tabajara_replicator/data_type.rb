
module TabajaraReplicator
	class DataType
		attr_reader :schema, :name, :kind, :type, :items

		def initialize(schema, name, kind, type, items = nil)
			@schema = schema
			@name = name
			@kind = kind
			@type = type
			@items = items
		end
	end
end

