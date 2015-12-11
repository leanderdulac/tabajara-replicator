
module TabajaraReplicator
	module MySQLDriver
		class << self
			def find_tables(where)
				query = "
				SELECT
								t.TABLE_SCHEMA AS schema_name,
								t.TABLE_NAME AS table_name,
								c.COLUMN_NAME AS column_name,
								c.COLUMN_TYPE AS column_type,
								c.DATA_TYPE AS data_type,
								CASE
												WHEN c.DATA_TYPE = 'enum' THEN LOWER(CONCAT(c.TABLE_NAME, '_', c.COLUMN_NAME, '_enum_t'))
												WHEN c.DATA_TYPE = 'tinyint' THEN 'int2'
												WHEN c.DATA_TYPE = 'mediumint' THEN 'integer'
												WHEN c.DATA_TYPE = 'tinyint unsigned' THEN 'int2'
												WHEN c.DATA_TYPE = 'smallint unsigned' THEN 'integer'
												WHEN c.DATA_TYPE = 'mediumint unsigned' THEN 'integer'
												WHEN c.DATA_TYPE = 'int unsigned' THEN 'int8'
												WHEN c.DATA_TYPE = 'bigint' THEN 'int8'
												WHEN c.DATA_TYPE = 'bigint unsigned' THEN 'numeric(20)'
												WHEN c.DATA_TYPE = 'double' THEN 'double precision'
												WHEN c.DATA_TYPE = 'float' THEN 'float4'
												WHEN c.DATA_TYPE = 'datetime' THEN 'timestamp'
												WHEN c.DATA_TYPE = 'longtext' THEN 'text'
												WHEN c.DATA_TYPE = 'mediumtext' THEN 'text'
												WHEN c.DATA_TYPE = 'blob' THEN 'bytea'
												WHEN c.DATA_TYPE = 'int' THEN 'int4'
												WHEN c.DATA_TYPE = 'char' THEN 'bpchar'
												ELSE c.DATA_TYPE
								END AS data_type,
								c.IS_NULLABLE AS nullable,
								c.COLUMN_DEFAULT AS default_value
				FROM information_schema.TABLES AS t
				JOIN information_schema.COLUMNS AS c
						ON t.TABLE_CATALOG = c.TABLE_CATALOG
								AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
								AND t.TABLE_NAME = c.TABLE_NAME
				WHERE 1=1
				"

				query += " AND t.TABLE_SCHEMA = '#{where[:database]}'" if where[:database]
				query += " AND t.TABLE_NAME = '#{where[:table]}'" if where[:name]

				query += " ORDER BY t.TABLE_NAME, c.ORDINAL_POSITION"

				results = TabajaraReplicator.application.mysql.query(query)

				results.group_by { |r| "#{r['schema_name']}.#{r['table_name']}" }.map do |row|
					columns = row[1]
					db = columns[0]['schema_name']
					table = columns[0]['table_name']
					indices = TabajaraReplicator.application.mysql.query("
																		SELECT
																						INDEX_NAME as index_name,
																						SEQ_IN_INDEX as column_index,
																						COLUMN_NAME as column_name,
																						NON_UNIQUE as non_unique,
																						INDEX_TYPE as index_type
																		FROM information_schema.STATISTICS
																		WHERE TABLE_SCHEMA = '#{db}'
																				AND TABLE_NAME = '#{table}'
																		").group_by { |i| i['index_name'] }


					convert_table(db, table, columns, indices)
				end
			end

			def convert_table(schema, name, columns, indices)
				table = Table.new(schema, name.underscore)

				table.columns = columns.map do |c|
					name = c['column_name'].underscore
					data_type = convert_data_type(c['data_type'])
					type_name = data_type == :enum ? "#{schema}.enum_#{table.name}_#{name}" : c['column_type']
					type = DataType.new(schema, type_name, convert_data_type(c['data_type']), convert_enum(c['column_type']))

					Column.new(table, name, type, !!c['nullable'], c['default_value'])
				end

				table.indices = indices.map do |i|
					name = i[0].underscore
					columns = i[1]
					column_names = columns.sort do |a, b|
						a['column_index'] <=> b['column_index']
					end.map { |c| c['column_name'] }

					Index.new(table, name, columns[0]['index_type'].downcase.to_sym, column_names, !columns[0]['non_unique'])
				end

				table
			end

			def convert_data_type(type)
				case type
				when 'enum'
					:enum
				when 'tinyint'
					:int2
				when 'mediumint'
					:integer
				when 'smallint unsigned'
					:integer
				when 'mediumint unsigned'
					:integer
				when 'int unsigned'
					:int8
				when 'bigint'
					:int8
				when 'bigint unsigned'
					:int8
				when 'double'
					:double
				when 'float'
					:float
				when 'datetime'
					:datetime
				when 'longtext'
					:text
				when 'mediumtext'
					:text
				when 'blob'
					:blob
				when 'int'
					:integer
				when 'char'
					:char
				else
					type
				end
			end

			def convert_enum(type)
				return nil unless type.start_with? 'enum('

				type[5..-2].split(',').map do |i|
					i[1..-2]
				end
			end
		end
	end
end

