
module TabajaraReplicator
	module PostgresDriver
		class << self
			def find_table(where)
				find_tables(where)[0]
			end

			def find_tables(where)
				query = "
								SELECT
									table_schema AS schema_name,
									table_name AS table_name,
									column_name,
									data_type,
									is_nullable AS nullable,
									udt_name,
									column_default AS default_value
								FROM 
									information_schema.columns
								WHERE 1=1"
				
				query += " AND table_schema = '#{where[:database]}'" if where[:database]
				query += " AND table_name = '#{where[:name]}'" if where[:name]

				results = TabajaraReplicator.application.pg.exec(query)

				results.group_by { |r| "#{r['schema_name']}.#{r['table_name']}" }.map do |row|
					columns = row[1]
					schema = columns[0]['schema_name']
					table = columns[0]['table_name']
					indices = TabajaraReplicator.application.pg.exec("
																														SELECT
																																i.relname as index_name,
																																IDX(ix.indkey, a.attnum) AS column_index,
																																a.attname as column_name,
																																ix.indisunique AS is_unique,
																																ix.indisprimary AS is_primary
																														FROM
																																pg_catalog.pg_class t,
																																pg_catalog.pg_namespace n,
																																pg_catalog.pg_class i,
																																pg_catalog.pg_index ix,
																																pg_catalog.pg_attribute a
																														WHERE
																																t.oid = ix.indrelid
																																AND i.oid = ix.indexrelid
																																AND a.attrelid = t.oid
																																AND n.oid = t.relnamespace
																																AND a.attnum = ANY(ix.indkey)
																																AND t.relkind = 'r'
																																AND n.nspname = '#{schema}'
																																AND t.relname = '#{table}'
																													 ").group_by { |i| i['index_name'] }

					convert_table(schema, table, columns, indices)
				end
			end

			def convert_table(schema, name, columns, indices)
				table = Table.new(schema, name.underscore)

				table.columns = columns.map do |c|
					name = c['column_name'].underscore
					type_name = c['udt_name'] || c['data_type']
					type = DataType.new(schema, type_name, convert_data_type(c), convert_enum(schema, type_name))

					Column.new(table, name, type, !!c['nullable'], c['default_value'])
				end

				table.indices = indices.map do |i|
					name = i[0].underscore
					columns = i[1]
					column_names = columns.sort do |a, b|
						a['column_index'] <=> b['column_index']
					end.map { |c| c['column_name'] }

					Index.new(table, name, :btree, column_names, !!columns[0]['is_unique'])
				end

				table
			end

			def convert_data_type(c)
				case c['data_type']
				when 'USER-DEFINED'
					c['udt_name'].start_with?('enum') ? :enum : c['udt_name'].to_sym
				else
					c['data_type']
				end
			end

			def convert_enum(schema, type)
				return nil if type.start_with? 'enum_'

				results = TabajaraReplicator.application.pg.exec("
															 SELECT e.enumlabel AS enum_label
															 FROM pg_catalog.pg_type t
															 JOIN pg_catalog.pg_enum e
																	 ON e.enumtypid = t.oid
															 JOIN pg_catalog.pg_namespace n
																	 ON n.oid = t.typnamespace
															 WHERE n.nspname = '#{schema}'
																	 AND t.typname = '#{type}'
															")

				results.map do |r|
					"'#{r['enum_label']}'"
				end
			end
		end
	end
end

