
module TabajaraReplicator
	class StructureImporter
		attr_reader :app

		def initialize(app)
			@app = app
		end

		def import
			app.config[:schemas].each do |k, v|
				sync_tables database: k
			end
		end

		def sync_tables(where, hints = [])
			iterate_tables(where) do |db, table, structure|
				sync_table db, table, structure, hints
			end
		end

		def sync_table(schema, table, structure, hints = [])
			target_schema = app.config[:schemas][schema.to_sym]
			target_structure = load_target(target_schema, table)
			is_new_table = target_structure[:columns].values.empty?

			if is_new_table
				create_table schema, table, structure[:columns]
			else
				structure[:columns].each do |c|
					target = target_structure[:columns].find { |t| t['column_name'] == c['column_name'] }

					if target
						sync_column schema, table, c, target, hints
					else
						create_column schema, table, c, hints, is_new_table
						is_new_table = false
					end
				end
			end
		end

		def run(command)
			puts command
			app.pg.exec command
		end

		def sync_column(schema, table, column, target, hints)
			actions = []
			query = "ALTER TABLE #{schema}.#{table.underscore} "

			if column['data_type'] != target['udt_name']
				type = column['data_type']

				if column['column_type'].start_with? 'enum('
					type = "#{schema}.#{type}"
					sync_data_type schema, table, column 
				end

				actions << "ALTER COLUMN #{column['column_name'].escape_pg} TYPE #{column['data_type']}"
			end

			if column['nullable'] != target['nullable']
				if column['nullable'] == 't'
					actions << "ALTER COLUMN #{column['column_name'].escape_pg} DROP NOT NULL"
				else
					actions << "ALTER COLUMN #{column['column_name'].escape_pg} SET NOT NULL"
				end
			end

			if mangle_default(schema, column) != target['default_value']
				if column['default_value'] == nil
					actions << "ALTER COLUMN #{column['column_name'].escape_pg} DROP DEFAULT"
				else
					actions << "ALTER COLUMN #{column['column_name'].escape_pg} SET DEFAULT #{mangle_default(schema, column)}"
				end
			end

			return if actions.empty?

			run "ALTER TABLE #{schema}.#{table.underscore} " + actions.join(", ")
		end

		def create_column(schema, table, column, hints, should_create_table)
			desc = gen_column_desc(schema, column)

			sync_data_type schema, table, column if column['column_type'].start_with? 'enum('

			run "ALTER TABLE #{schema}.#{table.underscore} ADD #{desc}"
		end

		def create_table(schema, table, columns)
			query = "CREATE TABLE #{schema}.#{table.underscore} ("

			query += columns.map do |c|
				sync_data_type schema, table, c if c['column_type'].start_with? 'enum('
				gen_column_desc schema, c
			end.join(', ')

			query += ")"

			run query
		end

		def sync_data_type(schema, table, column)
			values = column['column_type'][5..-2].split(',')
			target = load_target_enum(schema, table, column)

			if target.empty?
				run "CREATE TYPE #{schema}.#{column['data_type']} AS ENUM (#{values.join(", ")})"
			else
				actions = []
				added = values - target

				added.each do |v|
					run "ALTER TYPE #{schema}.#{column['data_type']} ADD VALUE #{v}"
				end
			end
		end

		def gen_column_desc(schema, column)
			type = column['data_type']

			type = "#{schema}.#{type}" if column['column_type'].start_with? 'enum('

			desc = "#{column['column_name'].escape_pg} #{type}"

			desc += " DEFAULT '#{mangle_default(schema, column)}'" if column['default_value']
			desc += " NOT NULL" if column['nullable'] == 'NO'

			desc
		end

		def mangle_default(schema, column)
			column['default_value']
			# if column['default_value'] == nil
			# 	nil
			# # elsif column['column_type'].start_with? 'enum('
			# # 	# "'#{column['default_value']}'::#{schema}.#{column['data_type']}"
			# # 	"#{column['default_value']}"
			# else
			# 	# column['default_value'].to_s.is_number? ? column['default_value'] : "'#{column['default_value']}'"
			# 	"#{column['default_value']}"
			# end
		end

		def iterate_tables(where)
			query = "
			SELECT
							t.TABLE_SCHEMA AS schema_name,
							t.TABLE_NAME AS table_name,
							c.COLUMN_NAME AS column_name,
							c.COLUMN_TYPE AS column_type,
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

			results = app.mysql.query(query)

			results.group_by { |r| "#{r['schema_name']}.#{r['table_name']}" }.each do |row|
				columns = row[1]
				db = columns[0]['schema_name']
				table = columns[0]['table_name']
				indices = app.mysql.query("
																	SELECT
																					INDEX_NAME as index_name,
																					SEQ_IN_INDEX as column_index,
																					COLUMN_NAME as column_name,
																					NON_UNIQUE as non_unique,
																					INDEX_TYPE as index_type
																	FROM information_schema.STATISTICS
																	WHERE TABLE_SCHEMA = '#{db}'
																			AND TABLE_NAME = '#{table}'
																	")

				yield db, table, {
					columns: columns,
					indices: indices
				}
			end
		end

		def load_target(schema, table)
			columns = app.pg.exec("
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
														WHERE
															table_schema = '#{schema}'
															AND table_name = '#{table.underscore}'
														")

			indices = app.pg.exec("
														SELECT
																i.relname as index_name,
																ix.indkey::text AS column_index,
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
																AND t.relname = '#{table.underscore}'
														")

			{
				columns: columns,
				indices: indices
			}
		end

		def load_target_enum(schema, table, column)
			results = app.pg.exec("
														 SELECT e.enumlabel AS enum_label
														 FROM pg_catalog.pg_type t
														 JOIN pg_catalog.pg_enum e
																 ON e.enumtypid = t.oid
														 JOIN pg_catalog.pg_namespace n
														 		 ON n.oid = t.typnamespace
														 WHERE n.nspname = '#{schema}'
														 		 AND t.typname = '#{column['data_type']}'
														")

			results.map do |r|
				"'#{r['enum_label']}'"
			end
		end
	end
end

