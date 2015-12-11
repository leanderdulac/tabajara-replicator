require 'yaml'
require 'kodama'
require 'daemons'
require 'optparse'
require 'mysql2'
require 'pg'
require_relative 'exts'
require_relative 'table'
require_relative 'column'
require_relative 'index'
require_relative 'data_type'
require_relative 'mysql_driver'
require_relative 'postgres_driver'
require_relative 'structure_importer'
require_relative 'data_importer'
require_relative 'replicator'

module TabajaraReplicator
	class Application
		attr_reader :config, :mysql, :pg

		def run
			options = {}

			OptionParser.new do |opts|
				opts.banner = "Usage: tabajara [options]"

				opts.on("-d", "--[no-]daemonize", "Run as a daemon") do |d|
					options[:daemonize] = d
				end
	
				opts.on("-f", "--config [file]", ) do |f|
					options[:config] = f
				end

				opts.on("-m", "--mode [mode]") do |m|
					options[:mode] = m
				end
			end.parse!

			parse_config options

			case options[:mode] || "replicate"
			when "sync"
				sync
			when "import"
				import
			when "replicate"
				start_replication
			else
				puts "Invalid mode #{options[:mode]}."
				exit 1
			end
		end

		def parse_config(options)
			@config = symbolize_keys(YAML.load_file(options[:config]))
			@config.merge! options

			@mysql = Mysql2::Client.new(config[:mysql])
			@pg = PG.connect(config[:postgres])
		end

		def sync
			StructureImporter.new(self).import
		end

		def import
			DataImporter.new(self).import
		end

		def start_replication
			Daemons.daemonize if config[:daemonize]

			replicator = Replication.new(self)

			Signal.trap(:INT) do
				replicator.stop do
					exit 0
				end
			end

			replicator.start
		end

		def symbolize_keys(hash)
			hash.inject({}){|result, (key, value)|
				new_key = case key
									when String then key.to_sym
									else key
									end
				new_value = case value
										when Hash then symbolize_keys(value)
										else value
										end
				result[new_key] = new_value
				result
			}
		end
	end
end

