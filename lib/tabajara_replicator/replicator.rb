
module TabajaraReplicator
	class Replicator
		attr_reader :app

		def initialize(app)
			@app = app

			Kodama::Client.start(@app.config[:mysql]) do |c|
				puts "Listening to #{@app.config[:mysql][:host]}:#{@app.config[:mysql][:port]}"

				c.binlog_position_file = @app.config[:binlog_position_file]
				c.log_level = @app.config[:log_level]
				c.connection_retry_limit = @app.config[:connection_retry_limit]
				c.connection_retry_wait = @app.config[:connection_retry_wait]

				c.on_row_event do |event|
					perform(event)
				end
			end
		end

		def perform(event)
			puts event.inspect
		end

		def stop
			yield
		end
	end
end

