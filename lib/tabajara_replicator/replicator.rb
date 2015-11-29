
module TabajaraReplicator
	class Replicator
		attr_reader :app

		def initialize(app)
			@app = app
		end

		def start
		end

		def stop
			yield
		end
	end
end

