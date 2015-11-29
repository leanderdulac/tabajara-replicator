require_relative 'tabajara_replicator/application'

module TabajaraReplicator
	class << self
		def application
			@application ||= TabajaraReplicator::Application.new
		end
	end
end

