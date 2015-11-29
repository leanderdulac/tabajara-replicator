require 'rake'

Gem::Specification.new do |s|
	s.platform    = Gem::Platform::RUBY
  s.name        = 'tabajara_replicator'
  s.version     = '1.0.0'
  s.date        = '2015-11-28'
  s.summary     = "MySQL to PostgreSQL replicator."
  s.description = "MySQL to PostgreSQL replicator."
  s.authors     = ["Jonathan Lima", 'Pedro Cesar']
  s.email       = 'jonathan@pagar.me'
  s.files       = ["lib/hola.rb"]
  s.homepage    = 'https://github.com/pagarme/tabajara-replicator'
  s.license     = 'MIT'
	s.files       = FileList['lib/**.rb',
                      'bin/*',
                      '[A-Z]*',
                      'test/**'].to_a

	s.add_dependency 'kodama'
	s.add_dependency 'daemons'
	s.add_dependency 'mysql2'
	s.add_dependency 'pg'
end

