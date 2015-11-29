
class String
  def underscore
    self.gsub(/::/, '/').
    gsub(/([A-Z]+)([A-Z][a-z])/,'\1_\2').
    gsub(/([a-z\d])([A-Z])/,'\1_\2').
    tr("-", "_").
    downcase
  end

  def is_number?
    true if Float(self) rescue false
  end

	# TODO: Use keyword list
	def escape_pg
		self == 'table' ? '"table"' : self
	end
end

