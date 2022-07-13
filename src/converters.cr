class UUIDConverter
  def self.from_rs(rs : DB::ResultSet)
    UUID.new(rs.read(String))
  end
end

struct UUID
  def from_json(pull : JSON::PullParser) : UUID
    UUID.new(pull.read_string)
  end

  def to_json(json : JSON::Builder)
    json.string(to_s)
  end
end

class JSONConverter
  def self.from_rs(rs : DB::ResultSet)
    JSON.parse(rs.read(String))
  end
end

class TimeConverter
  def self.from_rs(rs : DB::ResultSet)
    Time.parse_rfc3339(rs.read(String))
  end
end

class BoolConverter
  def self.from_rs(rs : DB::ResultSet)
    rs.read(Int) == 1
  end
end
