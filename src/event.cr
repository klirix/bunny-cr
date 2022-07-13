class Event
  include JSON::Serializable
  include DB::Serializable

  @[DB::Field(converter: UUIDConverter)]
  property id : UUID

  property streamName : String

  @[DB::Field(converter: JSONConverter)]
  property eventData : JSON::Any

  @[DB::Field(converter: TimeConverter)]
  property at : Time

  @[DB::Field(converter: BoolConverter)]
  property processing : Bool = false

  def initialize(@streamName, @eventData)
    @id = UUID.random
    @at = Time.utc
    @processing = false
  end

  def persist(db : DB::Database)
    db.exec("insert into events values(?, ?, ?, ?, ?)", id.to_s, streamName, eventData.to_json, at.to_rfc3339, processing)
  end

  def self.by_id?(id : UUID, db : DB::Database)
    db.query_one? "select * from events where id = ?", id.to_s, as: Event
  end

  def self.by_id(id : UUID, db : DB::Database)
    db.query_one "select * from events where id = ?", id.to_s, as: Event
  end
end
