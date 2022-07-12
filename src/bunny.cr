require "db"
require "sqlite3"
require "json"
require "uuid"

# db = DB.open "sqlite3:./file.db"

class UUIDConverter
  def self.from_rs(rs : DB::ResultSet)
    UUID.new(rs.read(String))
  end
end

struct UUID
  def from_json(pull : JSON::PullParser) : UUID
    UUID.new(pull)
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

class Event
  include JSON::Serializable
  DB.mapping({
    id: {
      type:      UUID,
      converter: UUIDConverter,
    },
    streamName: {
      type: String,
    },
    eventData: {
      type:      JSON::Any,
      converter: JSONConverter,
    },
    at: {
      type:      Time,
      converter: TimeConverter,
    },
    processing: {
      type:      Bool,
      converter: BoolConverter,
      default:   false,
    },
  })

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
    event = self.by_id? id, db
    return event unless event.nil?
    raise Exception.new
  end
end

def runIn(span : Time::Span, &block : ->)
  a = false
  spawn do
    sleep span
    block.call
  end
end

class GrabTimeoutException < Exception
end

class Bunny
  @db : DB::Database

  def initialize
    @db = DB.open "sqlite3://%3Amemory%3A"
    @db.exec("
      create table if not exists events (
        id text primary key,
        streamName text not null,
        eventData text not null,
        at text not null,
        processing integer
      );
    ")
    @db.exec("
      create index if not exists name_idx on events (streamName, at asc) where processing != 1;
    ")
    print "Bunny initialized\n"
  end

  def show
    Event.from_rs(@db.query("select * from events"))
  end

  @grabWaiters = Hash(String, Array(Channel(Event))).new
  @processingEvents = Hash(UUID, Channel(UUID)).new

  def grab(streamName, timeout = 5000, ackTimeout = 5000)
    event = @db.query_one? "select * from events where streamName = ? and processing != 1 order by at limit 1", streamName, as: Event
    if event
      @db.exec("update events set processing = 1 where id = ?", event.not_nil!.id.to_s)
      runIn ackTimeout.milliseconds do
        puts "Timeout reached for #{streamName} req"
        nack event.not_nil!.id, streamName
      end
      return event
    end
    timeoutChan = Channel(Int32).new

    runIn timeout.milliseconds do
      puts "Timeout reached waiting for #{streamName} req"
      timeoutChan.send(1)
    end

    idChannel = Channel(Event).new

    if @grabWaiters.has_key?(streamName)
      @grabWaiters[streamName].push(idChannel)
    else
      @grabWaiters[streamName] = [idChannel] of Channel(Event)
    end

    # Fiber.yield
    select
    when timeoutChan.receive
      @grabWaiters[streamName].delete(idChannel)
      raise GrabTimeoutException.new "Grab timed out"
    when event = idChannel.receive
      runIn ackTimeout.milliseconds do
        nack event.not_nil!.id, streamName
      end
      event
    end
  end

  def post(streamName : String, data : Object)
    event = Event.new(streamName, JSON.parse(data.to_json))
    event.persist(@db)
    if waiters = @grabWaiters.[]?(streamName)
      if chan = waiters.pop
        sleep 1.millisecond
        chan.send(event)
        @grabWaiters.delete(streamName) if waiters.size == 0
      end
    end
    event.id
  end

  def ack(id : UUID)
    res = @db.exec("delete from events where id = ? and processing = 0", id.to_s)
  end

  def nack(uuid : UUID, streamName : String)
    @db.exec("update events set processing = 0 where id = ?", uuid.to_s)
    if waiters = @grabWaiters.[]?(streamName)
      return if waiters.size === 0
      if chan = waiters.pop
        chan.send(Event.by_id(uuid, @db))
        @grabWaiters.delete(streamName) if waiters.size == 0
      end
    end
  end
end
