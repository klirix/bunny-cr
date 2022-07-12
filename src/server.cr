require "./bunny.cr"
require "kemal"

bunny = Bunny.new

before_all do |env|
  env.response.content_type = "application/json"
end

get "/" do
  "bunny"
end

get "/show" do
  bunny.show.to_json
end

get "/grab" do |env|
  begin
    bunny.grab(env.params.query["streamName"]).to_json
  rescue GrabTimeoutException
    env.response.status = HTTP::Status::REQUEST_TIMEOUT
    "Timed out".to_json
  end
end

post "/post" do |env|
  id = bunny.post env.params.json["streamName"].as(String), env.params.json["data"]
  id.to_json
end

post "/nack" do |env|
  bunny.nack UUID.new(env.params.query["id"]), env.params.query["streamName"]
  %{"true"}
end

post "/ack" do |env|
  bunny.ack UUID.new(env.params.query["id"])
  %{"true"}
end

Kemal.run(3005)
