# CHQ: Gemini AI generated the template server
# Import the WEBrick library, which is a standard Ruby library for creating web servers.
require 'webrick'

# Define the port number on which the server will listen.
PORT = ENV['PORT'] || 4026 # Use environment variable or default to 4026

# Create a new WEBrick HTTP server instance.
# The :Port option specifies the port number.
server = WEBrick::HTTPServer.new(:Port => PORT)

# Register a handler for the root path ("/").
# When a request comes in for this path, the block will be executed.
# req: The HTTP request object, containing details about the client's request.
# res: The HTTP response object, which you will populate with the server's response.
server.mount_proc '/' do |req, res|
  # Set the content type of the response to HTML.
  # This tells the browser how to interpret the response body.
  res.content_type = 'text/html'

  # Set the body of the response. This is the content that will be sent back to the client.
  res.body = '<h1>Hello, Ruby Server!</h1><p>This is a simple server built with WEBrick.</p>'

  # You can also inspect request details if needed:
  puts "Request Path: #{req.path}"
  puts "Request Method: #{req.request_method}"
end

# Register a signal trap for INT (interrupt) signal (Ctrl+C).
# When Ctrl+C is pressed, this block will be executed, gracefully shutting down the server.
trap 'INT' do
  server.shutdown
end

# Start the server. This will make the server listen for incoming requests.
puts "Starting Ruby server on http://localhost:#{PORT}/"
server.start
