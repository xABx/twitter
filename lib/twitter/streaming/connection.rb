require 'http/parser'
require 'openssl'
require 'resolv'

module Twitter
  module Streaming
    class Connection
      attr_reader :tcp_socket_class, :ssl_socket_class

      def initialize(options = {})
        @tcp_socket_class = options.fetch(:tcp_socket_class) { TCPSocket }
        @ssl_socket_class = options.fetch(:ssl_socket_class) { OpenSSL::SSL::SSLSocket }
        @using_ssl        = options.fetch(:using_ssl)        { false }
        @select_timeout   = options.fetch(:select_timeout)  { 90 }
      end

      def stream(request, response)
        client = connect(request)
        request.stream(client)

        loop do
          begin
            body = client.read_nonblock(1024) # rubocop:disable AssignmentInCondition
            response << body
          rescue IO::WaitReadable
            puts "[#{DateTime.now.to_s}] rescuing IO::WaitReadable and performing IO.select"

            #Wait 90 secs per twitter documentation on stalls
            readables, _, _ = IO.select([client], [], [], @select_timeout)
            if readables.nil?
              client.close
              raise Twitter::Error::ServerError.new('Streaming timeout')
            else
              retry
            end
          end
        end
      end

      def connect(request)
        client = new_tcp_socket(request.socket_host, request.socket_port)
        return client if !@using_ssl && request.using_proxy?

        client_context = OpenSSL::SSL::SSLContext.new
        ssl_client     = @ssl_socket_class.new(client, client_context)
        ssl_client.connect
      end

    private

      def new_tcp_socket(host, port)
        @tcp_socket_class.new(Resolv.getaddress(host), port)
      end
    end
  end
end
