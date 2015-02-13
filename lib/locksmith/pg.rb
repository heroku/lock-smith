require 'zlib'
require 'uri'
require 'timeout'
require 'locksmith/config'

module Locksmith
  module Pg
    class DbUrlConnectionAdapter
      def with_connection
        connection = connect
        yield(connection)
      end

      private
      def connect
        @conn ||= PG::Connection.open(
                                      dburl.host,
                                      dburl.port || 5432,
                                      nil, '', #opts, tty
                                      dburl.path.gsub("/",""), # database name
                                      dburl.user,
                                      dburl.password
                                     )
      end

      def dburl
        URI.parse(ENV["DATABASE_URL"])
      end

      def conn=(conn)
        @conn = conn
      end
    end

    class ActiveRecordConnectionAdapter
      attr_writer :error_handler

      def with_connection
        ::ActiveRecord::Base.connection_pool.with_connection do |connection|
          yield(connection.raw_connection)
        end
      rescue PG::Error => e
        error_handler.call(e)
      end

      def error_handler
        @error_handler || lambda {}
      end
    end

    extend self

    def connection_adapter
      @connection_adapter || DbUrlConnectionAdapter.new
    end

    def connection_adapter=(adapter)
      @connection_adapter=adapter
    end

    def lock(name, opts={})
      opts[:ttl] ||= 60
      opts[:attempts] ||= 3
      opts[:attempt_interval] ||= 0.1
      opts[:lspace] ||= (Config.pg_lock_space || -2147483648)

      if create(name, opts)
        begin
          Timeout::timeout(opts[:ttl]) {
            return(yield)
          }
        ensure
          delete(name, opts)
        end
      else
        raise Locksmith::UnableToLock
      end
    end

    def key(name)
      i = Zlib.crc32(name)
      # We need to wrap the value for postgres
      if i > 2147483647
        -(-(i) & 0xffffffff)
      else
        i
      end
    end

    def create(name, opts)
      lock_args = [opts[:lspace], key(name)]
      opts[:attempts].times.each do |i|
        res = connection_adapter.with_connection do |conn|
          conn.exec("select pg_try_advisory_lock($1,$2)", lock_args)
        end
        if res[0]["pg_try_advisory_lock"] == "t"
          return(true)
        else
          return(false) if i == (opts[:attempts] - 1)
          sleep opts[:attempt_interval]
        end
      end
    end

    def delete(name, opts)
      lock_args = [opts[:lspace], key(name)]
      connection_adapter.with_connection do |conn|
        conn.exec("select pg_advisory_unlock($1,$2)", lock_args)
      end
    end

    def log(data, &blk)
      Log.log({:ns => "postgresql-lock"}.merge(data), &blk)
    end

  end
end
