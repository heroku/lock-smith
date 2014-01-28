require 'timeout'
require 'thread'
require 'locksmith/config'

module Locksmith
  module Dynamodb
    extend self
    # s safe for threads. This module i
    @dynamo_lock = Mutex.new
    @table_lock = Mutex.new

    def lock(name, opts={})
      opts[:ttl] ||= 60
      opts[:attempts] ||= 3
      # Clean up expired locks. Does not grantee that we will
      # be able to acquire the lock, just a nice thing to do for
      # the other processes attempting to lock.
      delete(name) if expired?(name, opts[:ttl])
      if create(name, opts[:attempts])
        begin Timeout::timeout(opts[:ttl]) {return(yield)}
        ensure delete(name)
        end
      else
        raise Locksmith::UnableToLock
      end
    end

    def create(name, attempts)
      attempts.times do |i|
        begin
          locks.put({"Name" => name, "Created" => Time.now.to_i},
            :unless_exists => "Name")
          return(true)
        rescue AWS::DynamoDB::Errors::ConditionalCheckFailedException
          return(false) if i == (attempts - 1)
        end
      end
    end

    def delete(name)
      locks.at(name).delete
    end

    def expired?(name, ttl)
      if l = locks.at(name).attributes.to_h(:consistent_read => true)
        if t = l["Created"]
          t < (Time.now.to_i - ttl)
        end
      end
    end

    def locks
      table(lock_table)
    end

    def table(name)
      unless tables[name]
        @table_lock.synchronize do
          tables[name] = dynamo.tables[name].load_schema
        end
      end
      tables[name].items
    end

    def tables
      @tables ||= {}
    end

    def dynamo
      @dynamo_lock.synchronize do
        @db ||= AWS::DynamoDB.new(:access_key_id => Config.aws_id,
                                  :secret_access_key => Config.aws_secret)
      end
    end

    def lock_table
      @lock_table
    end

    def lock_table=(table_name)
      @lock_table = table_name
    end

  end
end
