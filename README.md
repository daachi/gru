# Gru

Manage worker/minion counts in an atomic fashion.


## Installation

Add this line to your application's Gemfile:

```ruby
gem 'gru'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install gru

## Usage

    require 'gru'
    require 'redis'
    require 'logger'

    client = Redis.new

    class Gru::Adapters::RedisAdapter
      def hostname
        @hostname ||= ARGV[0]
      end
    end

    workers = { 'test_worker' => 5 }
    global = { 'test_worker' => 10 }
    manager = Gru.with_redis_connection(client,workers,global,true)
    manager.register_worker_queues
    logger = Logger.new(STDOUT)

    loop do
      logger.info("STATE: #{manager.adjust_workers.inspect}")
      sleep(1)
    end

More to come soon!

## Contributing

1. Fork it ( https://github.com/[my-github-username]/gru/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
