require 'redis'
require 'thread'

class Redis
  module SlaveRead
    module Interface
      class Base
        attr_accessor :master, :slaves, :nodes, :read_master, :all

        class << self
          def slave(commands)
            commands.each do |method|
              define_method(method) do |*args|
                next_node.send(method, *args)
              end
            end
          end

          def master(commands)
            commands.each do |method|
              define_method(method) do |*args|
                master.send(method, *args)
              end
            end
          end

          def all(commands)
            commands.each do |method|
              define_method(method) do |*args|
                @all.each do |node|
                  node.send(method, *args)
                end
              end
            end
          end
        end

        def initialize(options = {})
          @block_exec_mutex = Mutex.new
          @round_robin_mutex = Mutex.new
          @master = options[:master] || raise("Must specify a master")
          @slaves = options[:slaves] || []
          @read_master = options[:read_master].nil? || options[:read_master]
          @all = slaves + [@master]
          @nodes = slaves.dup
          @nodes.unshift @master if @read_master
          @index = rand(@nodes.length)
        end

        def method_missing(method, *args, &block)
          if master.respond_to?(method)
            if slave_command?(method)
              define_singleton_method(method) do |*_args, &_block|
                next_node.send(method, *_args, &_block)
              end
            else
              define_singleton_method(method) do |*_args, &_block|
                @master.send(method, *_args, &_block)
              end
            end
            send(method, *args, &block)
          else
            super
          end
        end

        def respond_to_missing?(method_name, include_private = false)
          master.respond_to?(method_name, include_private) ||
            slaves.any? { |slave| slave.respond_to?(method_name, include_private) } ||
            super
        end


        def pipelined(*args, &block)
          @block_exec_mutex.synchronize do
            @locked_node = @master
            result = @master.send(:pipelined, *args, &block)
            @locked_node = nil
            result
          end
        end

        def exec(*args)
          @block_exec_mutex.synchronize do
            @locked_node = nil
            @master.send(:exec, *args)
          end
        end

        def discard(*args)
          @block_exec_mutex.synchronize do
            @locked_node = nil
            @master.send(:discard, *args)
            @locked_node = nil
          end
        end

        def multi(*args, &block)
          @block_exec_mutex.synchronize do
            @locked_node = @master
            replies = @master.send(:multi, *args, &block)
            @locked_node = nil
            replies
          end
        end

        private

        def next_node
          @round_robin_mutex.synchronize do
            return @master if @locked_node || @nodes.empty?

            @index = (@index + 1) % @nodes.length
            @nodes[@index]
          end
        end

        def slave_command?(method)
          Redis::SlaveRead::Interface::Hiredis::SLAVE_COMMANDS.include?(method.to_s)
        end
      end
    end
  end
end
