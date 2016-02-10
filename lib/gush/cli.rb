require 'terminal-table'
require 'colorize'
require 'thor'
require 'launchy'
require 'sidekiq'
require 'sidekiq/api'

module Gush
  class CLI < Thor
    class_option :endpoint, desc: "configuration file to use or Rails 3/4 app Root folder", aliases: "-f"
    class_option :concurrency, desc: "concurrency setting for Sidekiq", aliases: "-c"
    class_option :redis, desc: "Redis URL to use", aliases: "-r"
    class_option :namespace, desc: "namespace to run jobs in", aliases: "-n"
    class_option :environment, desc: "Sidekiq environment", aliases: "-e"

    def initialize(*)
      super
      Gush.configure do |config|
        config.endpoint    = options.fetch("endpoint",    config.endpoint)
        config.concurrency = options.fetch("concurrency", config.concurrency)
        config.redis_url   = options.fetch("redis",       config.redis_url)
        config.namespace   = options.fetch("namespace",   config.namespace)
        config.environment = options.fetch("environment", config.environment)
      end
      load_endpoint
    end

    desc "create [WorkflowClass]", "Registers new workflow"
    def create(name)
      workflow = client.create_workflow(name)
      puts "Workflow created with id: #{workflow.id}"
      puts "Start it with command: gush start #{workflow.id}"
    end

    desc "start [workflow_id]", "Starts Workflow with given ID"
    def start(*args)
      id = args.shift
      workflow = client.find_workflow(id)
      client.start_workflow(workflow, args)
    end

    desc "create_and_start [WorkflowClass]", "Create and instantly start the new workflow"
    def create_and_start(name, *args)
      workflow = client.create_workflow(name)
      client.start_workflow(workflow.id, args)
      puts "Created and started workflow with id: #{workflow.id}"
    end

    desc "stop [workflow_id]", "Stops Workflow with given ID"
    def stop(*args)
      id = args.shift
      client.stop_workflow(id)
    end

    desc "clear", "Clears all jobs from Sidekiq queue"
    def clear
      Sidekiq::Queue.new(client.configuration.namespace).clear
    end

    desc "show [workflow_id]", "Shows details about workflow with given ID"
    option :skip_overview, type: :boolean
    option :skip_jobs, type: :boolean
    option :jobs, default: :all
    def show(workflow_id)
      workflow = client.find_workflow(workflow_id)

      display_overview_for(workflow) unless options[:skip_overview]

      display_jobs_list_for(workflow, options[:jobs]) unless options[:skip_jobs]
    end

    desc "rm [workflow_id]", "Delete workflow with given ID"
    def rm(workflow_id)
      workflow = client.find_workflow(workflow_id)
      client.destroy_workflow(workflow)
    end

    desc "list", "Lists all workflows with their statuses"
    def list
      workflows = client.all_workflows
      rows = workflows.map do |workflow|
        [workflow.id, workflow.class, {alignment: :center, value: status_for(workflow)}]
      end
      headers = [
        {alignment: :center, value: 'id'},
        {alignment: :center, value: 'name'},
        {alignment: :center, value: 'status'}
      ]
      puts Terminal::Table.new(headings: headers, rows: rows)
    end

    desc "workers", "Starts Sidekiq workers"
    def workers
      config = client.configuration
      Kernel.exec "bundle exec sidekiq -r #{config.endpoint} -c #{config.concurrency} -q #{config.namespace} -e #{config.environment} -v"
    end

    desc "viz [WorkflowClass]", "Displays graph, visualising job dependencies"
    def viz(name)
      client
      workflow = name.constantize.new
      graph = Graph.new(workflow)
      graph.viz
      Launchy.open graph.path
    end

    private

    def client
      @client ||= Client.new
    end

    def overview(workflow)
      CLI::Overview.new(workflow)
    end

    def display_overview_for(workflow)
      puts overview(workflow).table
    end

    def status_for(workflow)
      overview(workflow).status
    end

    def display_jobs_list_for(workflow, jobs)
      puts overview(workflow).jobs_list(jobs)
    end

    def gushfile
      Gush.configuration.gushfile
    end
    
    def preload_rails_app
      if File.exist?("./config/application.rb") && File.exist?("./config/environment.rb")
        if ::Rails::VERSION::MAJOR < 4
          require File.expand_path("./config/environment.rb")
          ::Rails.application.eager_load!
        else
          # Painful contortions, see sidekiq#1791 for discussion
          require File.expand_path("./config/application.rb")
          ::Rails::Application.initializer "gush.eager_load" do
            ::Rails.application.config.eager_load = true
          end
          require File.expand_path("./config/environment.rb")
        end
      end
    end

    def load_endpoint
      endpoint = client.configuration.endpoint
      if !endpoint.exist?
        raise Thor::Error, "#{endpoint} not found, please add it to your project or make sure it points to the root dir of a Rails 3/4 app".colorize(:red)
      end
      
      require endpoint if !File.directory?(endpoint) && File.exist?(endpoint)
      
    rescue LoadError
      raise Thor::Error, "failed to require #{endpoint}".colorize(:red)
    end
    
    
    def load_rails_app
    end
    
  end
end
