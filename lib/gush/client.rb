module Gush
  class Client
    attr_reader :configuration

    def initialize(config = Gush.configuration)
      ap config
      @configuration = config
      @sidekiq = build_sidekiq
    end

    def configure
      yield configuration
      @sidekiq = build_sidekiq
    end

    def create_workflow(name)
      begin
        flow = name.constantize.create
      rescue NameError
        raise WorkflowNotFound.new("Workflow with given name doesn't exist")
      end
      flow
    end

    def start_workflow(workflow, job_names = [])
      workflow.mark_as_started
      persist_workflow(workflow)
      
      workflow_report({
        workflow_id:  workflow.id,
        status:       workflow.status,
        started_at:   workflow.started_at,
        finished_at:  workflow.finished_at
      })

      jobs = if job_names.empty?
               workflow.initial_jobs
             else
               job_names.map {|name| workflow.find_job(name) }
             end

      jobs.each do |job|
        enqueue_job(workflow.id, job)
      end
    end

    def stop_workflow(id)
      workflow = find_workflow(id)
      workflow.mark_as_stopped
      persist_workflow(workflow)
    end

    def next_free_job_id(workflow_id,job_klass)
      job_identifier = nil
      loop do
        id = SecureRandom.uuid
        job_identifier = "#{job_klass}-#{id}"
        
        break if !redis.exists("gush.jobs.#{workflow_id}.#{job_identifier}")
      end

      job_identifier
    end

    def next_free_workflow_id
      id = nil
      loop do
        id = SecureRandom.uuid
        break if !redis.exists("gush.workflow.#{id}")
      end

      id
    end

    def all_workflows
      redis.keys("gush.workflows.*").map do |key|
        id = key.sub("gush.workflows.", "")
        find_workflow(id)
      end
    end

    def find_workflow(id)
      data = redis.get("gush.workflows.#{id}")
      unless data.nil?
        hash = Gush::JSON.decode(data, symbolize_keys: true)
        keys = redis.keys("gush.jobs.#{id}.*")
        nodes = redis.mget(*keys).map { |json| Gush::JSON.decode(json, symbolize_keys: true) }
        workflow_from_hash(hash, nodes)
      else
        raise WorkflowNotFound.new("Workflow with given id doesn't exist")
      end
    end

    def persist_workflow(workflow, persist_jobs = true)
      redis.set("gush.workflows.#{workflow.id}", workflow.to_json)
      workflow.jobs.each {|job| persist_job(workflow.id, job) } if persist_jobs
      workflow.mark_as_persisted
      true
    end
    
    def reload_workflow_jobs(id)
      jobs = []
      data = redis.get("gush.workflows.#{id}")
      unless data.nil?
        hash = Gush::JSON.decode(data, symbolize_keys: true)
        keys = redis.keys("gush.jobs.#{id}.*")
        nodes = redis.mget(*keys).map { |json| Gush::JSON.decode(json, symbolize_keys: true) }
        nodes.each do |node|
          jobs << Gush::Job.from_hash(hash, node)
        end
        return jobs
      else
        raise WorkflowNotFound.new("Workflow with given id doesn't exist")
      end
    end
    

    def persist_job(workflow_id, job)
      redis.set("gush.jobs.#{workflow_id}.#{job.name}", job.to_json)
    end

    def load_job(workflow_id, job_id)
      workflow = find_workflow(workflow_id)
      job_name_match = /(?<klass>\w*[^-])-(?<identifier>.*)/.match(job_id)
      hypen = '-' if job_name_match.nil?

      keys = redis.keys("gush.jobs.#{workflow_id}.#{job_id}#{hypen}*")
      return nil if keys.nil?

      data = redis.get(keys.first)
      return nil if data.nil?

      data = Gush::JSON.decode(data, symbolize_keys: true)
      Gush::Job.from_hash(workflow, data)
    end

    def destroy_workflow(workflow)
      redis.del("gush.workflows.#{workflow.id}")
      workflow.jobs.each {|job| destroy_job(workflow.id, job) }
    end

    def destroy_job(workflow_id, job)
      redis.del("gush.jobs.#{workflow_id}.#{job.name}")
    end

    def worker_report(message)
      report("gush.workers.status", message)
    end

    def workflow_report(message)
      report("gush.workflows.status", message)
    end

    def enqueue_job(workflow_id, job)
      job.enqueue!
      persist_job(workflow_id, job)
      
      workflow = find_workflow(workflow_id)
      
      sidekiq.push(
        'class' => Gush::Worker,
        'queue' => configuration.namespace,
        'retry' => workflow.retry_jobs,
        'args'  => [workflow_id, job.name]
      )
    end

    private

    attr_reader :sidekiq, :redis

    def workflow_from_hash(hash, nodes = nil)
      flow = hash[:klass].constantize.new *hash[:arguments]
      flow.jobs = []
      flow.stopped = hash.fetch(:stopped, false)
      flow.id = hash[:id]

      (nodes || hash[:nodes]).each do |node|
        flow.jobs << Gush::Job.from_hash(flow, node)
      end

      flow
    end

    def report(key, message)
      redis.publish(key, Gush::JSON.encode(message))
    end


    def build_sidekiq
      Sidekiq::Client.new()
    end
    
    def redis
      ap configuration
      @redis ||= Redis.new(url: configuration.redis_url)
    end

    def build_redis_pool
      Redis.new(url: configuration.redis_url)
    end

    def connection_pool
      @connection_pool ||= ConnectionPool.new(size: configuration.concurrency, timeout: 1) { build_redis_pool }
    end
  end
end
