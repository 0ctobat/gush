require 'securerandom'

module Gush
  class Workflow
    attr_accessor :id, :jobs, :stopped, :persisted, :arguments, :retry_jobs

    def initialize(*args)
      @id = id
      @jobs = []
      @retry_jobs = false
      @dependencies = []
      @persisted = false
      @stopped = false
      @arguments = args

      setup
    end

    def self.find(id)
      Gush::Client.new.find_workflow(id)
    end

    def self.create(*args)
      flow = new(*args)
      flow.save
      flow
    end

    def continue
      client = Gush::Client.new
      failed_jobs = jobs.select(&:failed?)

      failed_jobs.each do |job|
        client.enqueue_job(id, job)
      end
    end
    
    
    def skip_failed_job(job_name)
      client = Gush::Client.new
      
      job = find_job(job_name)
      return if job.nil? || !job.failed?
      
      job.finish!
      client.persist_job(self.id, job)
      
      message = {
        status: :finished,
        workflow_id: self.id,
        job: job_name,
        duration: 0
      }
      client.worker_report(message)
      
      reload_jobs
      client.persist_workflow(self, false)
      
      client.workflow_report({
        workflow_id:  self.id,
        status:       self.status,
        started_at:   self.started_at,
        finished_at:  self.finished_at
      })
      
      job.outgoing.each do |job_name|
        out = client.load_job(self.id, job_name)
        if out.ready_to_start?
          client.enqueue_job(self.id, out)
        end
      end
    end

    def save
      persist!
    end

    def configure(*args)
    end

    def mark_as_stopped
      @stopped = true
    end

    def start!
      client.start_workflow(self)
    end

    def persist!
      client.persist_workflow(self)
    end

    def mark_as_persisted
      @persisted = true
    end

    def mark_as_started
      @stopped = false
    end

    def resolve_dependencies
      @dependencies.each do |dependency|
        from = find_job(dependency[:from])
        to   = find_job(dependency[:to])

        to.incoming << dependency[:from]
        from.outgoing << dependency[:to]
      end
    end

    def find_job(name)
      match_data = /(?<klass>\w*[^-])-(?<identifier>.*)/.match(name.to_s)
      if match_data.nil?
        job = jobs.find { |node| node.class.to_s == name.to_s }
      else
        job = jobs.find { |node| node.name.to_s == name.to_s }
      end
      job
    end

    def finished?
      jobs.all?(&:finished?)
    end

    def started?
      !!started_at
    end

    def running?
      started? && !finished?
    end

    def failed?
      jobs.any?(&:failed?)
    end

    def stopped?
      stopped
    end

    def run(klass, opts = {})
      options =

      node = klass.new(self, {
        name: client.next_free_job_id(id,klass.to_s),
        params: opts.fetch(:params, {})
      })

      jobs << node

      deps_after = [*opts[:after]]
      deps_after.each do |dep|
        @dependencies << {from: dep.to_s, to: node.name.to_s }
      end

      deps_before = [*opts[:before]]
      deps_before.each do |dep|
        @dependencies << {from: node.name.to_s, to: dep.to_s }
      end
      
      node.name
    end
    
    def sidekiq_retry_jobs(n)
      @retry_jobs = n
    end
    
    def reload
      self.class.find(id)
    end

    def initial_jobs
      jobs.select(&:has_no_dependencies?)
    end

    def status
      case
        when failed?
          :failed
        when running?
          :running
        when finished?
          :finished
        when stopped?
          :stopped
        else
          :running
      end
    end

    def started_at
      first_job ? first_job.started_at : nil
    end

    def finished_at
      last_job ? last_job.finished_at : nil
    end

    def to_hash
      name = self.class.to_s
      {
        name: name,
        id: id,
        arguments: @arguments,
        total: jobs.count,
        finished: jobs.count(&:finished?),
        klass: name,
        jobs: jobs.map(&:as_json),
        status: status,
        stopped: stopped,
        started_at: started_at,
        finished_at: finished_at
      }
    end

    def to_json(options = {})
      Gush::JSON.encode(to_hash)
    end

    def self.descendants
      ObjectSpace.each_object(Class).select { |klass| klass < self }
    end

    def id
      @id ||= client.next_free_workflow_id
    end
    
    def reload_jobs
      @jobs = client.reload_workflow_jobs(id)
    end

    private

    def setup
      configure(*@arguments)
      resolve_dependencies
    end

    def client
      @client ||= Client.new
    end

    def first_job
      jobs.min_by{ |n| n.started_at || Time.now.to_i }
    end

    def last_job
      jobs.max_by{ |n| n.finished_at || 0 } if finished?
    end
  end
end
