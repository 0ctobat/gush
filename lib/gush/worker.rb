require 'sidekiq'
require 'yajl'

module Gush
  class Worker
    include ::Sidekiq::Worker

    def perform(workflow_id, job_id)
      setup_job(workflow_id, job_id)

      job.payloads_hash = incoming_payloads
      
      start = Time.now
      report(:started, start)

      failed = false
      error = nil
      
      report(:requeued, start) if job.failed?
      mark_as_requeued if job.failed?
      
      mark_as_started
      begin
        job.work
      rescue Exception => error
        mark_as_failed
        report(:failed, start, error.message)
                
        reload_workflow_jobs
        check_and_report_workflow_status
        
        raise error
      else
        mark_as_finished
        report(:finished, start)
        
        reload_workflow_jobs
        check_and_report_workflow_status if @workflow.finished?

        enqueue_outgoing_jobs
      end
    end

    private
    attr_reader :client, :workflow, :job

    def client
      @client ||= Gush::Client.new(Gush.configuration)
    end

    def setup_job(workflow_id, job_id)
      @workflow ||= client.find_workflow(workflow_id)
      @job ||= workflow.find_job(job_id)
    end

    def incoming_payloads
      payloads = {}
      job.incoming.each do |job_name|
       job = client.load_job(workflow.id, job_name)
       payloads[job.klass.to_s] ||= []
       payloads[job.klass.to_s] << {:id => job.name, :payload => job.output_payload}
      end
      payloads
    end
    
    def mark_as_requeued
      job.requeue!
      client.persist_job(workflow.id, job)
      reload_workflow_jobs
      check_and_report_workflow_status
    end

    def mark_as_finished
      job.finish!
      client.persist_job(workflow.id, job)
    end

    def mark_as_failed
      job.fail!
      client.persist_job(workflow.id, job)
    end

    def mark_as_started
      job.start!
      client.persist_job(workflow.id, job)
    end

    def report_workflow_status
      client.workflow_report({
        workflow_id:  workflow.id,
        status:       workflow.status,
        started_at:   workflow.started_at,
        finished_at:  workflow.finished_at
      })
    end

    def report(status, start, error = nil)
      message = {
        status: status,
        workflow_id: workflow.id,
        job: job.name,
        duration: elapsed(start)
      }
      message[:error] = error if error
      client.worker_report(message)
    end

    def elapsed(start)
      (Time.now - start).to_f.round(3)
    end
    
    
    def reload_workflow_jobs
      @workflow.reload_jobs
    end
    
    
    def check_and_report_workflow_status
      client.persist_workflow(@workflow, false)
      report_workflow_status
    end

    def enqueue_outgoing_jobs
      job.outgoing.each do |job_name|
        out = client.load_job(workflow.id, job_name)
        if out.ready_to_start?
          client.enqueue_job(workflow.id, out)
        end
      end
    end
  end
end
