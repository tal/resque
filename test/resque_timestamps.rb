require File.dirname(__FILE__) + '/test_helper'

context "Resque Timestamps" do
  setup do
    Resque.redis.flushall
    
    @hour = 3600
    @day = @hour * 24
  end
  
  test "can put jobs on a queue by way of an ivar" do
    assert_equal 0, Resque.size(:time__ts)
    assert Resque.run_at(Time.now-@hour, TimeStamped, 20)
    assert Resque.run_at(Time.now-@hour*2, TimeStamped, 10)
    assert Resque.run_at(Time.now+@hour, TimeStamped, 30)
    
    job = Resque.reserve(:time__ts)
    
    assert_kind_of Resque::Job, job
    assert_equal TimeStamped, job.payload_class
    assert_equal 10, job.args[0]
    
    assert Resque.reserve(:time__ts)
    assert_equal nil, Resque.reserve(:time__ts)
    assert_equal 1, Resque.dequeue(TimeStamped)
  end
  
  test "work with workers" do
    Resque.redis.flushall
    @worker = Resque::Worker.new(:time__ts)
    Resque.run_at(Time.now-@hour, TimeStamped, 20)
    Resque.run_at(Time.now+@hour, TimeStamped, 30)
    Resque.run_at(Time.now+1, TimeStamped, 40)
    
    assert_equal 3, Resque.size(:time__ts)
    @worker.work(0)
    assert_equal 2, Resque.size(:time__ts)
    sleep(1)
    @worker.work(0)
    assert_equal 1, Resque.size(:time__ts)
  end
  
end
