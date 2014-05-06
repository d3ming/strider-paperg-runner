
var _ = require('lodash')
  , path = require('path')
  , async = require('async')
  , Step = require('step')
  , utils = require('./utils')
  , Job = require('./job')
  , AWS = require('aws-sdk')
  , process = require('process');

module.exports = Worker

var NODE_RULE = {
  filename: 'package.json',
  exists: true,
  language: 'node.js',
  framework: null,
  prepare: 'npm install',
  test: 'npm test',
  start: 'npm start',
  path: path.join(__dirname, '../node_modules/npm/bin')
}

var AWS_CREDENTIAL = { accessKeyId: process.env.EC2_ACCESS_KEY, secretAccessKey: process.env.EC2_SECRET_KEY, region: 'us-east-1' };
var TASK_QUEUE_URL = process.env.TASK_QUEUE_URL;  // set this in the environment to enable durability
var TASK_POLL_INTERVAL_SEC = 5;       // retrieve messages from SQS every X seconds
var TASK_MONITOR_INTERVAL_SEC = 30;   // heartbeat to SQS every X seconds
var TASK_PENDING_TIMEOUT_SEC = 180;   // allow up to X seconds between hearbeats
var TASK_RECEIVE_LIMIT = 3;           // stop trying to process a message after X attempts
var TASK_RUNNABLE_LIMIT = 8;          // TODO: use this to throttle SQS polling and compute optimal batch size in receive

AWS.config.update(AWS_CREDENTIAL);

function Worker(config) {
  var dotStrider = path.join(process.env.HOME || '/', '.strider')
  this.config = _.extend({
    pty: true,
    logger: console,
    emitter: null,
    dataDir: process.env.STRIDER_CLONE_DEST || dotStrider
  }, config)
  this.log = this.config.logger.log
  this.debug = this.config.logger.debug
  this.error = this.config.logger.error
  this.queues = {}  // this is a map of repo domains to job queues, e.g. "master.placelocal.paperg": async.queue(this.processJob.bind(this), 1)
  this.rules = [NODE_RULE]
  this.hooks = []
  
  this.durable = TASK_QUEUE_URL? true : false;
  // if the queue endpoint is defined, then poll the corresponding SQS queue for tasks
  if (this.durable) {
    this.sqs = new AWS.SQS();
    this.monitor.handles = {};
    setInterval(this.receive.bind(this), TASK_POLL_INTERVAL_SEC * 1000);
    setInterval(this.monitor.bind(this), TASK_MONITOR_INTERVAL_SEC * 1000);    
  }
}

Worker.prototype = {
  attach: function (emitter) {
    var self = this;
    emitter.on('queue.new_job', function (task) {
      self.log('Got new task: job_id = %s', task.job_id);    
      if (self.durable) {
        // commit message to the durable SQS buffer
        self.send(task);
      } else {
        // no buffer, schedule the job right away
        self.schedule(task);
      }  
    })
  },
  
  // send message to SQS buffer
  send: function(task) {
    var self = this;
    self.sqs.sendMessage({ 'QueueUrl': TASK_QUEUE_URL, 'MessageBody': JSON.stringify(task) }, function(err, data) {
      if (err) {
        self.debug("send: error: %s, task: %s", err, task);
      } else {
        self.debug("send: success: MessageId = %s", data.MessageId);
      }
    });    
  },
  
  // receive message from SQS buffer
  receive: function() {
    var self = this;
    
    // TODO: it is a good idea to govern concurrency somehow, and to optimize batch size (MaxNumberOfMessages)
    self.sqs.receiveMessage({
        'QueueUrl': TASK_QUEUE_URL, 'WaitTimeSeconds': TASK_POLL_INTERVAL_SEC, 
        'VisibilityTimeout': TASK_PENDING_TIMEOUT_SEC, 'MaxNumberOfMessages': 1,
        'AttributeNames': ['ApproximateReceiveCount']
    }, function(err, data){
      if (err) {
        self.error("receive: SQS receive error: %s", err);
      } else {
        for (i in data.Messages) {
          var message = data.Messages[i];
          var handle = message.ReceiptHandle;
          var receiveCount = message.Attributes.ApproximateReceiveCount;
          self.debug("receiveMessage: MessageId = %s, ApproximateReceiveCount = %s", message.MessageId, receiveCount);
          
          var task = JSON.parse(message.Body);
          if (receiveCount > TASK_RECEIVE_LIMIT) {
            console.error("receiveMessage: SQS message exceeded receive limit, removing: MessageId = %s, task: %s", 
                message.MessageId, JSON.stringify(task));
                // TODO: send the message we are about to delete to a recycle queue
                self.sqs.deleteMessage({'QueueUrl': TASK_QUEUE_URL, 'ReceiptHandle': handle}, function(err, data){
                  if (err) {
                    self.error("receiveMessage: SQS delete error: job_id = %s, message id = %s, err = %s", task.job_id, message.MessageId, err);
                    // not a whole lot else we can do here, the task will run again
                  } else {
                    self.debug("receiveMessage: SQS delete success: job_id = %s, message id = %s", task.job_id, message.MessageId);
                  }
                })    
          } else {
            self.monitor.handles[task.job_id] = handle;
            self.schedule(task, { 'handle': handle });
          }
        }
      }
    });
  },
  
  monitor: function() {
    var self = this;
    
    // gather all the message handles waiting in scheduler queues
    var handles = [];
    for (k in self.monitor.handles) {
      var handle = [k,self.monitor.handles[k]];
      handles.push(handle);
    }
  
    reqs = [];
    // send a batch request to keep the messages hidden
    while (handles.length) {
      // update works for up to 10 messages
      var batch = handles.slice(0,10);
      handles = handles.slice(10);
      
      reqs.push(function(batch){
        var request = { 'QueueUrl': TASK_QUEUE_URL, 'Entries': batch.map(function(handle, index){
          return { 'Id': 'handle' + handle[0], 'ReceiptHandle': handle[1], 'VisibilityTimeout': TASK_PENDING_TIMEOUT_SEC }
        })};
        return function(next) {
          self.sqs.changeMessageVisibilityBatch(request, function(err, data) {
            if (err) {
              self.error('monitor: SQS update visibility error: %s', err);
            } else {
              self.debug('monitor: visibility updated: %s handles, jobs: %j', batch.length, batch.map(function(handle){ return handle[0]; }));
            }
            next(err, batch);
          });
        };
      }(batch));
    }
    async.parallel(reqs, function(err, results) {
      if (err) {
        self.error('monitor: async execution error: %j', err);
      } else {
        self.debug('monitor: async execution completed, batches = %s', results.length);
      }
    });
  },
  
  schedule: function(task, props) {
    var self = this;
    
    var domain = utils.domainForGithubRepoBranch(task.repo_ssh_url, task.github_commit_info.branch);
    self.debug('schedule: domain = %s, job_id = %s', domain, task.job_id);  // e.g. master.placelocal.paperg
  
    var queue = self.queues[domain];
    if (!queue) {
      queue = self.queues[domain] = async.queue(self.processJob.bind(self), 1);
    }
    
    queue.push({'task': task, 'props': props});
    
    // update scheduler stats
    var stats = self.stats = { queues: 0, total: 0, runnable: 0, running: 0 };
    for (k in self.queues) {
      var queue = self.queues[k];
      if (queue) {
        var l = queue.length();
        stats.queues += 1;
        stats.total += l;
        stats.runnable += l > 0? 1 : 0;
        stats.running += queue.running();
      }
    }
    console.debug("schedule: queue stats: queues = %s, tasks = %s, runnable = %s, running = %s", stats.queues, stats.total, stats.runnable, stats.running);
  },
  
  buildContext: function (config, extdir) {
    var self = this
    return {
      addDetectionRules: function (rules) {
        self.rules = self.rules.concat(rules)
      },
      addDetectionRule: function (rule) {
        self.rules.push(rule)
      },
      addBuildHooks: function (hooks) {
        self.hooks = self.hooks.concat(hooks)
      },
      addBuildHook: function (hook) {
        self.hooks.push(hook)
      },
      config: config,
      extdir: extdir,
      npmCmd: 'npm'
    }
  },
  processJob: function (data, next) {
    var self = this;
    var task = data.task;
    var props = data.props;
    var io;
    
    self.debug('processJob begin: job_id = %s', task.job_id);
    
    var work = function () {
      var shortname = task.repo_config.url.split('/').slice(-2).join('/');
      var dirname = task.user_id + '.' + shortname.replace('/', '.');
      var branch = task.github_commit_info && task.github_commit_info.branch || "master";
      var dir = path.join(self.config.dataDir, dirname, branch);
      var job = new Job(task, dir, self.config.emitter, { logger: self.config.logger, pty: self.config.pty });
        
      io = job.io;
      job.start();
          
      var done = function (err, tasks) {
        // err may be an exception or a status code
        var code = err;
        if (typeof err != 'number') {
          io.emit('error', err)
          code = -1;
        }
        
        commit(err, function() {
          job.complete(code, null, tasks, next);
          self.debug('processJob done: job_id = %s, code = %s', task.job_id, code);
        });
      }

      Step(
        function() {
          job.gitStep(done, this)
        },
        function(err, context) {
          this.context = context
          utils.collectPhases(done, context, self.rules, this)
        },
        function(err, result, results) {
          utils.processPhases(this, self.hooks, this.context, results)
        },
        function (err, tasks) {
          if (err) {
            return done(err, tasks)
          }
          done(0, tasks)
        }
      )
    }
    
    var commit = function(err, next) {
      if (self.durable) {
        // if success, commit completion of the job to SQS TODO: optimistic, what if this fails?
        self.sqs.deleteMessage({'QueueUrl': TASK_QUEUE_URL, 'ReceiptHandle': props.handle}, function(err, data){
          if (err) {
            self.error("processJob error: SQS delete message: job_id = %s, err = %s", task.job_id, err);
            // not a whole lot else we can do here, the task will run again
          } else {
            self.debug("processJob: SQS delete success: job_id = %s, data = %j", task.job_id, data);
          }
          // remove sqs handle from the map of monitored handles
          // TODO: if tasks are running concurrently via async lib, this kind of thing may be an issue
          delete self.monitor.handles[task.job_id];
          next();
        })
      } else {
        // commit is a noop when durability is off
        next();
      }
    }
    
    var domain = require('domain').create()    
    domain.on('error', function (err) {
      self.error('processJob: domain error: job_id = %s: ', task.job_id, err.stack);
      if (io) io.emit('error', err)
      commit(err, next)
    })
      
    domain.run(work);
  }
}
