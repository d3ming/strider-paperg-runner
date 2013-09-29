
// A class for collecting and managing job data

var _ = require('lodash')

  , utils = require('./utils')
  , consts = require('./consts')

module.exports = JobData

function JobData(io) {
  this.jobs = {}
  this.io = io
  this.attach()
}

JobData.prototype = {
  // public api
  add: function (data) {
    var job = this.jobs[data._id] = _.cloneDeep(consts.skels.job)
    job.id = data._id
    job.data = data
    job.queued = new Date()
    for (var i=0; i<consts.phases.length; i++) {
      job.phases[consts.phases[i]] = _.cloneDeep(consts.skels.phase)
    }
    return job
  },
  get: function (id) {
    return this.jobs[id]
  },
  pop: function (id) {
    var job = this.jobs[id]
    if (!job) {
      throw new Error('Tried to pop a nonexistent job ' + id)
    }
    delete this.jobs[id]
    return job
  },

  // private api
  attach: function () {
    var self = this
    Object.keys(this.listeners).forEach(function (name) {
      self.io.on('job.status.' + name, function (id) {
        if (!self.jobs[id]) {
          console.warn('[simple-runner][jobdata] got a status update, but the job was never added', id, Object.keys(self.jobs))
          return
        }
        return self.listeners[name].apply(self.jobs[id], [].slice.call(arguments, 1))
      })
    })
  },
  // all listeners are called on the *job object* referred to by the
  // first id of the event
  listeners: {
    'phase.done': function (data) {
      this.phases[data.phase].finished = data.time
      this.phases[data.phase].exitCode = data.exitCode
      if (data.phase === 'test') this.test_status = data.code
      if (data.phase === 'deploy') this.deploy_status = data.code
      if (!data.next) return
      this.phase = data.next
    },
    'command.start': function (data) {
      var phase = this.phases[this.phase]
        , command = _.extend({}, consts.skels.command, data)
      phase.commands.push(command)
    },
    'command.done': function (exitCode, time, elapsed) {
      var phase = this.phases[this.phase]
        , command = phase.commands[phase.commands.length - 1]
      command.finished = time
      command.exitCode = exitCode
    },
    'stdout': function (text) {
      var command = utils.ensureCommand(this.phases[this.phase])
      command.out += text
      command.merged += text
      this.std.out += text
      this.std.merged += text
    },
    'stderr': function (text) {
      var command = utils.ensureCommand(this.phases[this.phase])
      command.err += text
      command.merged += text
      this.std.err += text
      this.std.merged += text
    },
  }
}