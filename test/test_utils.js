
var expect = require('chai').expect
  , utils = require('../lib/utils')
  , cmd = require('../lib/cmd')

describe('utils', function () {
  describe('.getUrls', function () {
    var repo = 'git@github.com:Strider-CD/strider.git'
      , https_end = 'github.com/Strider-CD/strider.git'
      , api_key = '123456asd345'
    it('should no-op if using an ssh key', function () {
      expect(utils.getUrls(repo, 'asd', api_key)).to.eql([repo, repo])
    })

    it('should make a screen-friendly version without the api key', function () {
      expect(utils.getUrls(repo, false, api_key)).to.eql([
        'https://' + api_key + ':@' + https_end,
        'https://[github oauth key]@' + https_end
      ])
    })

    it('should just give the https urls if no api and no ssh', function () {
      expect(utils.getUrls(repo, false, false)).to.eql([
        'https://' + https_end,
        'https://' + https_end
      ])
    })
  })
  describe('.processDetectionRules', function() {

    it('should ignore filenames which are already strings', function(done) {

      var ctx = {
        data: {someData:true}
      }
      var rules = [{
        filename:"foo.js",
        exists:true
      }, {
        filename:"foo2.js",
        exists:true
      }]

      utils.processDetectionRules(rules, ctx, function(err, results) {
        expect(err).to.be.null
        expect(results).to.have.length(2)
        expect(results).to.contain(rules[0])
        expect(results).to.contain(rules[1])
        done()
      })

    })

    it('should process filename function types', function(done) {

      var ctx = {
        data: {someData:true}
      }
      var rules = [
        {
          filename:"foo.js",
          exists:true
        },
        {
          filename:"foo2.js",
          exists:true
        },
        {
          filename:function(tctx, cb) {
            expect(tctx.data).to.exist
            expect(tctx.data.someData).to.eql(ctx.data.someData)
            cb(null, "foo3.js")
          },
          exists: true
        }
      ]

      utils.processDetectionRules(rules, ctx, function(err, results) {
        expect(err).to.be.null
        expect(results).to.have.length(3)
        expect(results).to.contain(rules[0])
        expect(results).to.contain(rules[1])
        expect(results[2]).to.eql({exists:true, filename:"foo3.js"})
        done()
      })
    })

    it('should handle errors', function(done) {

      var ctx = {
        data: {someData:true}
      }
      var rules = [
        {
          filename:"foo.js",
          exists:true
        },
        {
          filename:"foo2.js",
          exists:true
        },
        {
          filename:function(tctx, cb) {
            cb("problem!", null)
          },
          exists: true
        }
      ]

      utils.processDetectionRules(rules, ctx, function(err, results) {
        expect(err).to.exist
        expect(err).to.eql('problem!')
        expect(results).to.be.null
        done()
      })
    })
  }) // end .processDetectionRules

  describe('.getHookFn', function () {
    it('should no-op when no hook is found', function (done) {
      // is there a better way to validate this?
      utils.getHookFn(undefined)({}, function (code) {
        expect(code).to.equal(0)
        done()
      })
    })

    it('should make a forkProc hook if a string is found', function (done) {
      var callback = function (code){
            expect(code).to.equal(0)
            done()
          }
        , command = 'make test'
        , wrapped = cmd.shellWrap(command)
        , cwd = '/'
      utils.getHookFn(command)({
        workingDir: cwd,
        shellWrap: cmd.shellWrap,
        forkProc: function (dir, command, args, next) {
          expect(dir).to.equal(cwd)
          expect(command).to.equal(wrapped.cmd)
          expect(args).to.eql(wrapped.args)
          next(0)
        }
      }, callback)
    })

    it('should call a function transparently', function (done) {
      var ctx = {}
        , next = function () {}
      utils.getHookFn(function (context, cb) {
        expect(context).to.equal(ctx)
        expect(cb).to.equal(next)
        done()
      })(ctx, next)
    })

    it('should return false if an illegal hook (not a string or function) is passed', function () {
      expect(utils.getHookFn({})).to.be.false
    })
  })

  // TODO: add tests about tasks. What are they for?
  describe('.makeHook', function () {

    it('should return false if a bad hook is given', function () {
      expect(utils.makeHook(null, {'test': {}}, 'test')).to.be.false
    })

    describe('when given a normal hook', function () {
      var test = function (context, next) {
        if (context.fail) return next(1)
        next(0)
      }
      it('should pass correctly', function (done) {
        var hook = utils.makeHook({fail: false}, {'test': test}, 'test')
        expect(hook).to.be.a.function
        hook(function (err, data) {
          expect(err).to.not.be.ok
          expect(data.phase).to.equal('test')
          expect(data.code).to.equal(0)
          done()
        })
      })
      it('should fail correctly', function (done) {
        var hook = utils.makeHook({fail: true}, {'test': test}, 'test')
        expect(hook).to.be.a.function
        hook(function (err, data) {
          expect(err).to.be.ok
          expect(err.phase).to.equal('test')
          expect(err.code).to.equal(1)
          done()
        })
      })
      it('should pass a failing cleanup', function (done) {
        var hook = utils.makeHook({fail: true}, {'cleanup': test}, 'cleanup')
        expect(hook).to.be.a.function
        hook(function (err, data) {
          expect(err).to.not.be.ok
          expect(data.phase).to.equal('cleanup')
          expect(data.code).to.equal(1)
          done()
        })
      })
    })
  })
  
  describe('.normalizeDomain', function () {
    it('should not modify correct domain', function (done) {
      var domain = 'host1.sub-1.domain.org';
      expect(utils.normalizeDomain(domain)).to.equal(domain);
      domain = 'domain';
      expect(utils.normalizeDomain(domain)).to.equal(domain);
      done();
    })
    
    it('should replace illegal characters', function (done) {
      expect(utils.normalizeDomain('bad!host.bad_domain.org')).to.equal('bad-host.bad-domain.org');
      expect(utils.normalizeDomain('???bad!!!!host@#*!$.____bad()domain&^&$.org')).to.equal('bad----host.bad--domain.org');
      expect(utils.normalizeDomain('bad!domain')).to.equal('bad-domain');
      expect(utils.normalizeDomain('1domain')).to.equal('domain');
      done();
    })
    
    it('should remove leading digits', function (done) {
      expect(utils.normalizeDomain('1host.1domain.org')).to.equal('host.domain.org');
      expect(utils.normalizeDomain('12345host123.12345domain1.org')).to.equal('host123.domain1.org');
      done();
    })    
    
    it('should remove leading and tailing hyphens', function (done) {
      expect(utils.normalizeDomain('-host-.-domain-.org')).to.equal('host.domain.org');
      expect(utils.normalizeDomain('---host-1---.---domain123-----.-org-')).to.equal('host-1.domain123.org');
      done();
    })
    
    it('should riase an error when paramters are illegal', function (done) {
      expect(function(){ utils.normalizeDomain(null); }).to.throw(Error, /domain is empty/);
      expect(function(){ utils.normalizeDomain(undefined) }).to.throw(Error, /domain is empty/);
      expect(function(){ utils.normalizeDomain('') }).to.throw(Error, /domain is empty/);
      expect(function(){ utils.normalizeDomain(123) }).to.throw(Error, /domain is not a string/);
      expect(function(){ utils.normalizeDomain({}) }).to.throw(Error, /domain is not a string/);
      done();
    })
  })
  
  describe('.domainForGithubRepoBranch', function () {
    it('should generate correct domain', function (done) {
      var repo_ssh_url = 'git@github.com:vasili-zolotov/strider-simple-runner.git';
      expect(utils.domainForGithubRepoBranch(repo_ssh_url,'my-branch')).to.equal('my-branch.vasili-zolotov.strider-simple-runner');
      expect(utils.domainForGithubRepoBranch(repo_ssh_url,'Branch_1')).to.equal('branch-1.vasili-zolotov.strider-simple-runner');
      expect(utils.domainForGithubRepoBranch(repo_ssh_url,'1.0.0')).to.equal('v1-0-0.vasili-zolotov.strider-simple-runner');
      done();
    })
    
    it('should riase an error when paramters are illegal', function (done) {
      var repo_ssh_url = 'git@github.com:vasili-zolotov/strider-simple-runner.git';
      var repo_https_url = 'https://github.com/vasili-zolotov/strider-simple-runner.git';
      expect(function(){ utils.domainForGithubRepoBranch(null, 'branch'); }).to.throw(Error, /repo_ssh_url is empty/);
      expect(function(){ utils.domainForGithubRepoBranch(repo_ssh_url, null) }).to.throw(Error, /branch is empty/);
      expect(function(){ utils.domainForGithubRepoBranch('', '') }).to.throw(Error, /repo_ssh_url is empty|branch is empty/);
      expect(function(){ utils.domainForGithubRepoBranch(123, 'branch1') }).to.throw(Error, /repo_ssh_url is not a string/);
      expect(function(){ utils.domainForGithubRepoBranch(123, null) }).to.throw(Error, /repo_ssh_url is not a string|branch is empty/);
      expect(function(){ utils.domainForGithubRepoBranch(undefined, {}) }).to.throw(Error, /repo_ssh_url is empty|branch is not a string/);
      expect(function(){ utils.domainForGithubRepoBranch(123, new Date()) }).to.throw(Error, /repo_ssh_url is not a string|branch is not a string/);
      expect(function(){ utils.domainForGithubRepoBranch(repo_https_url, 'branch1') }).to.throw(Error, /repo_ssh_url is not correct/);
      done();
    })
  })
  
})
