package runner

import (
	"context"
	"io"
	"os"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/iron-io/functions/api/runner/protocol"
	"github.com/iron-io/runner/drivers"
)

// TaskRequest stores the task to be executed by the common concurrency stream,
// whatever type the ask actually is, either sync or async. It holds in itself
// the channel to return its response to its caller.
type TaskRequest struct {
	Ctx      context.Context
	Config   *Config
	Response chan TaskResponse
}

// TaskResponse holds the response metainformation of a TaskRequest
type TaskResponse struct {
	Result drivers.RunResult
	Err    error
}

// Hot containers - theory of operation
//
// A function is converted into a hot container if its `Format` is either
// models.FormatHTTP or models.FormatJSON. At the very first task request a hot
// container shall be started and run it. It has two internal clocks: one ping,
// whose responsibility is to start new hot containers as they get clogged with
// work; and the other that actually stops the container if it sits doing
// nothing long enough.
//
// Internally, the hot container uses a modified Config whose Stdin and Stdout
// are bound to an internal pipe. This internal pipe is fed with incoming tasks
// Stdin and feeds incoming tasks with Stdout.
//
// Each execution is the alternation of feeding hot containers stdin with tasks
// stdin, and reading the answer back from containers stdout. For both `Format`s
// we send embedded into the message metadata to help the container to know when
// to stop reading from its stdin and Functions expect the container to do the
// same. Refer to api/runner/protocol.go for details of these communications.
//
// Hot Containers implementation relies in two moving parts (drawn below):
// hotcontainermgr and hotcontainer. Refer to their respective comments for
// details.
//                             │
//                         Incoming
//                           Task
//                             │
//                             ▼
//                     ┌───────────────┐
// ┌───────────┐       │ Task Request  │
// │  Runner   │◀──────│   Main Loop   │
// └───────────┘       └───────────────┘
//                             │
//                      ┌──────▼────────┐
//                     ┌┴──────────────┐│
//                     │   Per Image   ││
//             ┌───────│ Hot Container │├───────┐
//             │       │    Manager    ├┘       │
//             │       └───────────────┘        │
//             │               │                │
//             ▼               ▼                ▼
//       ┌───────────┐   ┌───────────┐    ┌───────────┐
//       │    Hot    │   │    Hot    │    │    Hot    │
//       │ Container │   │ Container │    │ Container │
//       └───────────┘   └───────────┘    └───────────┘
//          Timeout                          Timeout
//           Start                          Terminate
//          (ping)                      (internal clock)

// These are the two clocks important for hot containers life cycle. Scales up
// if no Hot Container does not pick any task in hotContainerScaleUpTimeout.
// Scales down if the container is idle for hotContainerScaleDownTimeout.
const (
	hotContainerScaleUpTimeout   = 1 * time.Second
	hotContainerScaleDownTimeout = 5 * time.Second
)

// StartWorkers handle incoming tasks and spawns self-regulating container
// worker.
func StartWorkers(ctx context.Context, rnr *Runner, tasks <-chan TaskRequest) {
	var wg sync.WaitGroup
	defer wg.Wait()
	var hcmgr hotcontainermgr

	for {
		select {
		case <-ctx.Done():
			return
		case task := <-tasks:

			isStream, err := protocol.IsStreamable(task.Config.Format)
			if err != nil {
				logrus.WithError(err).Info("could not detect container IO protocol")
				wg.Add(1)
				go runTaskReq(rnr, &wg, task)
				continue
			}

			if !isStream {
				wg.Add(1)
				go runTaskReq(rnr, &wg, task)
				continue
			}

			p := hcmgr.getPipe(ctx, rnr, task.Config)
			if p == nil {
				logrus.Info("could not find a hot container - running regularly")
				wg.Add(1)
				go runTaskReq(rnr, &wg, task)
				continue
			}

			select {
			case <-ctx.Done():
				return
			case p <- task:
			}
		}
	}
}

// RunTask helps sending a TaskRequest into the common concurrency stream.
func RunTask(tasks chan TaskRequest, ctx context.Context, cfg *Config) (drivers.RunResult, error) {
	tresp := make(chan TaskResponse)
	treq := TaskRequest{Ctx: ctx, Config: cfg, Response: tresp}
	tasks <- treq
	resp := <-treq.Response
	return resp.Result, resp.Err
}

// hotcontainermgr is the intermediate between the common concurrency stream and
// hot containers. All hot containers share a single TaskRequest stream per
// image (chn), but each image may have more than one hot container (hc).
type hotcontainermgr struct {
	chn map[string]chan TaskRequest
	hc  map[string]*hotcontainersvr
}

func (h *hotcontainermgr) getPipe(ctx context.Context, rnr *Runner, cfg *Config) chan TaskRequest {
	if h.chn == nil {
		h.chn = make(map[string]chan TaskRequest)
		h.hc = make(map[string]*hotcontainersvr)
	}

	image := cfg.Image
	tasks, ok := h.chn[image]
	if !ok {
		h.chn[image] = make(chan TaskRequest)
		tasks = h.chn[image]
		svr := newHotcontainersvr(ctx, cfg, rnr, tasks)
		if err := svr.launch(ctx); err != nil {
			logrus.WithError(err).Error("cannot start hot container supervisor")
			return nil
		}
		h.hc[image] = svr
	}

	return tasks
}

// hotcontainersvr is part of hotcontainermgr, abstracted apart for
// simplicity, its only purpose is to test for hot containers saturation and
// try starting as many as needed.
type hotcontainersvr struct {
	cfg   *Config
	rnr   *Runner
	tasks <-chan TaskRequest
	ping  chan struct{}
	maxc  chan struct{}
}

func newHotcontainersvr(ctx context.Context, cfg *Config, rnr *Runner, tasks <-chan TaskRequest) *hotcontainersvr {
	svr := &hotcontainersvr{
		cfg:   cfg,
		rnr:   rnr,
		tasks: tasks,
		ping:  make(chan struct{}),
		maxc:  make(chan struct{}, cfg.MaxConcurrency),
	}
	go svr.scale(ctx)
	return svr
}

func (svr *hotcontainersvr) scale(ctx context.Context) {
	for {
		timeout := time.After(hotContainerScaleUpTimeout)
		select {
		case <-ctx.Done():
			return

		case <-timeout:
			if err := svr.launch(ctx); err != nil {
				logrus.WithError(err).Error("cannot start more hot containers")
			}

		case svr.ping <- struct{}{}:
			time.Sleep(1 * time.Second)
		}
	}

}

func (svr *hotcontainersvr) launch(ctx context.Context) error {
	select {
	case svr.maxc <- struct{}{}:
		hc, err := newHotcontainer(
			svr.cfg,
			protocol.Protocol(svr.cfg.Format),
			svr.tasks,
			svr.rnr,
			svr.ping,
		)
		if err != nil {
			return err
		}
		go func() {
			hc.serve(ctx)
			<-svr.maxc
		}()
	default:
	}

	return nil
}

// hotcontainer actually interfaces an incoming task from the common concurrency
// stream into a long lived container. If idle long enough, it will stop. It
// uses route configuration to determine which protocol to use.
type hotcontainer struct {
	cfg   *Config
	proto protocol.ContainerIO
	tasks <-chan TaskRequest
	ping  <-chan struct{}

	// Side of the pipe that takes information from outer world
	// and injects into the container.
	in  io.Writer
	out io.Reader

	// Receiving side of the container.
	containerIn  io.Reader
	containerOut io.Writer

	rnr *Runner
}

func newHotcontainer(cfg *Config, proto protocol.Protocol, tasks <-chan TaskRequest, rnr *Runner, ping <-chan struct{}) (*hotcontainer, error) {
	stdinr, stdinw := io.Pipe()
	stdoutr, stdoutw := io.Pipe()

	p, err := protocol.New(proto, stdinw, stdoutr)
	if err != nil {
		return nil, err
	}

	hc := &hotcontainer{
		cfg:   cfg,
		proto: p,
		tasks: tasks,
		ping:  ping,

		in:  stdinw,
		out: stdoutr,

		containerIn:  stdinr,
		containerOut: stdoutw,

		rnr: rnr,
	}

	return hc, nil
}

func (hc *hotcontainer) serve(ctx context.Context) {
	lctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		for {
			inactivity := time.After(hotContainerScaleDownTimeout)

			select {
			case <-lctx.Done():
				return

			case <-inactivity:
				cancel()

			case <-hc.ping:

			case task := <-hc.tasks:
				if err := hc.proto.Dispatch(lctx, task.Config.Stdin, task.Config.Stdout); err != nil {
					logrus.WithField("ctx", lctx).Info("task failed")
					task.Response <- TaskResponse{
						&runResult{StatusValue: "error", error: err},
						err,
					}
					continue
				}

				task.Response <- TaskResponse{
					&runResult{StatusValue: "success"},
					nil,
				}
			}
		}
	}()

	cfg := *hc.cfg
	cfg.Timeout = 0 // add a timeout to simulate ab.end. failure.
	cfg.Stdin = hc.containerIn
	cfg.Stdout = hc.containerOut
	cfg.Stderr = os.Stderr
	result, err := hc.rnr.Run(lctx, &cfg)
	if err != nil {
		logrus.WithError(err).Error("hot container failure")
	}
	logrus.WithField("result", result).Info("hot container done")
	cancel()
	wg.Wait()
}

func runTaskReq(rnr *Runner, wg *sync.WaitGroup, task TaskRequest) {
	defer wg.Done()
	result, err := rnr.Run(task.Ctx, task.Config)
	select {
	case task.Response <- TaskResponse{result, err}:
		close(task.Response)
	default:
	}
}

type runResult struct {
	error
	StatusValue string
}

func (r *runResult) Error() string {
	if r.error == nil {
		return ""
	}
	return r.error.Error()
}

func (r *runResult) Status() string { return r.StatusValue }
