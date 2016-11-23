package runner

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"

	"cirello.io/supervisor"
	"github.com/Sirupsen/logrus"
	"github.com/iron-io/functions/api/models"
	"github.com/iron-io/functions/api/runner/protocol"
	"github.com/iron-io/runner/drivers"
)

type TaskRequest struct {
	Ctx      context.Context
	Config   *Config
	Response chan TaskResponse
}

type TaskResponse struct {
	Result drivers.RunResult
	Err    error
}

// Hot containers - theory of operation
// A function is converted into a hot container once it achieves a certain
// threshold of wait. The longer a task is waiting by the queue, more likely it
// will trigger a timeout on which a hot container will be started. At same
// time, if a certain image does not generate any workload in a certain timeout
// period, the hot container will be stopped. Internally, the hot container uses
// a modified Config, whose Stdin and Stdout, are bound to an internal pipe.
// This internal pipe is fed with incoming tasks Stdin and feeds incoming
// tasks with Stdout. The problem here is to know when to stop reading. When
// does the executor must stop writing into hot container's Stdin, and start
// reading its Stdout? Also once started reading Stdout, when does it stop?
// docs/functions-format.md proposes several protocols, among them HTTP/1 and
// JSON. Both of them help with these problems: HTTP/1 can make use of
// Content-Length header, and JSON can start reading a JSON and stops when the
// root object is finished reading. In any case, either enveloping or size
// tracking are mandatory for binary-safe operations - and message bound
// detection.

// StartWorkers handle incoming tasks and spawns self-regulating container
// workers.
func StartWorkers(ctx context.Context, rnr *Runner, tasks <-chan TaskRequest) {
	var wg sync.WaitGroup

	var hcmgr hotcontainermgr

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case task := <-tasks:
			if task.Config.Format == models.FormatDefault {
				wg.Add(1)
				go runTaskReq(rnr, &wg, task)
				continue
			}

			p := hcmgr.GetPipe(ctx, rnr, task.Config)
			if p == nil {
				wg.Add(1)
				logrus.Info("could not find a hot container - running regularly")
				go runTaskReq(rnr, &wg, task)
				continue
			}
			p <- task
		}
	}

}

type hotcontainermgr struct {
	chn map[string]chan TaskRequest
	hc  map[string]*hotContainerSupervisor
}

func (h *hotcontainermgr) GetPipe(ctx context.Context, rnr *Runner, cfg *Config) chan TaskRequest {
	if h.chn == nil {
		h.chn = make(map[string]chan TaskRequest)
		h.hc = make(map[string]*hotContainerSupervisor)
	}

	image := cfg.Image
	tasks, ok := h.chn[image]
	if !ok {
		h.chn[image] = make(chan TaskRequest)
		tasks = h.chn[image]
		svr := newHotContainerSupervisor(ctx, cfg, rnr, tasks)
		if err := svr.launch(); err != nil {
			logrus.WithError(err).Error("cannot start hot container supervisor")
			return nil
		}
		h.hc[image] = svr
	}

	return tasks
}

type hotContainerSupervisor struct {
	cfg   *Config
	rnr   *Runner
	tasks <-chan TaskRequest
	supervisor.Supervisor
}

func newHotContainerSupervisor(ctx context.Context, cfg *Config, rnr *Runner, tasks <-chan TaskRequest) *hotContainerSupervisor {
	svr := &hotContainerSupervisor{
		cfg:   cfg,
		rnr:   rnr,
		tasks: tasks,
		Supervisor: supervisor.Supervisor{
			Name:        fmt.Sprintf("hot container manager for %s", cfg.Image),
			MaxRestarts: supervisor.AlwaysRestart,
			Log: func(msg interface{}) {
				logrus.Debug(msg)
			},
		},
	}
	go svr.Supervisor.Serve(ctx)
	return svr
}

func (svr *hotContainerSupervisor) launch() error {
	// TODO(ccirello): find a better naming strategy than rand.Int()
	hc, err := newHotContainer(
		fmt.Sprintf("%x", md5.Sum([]byte(fmt.Sprint(svr.cfg.Image, "-", rand.Int())))),
		svr.cfg.Image,
		protocol.Protocol(svr.cfg.Format),
		svr.tasks,
		svr.rnr,
	)
	if err != nil {
		return err
	}
	svr.AddService(hc, supervisor.Transient)
	return nil
}

type hotcontainer struct {
	name  string
	image string
	proto protocol.ContainerIO
	tasks <-chan TaskRequest

	// Side of the pipe that takes information from outer world
	// and injects into the container.
	in  io.Writer
	out io.Reader

	// Receiving side of the container.
	containerIn  io.Reader
	containerOut io.Writer

	rnr *Runner
}

func newHotContainer(name, image string, proto protocol.Protocol, tasks <-chan TaskRequest, rnr *Runner) (*hotcontainer, error) {
	stdinr, stdinw := io.Pipe()
	stdoutr, stdoutw := io.Pipe()

	p, err := protocol.New(proto, stdinw, stdoutr)
	if err != nil {
		return nil, err
	}

	hc := &hotcontainer{
		name:  name,
		image: image,
		proto: p,
		tasks: tasks,

		in:  stdinw,
		out: stdoutr,

		containerIn:  stdinr,
		containerOut: stdoutw,

		rnr: rnr,
	}

	return hc, nil
}

func (hc *hotcontainer) String() string {
	return fmt.Sprintf("hot container %v", hc.name)
}

func (hc *hotcontainer) Serve(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		for {
			select {
			case <-ctx.Done():
				return

			case task := <-hc.tasks:
				if err := hc.proto.Dispatch(task.Config.Stdin, task.Config.Stdout); err != nil {
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

	result, err := hc.rnr.Run(ctx, &Config{
		ID:     hc.name,
		Image:  hc.image,
		Stdin:  hc.containerIn,
		Stdout: hc.containerOut,
		Stderr: os.Stderr,
	})
	if err != nil {
		logrus.WithError(err).Error("hot container failure")
	}
	logrus.WithField("result", result).Info("hot container done")
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

func RunTask(tasks chan TaskRequest, ctx context.Context, cfg *Config) (drivers.RunResult, error) {
	tresp := make(chan TaskResponse)
	treq := TaskRequest{Ctx: ctx, Config: cfg, Response: tresp}
	tasks <- treq
	resp := <-treq.Response
	return resp.Result, resp.Err
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
