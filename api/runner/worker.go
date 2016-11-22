package runner

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"

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

	wg.Add(1)
	hot := make(chan TaskRequest)
	go func() {
		wg.Done()

		stdinr, stdinw := io.Pipe()
		stdoutr, stdoutw := io.Pipe()
		prot, err := protocol.New(protocol.JSON, stdinw, stdoutr)
		if err != nil {
			logrus.WithError(err).Error("hot container bootstrap failure")
		}

		wg.Add(1)
		go func() {
			wg.Done()
			result, err := rnr.Run(ctx, &Config{
				ID: fmt.Sprint("hot-container-ccirello-hotcont-http-", rand.Intn(1000)),
				// Image:  "ccirello/hotcont:http",
				Image:  "ccirello/hotcontjson:0.0.1",
				Stdin:  stdinr,
				Stdout: stdoutw,
				Stderr: os.Stderr,
			})
			if err != nil {
				logrus.WithError(err).Error("hot container failure")
			}
			logrus.WithField("result", result).Info("hot container done")
		}()

		for {
			select {
			case <-ctx.Done():
				wg.Wait()
				return

			case task := <-hot:
				if err := prot.Dispatch(task.Config.Stdin, task.Config.Stdout); err != nil {
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
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case task := <-tasks:
			if task.Config.Format == models.FormatDefault {
				wg.Add(1)
				go runTaskReq(rnr, &wg, task)
			} else {
				hot <- task
			}
		}
	}

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
