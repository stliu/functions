package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"github.com/iron-io/functions/fn/langs"
	"github.com/urfave/cli"
)

func isFuncfile(path string, info os.FileInfo) bool {
	if info.IsDir() {
		return false
	}

	basefn := filepath.Base(path)
	for _, fn := range validfn {
		if basefn == fn {
			return true
		}
	}

	return false
}

func walker(path string, info os.FileInfo, err error, f func(path string) error) {
	if err := f(path); err != nil {
		fmt.Fprintln(os.Stderr, path, err)
	}
}

type commoncmd struct {
	wd          string
	verbose     bool
	force       bool
	recursively bool

	verbwriter io.Writer
}

func (c *commoncmd) flags() []cli.Flag {
	return []cli.Flag{
		cli.StringFlag{
			Name:        "d",
			Usage:       "working directory",
			Destination: &c.wd,
			EnvVar:      "WORK_DIR",
			Value:       "./",
		},
		cli.BoolFlag{
			Name:        "v",
			Usage:       "verbose mode",
			Destination: &c.verbose,
		},
		cli.BoolFlag{
			Name:        "f",
			Usage:       "force updating of all functions that are already up-to-date",
			Destination: &c.force,
		},
		cli.BoolFlag{
			Name:        "r",
			Usage:       "recursively scan all functions",
			Destination: &c.recursively,
		},
	}
}

func (c *commoncmd) scan(walker func(path string, info os.FileInfo, err error) error) {
	c.verbwriter = ioutil.Discard
	if c.verbose {
		c.verbwriter = os.Stderr
	}

	var walked bool

	err := filepath.Walk(c.wd, func(path string, info os.FileInfo, err error) error {
		if !c.recursively && path != c.wd && info.IsDir() {
			return filepath.SkipDir
		}

		if !isFuncfile(path, info) {
			return nil
		}

		if c.recursively && !c.force && !isstale(path) {
			return nil
		}

		e := walker(path, info, err)
		now := time.Now()
		os.Chtimes(path, now, now)
		walked = true
		return e
	})
	if err != nil {
		fmt.Fprintf(c.verbwriter, "file walk error: %s\n", err)
	}

	if !walked {
		fmt.Println("No function file found.")
		return
	}
}

// Theory of operation: this takes an optimistic approach to detect whether a
// package must be rebuild/bump/published. It loads for all files mtime's and
// compare with functions.json own mtime. If any file is younger than
// functions.json, it triggers a rebuild.
// The problem with this approach is that depending on the OS running it, the
// time granularity of these timestamps might lead to false negatives - that is
// a package that is stale but it is not recompiled. A more elegant solution
// could be applied here, like https://golang.org/src/cmd/go/pkg.go#L1111
func isstale(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return true
	}

	fnmtime := fi.ModTime()
	dir := filepath.Dir(path)
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		if info.ModTime().After(fnmtime) {
			return errors.New("found stale package")
		}
		return nil
	})

	return err != nil
}

func (c commoncmd) buildfunc(path string) (*funcfile, error) {
	funcfile, err := parsefuncfile(path)
	if err != nil {
		return nil, err
	}

	if err := c.localbuild(path, funcfile.Build); err != nil {
		return nil, err
	}

	if err := c.dockerbuild(path, funcfile); err != nil {
		return nil, err
	}

	return funcfile, nil
}

func (c commoncmd) localbuild(path string, steps []string) error {
	for _, cmd := range steps {
		exe := exec.Command("/bin/sh", "-c", cmd)
		exe.Dir = filepath.Dir(path)
		exe.Stderr = c.verbwriter
		exe.Stdout = c.verbwriter
		fmt.Fprintf(c.verbwriter, "- %s:\n", cmd)
		if err := exe.Run(); err != nil {
			return fmt.Errorf("error running command %v (%v)", cmd, err)
		}
	}

	return nil
}

func (c commoncmd) dockerbuild(path string, ff *funcfile) error {
	dir := filepath.Dir(path)

	var helper langs.LangHelper
	dockerfile := filepath.Join(dir, "Dockerfile")
	if !exists(dockerfile) {
		err := writeTmpDockerfile(dir, ff)
		defer os.Remove(filepath.Join(dir, "Dockerfile"))
		if err != nil {
			return err
		}
		helper, err = langs.GetLangHelper(*ff.Runtime)
		if err != nil {
			return err
		}
		if helper.HasPreBuild() {
			err := helper.PreBuild()
			if err != nil {
				return err
			}
		}
	}

	fmt.Printf("Building image %v\n", ff.FullName())
	cmd := exec.Command("docker", "build", "-t", ff.FullName(), ".")
	cmd.Dir = dir
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("error running docker build: %v", err)
	}
	if helper != nil {
		err := helper.AfterBuild()
		if err != nil {
			return err
		}
	}
	return nil
}

func exists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

var acceptableFnRuntimes = map[string]string{
	"elixir":    "iron/elixir",
	"erlang":    "iron/erlang",
	"gcc":       "iron/gcc",
	"go":        "iron/go",
	"java":      "iron/java",
	"leiningen": "iron/leiningen",
	"mono":      "iron/mono",
	"node":      "iron/node",
	"perl":      "iron/perl",
	"php":       "iron/php",
	"python":    "iron/python:2",
	"ruby":      "iron/ruby",
	"scala":     "iron/scala",
	"rust":      "corey/rust-alpine",
}

const tplDockerfile = `FROM {{ .BaseImage }}
WORKDIR /function
ADD . /function/
ENTRYPOINT [{{ .Entrypoint }}]
`

func writeTmpDockerfile(dir string, ff *funcfile) error {
	if ff.Entrypoint == nil || *ff.Entrypoint == "" {
		return errors.New("entrypoint is missing")
	}

	runtime, tag := ff.RuntimeTag()
	rt, ok := acceptableFnRuntimes[runtime]
	if !ok {
		return fmt.Errorf("cannot use runtime %s", runtime)
	}

	if tag != "" {
		rt = fmt.Sprintf("%s:%s", rt, tag)
	}

	fd, err := os.Create(filepath.Join(dir, "Dockerfile"))
	if err != nil {
		return err
	}

	// convert entrypoint string to slice
	epvals := strings.Fields(*ff.Entrypoint)
	var buffer bytes.Buffer
	for i, s := range epvals {
		if i > 0 {
			buffer.WriteString(", ")
		}
		buffer.WriteString("\"")
		buffer.WriteString(s)
		buffer.WriteString("\"")
	}
	fmt.Println(buffer.String())

	t := template.Must(template.New("Dockerfile").Parse(tplDockerfile))
	err = t.Execute(fd, struct {
		BaseImage, Entrypoint string
	}{rt, buffer.String()})
	fd.Close()
	return err
}

func extractEnvConfig(configs []string) map[string]string {
	c := make(map[string]string)
	for _, v := range configs {
		kv := strings.SplitN(v, "=", 2)
		c[kv[0]] = os.ExpandEnv(kv[1])
	}
	return c
}
