// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package webv2

import (
	"context"
	"embed"
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/google/subcommands"
)

var FrontendDir embed.FS

type WebCmd struct {
	DistDir  embed.FS
	logLevel string
	open     bool
	port	int
}

// Name returns the name of operation.
func (cmd *WebCmd) Name() string {
	return "web"
}

// Synopsis returns summary of operation.
func (cmd *WebCmd) Synopsis() string {
	return "run the web UI for HarbourBridge"
}

func (cmd *WebCmd) Usage() string {
	return fmt.Sprintf(`%v web`, path.Base(os.Args[0]))
}

func (cmd *WebCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&cmd.logLevel, "log-level", "DEBUG", "Configure the logging level for the command (INFO, DEBUG), defaults to DEBUG")
	f.BoolVar(&cmd.open, "open", false, "Opens the Harbourbridge web interface in the default browser, defaults to false")
	f.IntVar(&cmd.port, "port", 8080, "The port in which Harbourbridge will run, defaults to 8080")
}

func (cmd *WebCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	os.RemoveAll(filepath.Join(os.TempDir(), constants.HB_TMP_DIR))
	FrontendDir = cmd.DistDir
	var err error
	defer func() {
		if err != nil {
			fmt.Printf("FATAL error, unable to start webapp: %s", err)
		}
	}()
	err = App(cmd.logLevel, cmd.open, cmd.port)
	return subcommands.ExitSuccess
}
