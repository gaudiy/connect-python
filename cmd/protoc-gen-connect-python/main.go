// Copyright 2024 Gaudiy, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"fmt"
	"os"

	"google.golang.org/protobuf/compiler/protogen"

	"github.com/gaudiy/connect-python/cmd/protoc-gen-connect-python/generator"
)

var pluginVersion = "devel"

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--version" {
		fmt.Fprintln(os.Stdout, pluginVersion)
		os.Exit(0)
	}

	var f flag.FlagSet
	var cfg generator.Config
	protogen.Options{ParamFunc: f.Set}.Run(func(plugin *protogen.Plugin) error {
		gen, err := generator.NewGenerator(plugin, &cfg)
		if err != nil {
			return err
		}
		gen.Generate()
		return nil
	})
}
