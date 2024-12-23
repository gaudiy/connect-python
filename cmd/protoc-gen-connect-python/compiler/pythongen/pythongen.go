// Copyright 2024 Gaudiy, Inc.
// SPDX-License-Identifier: Apache-2.0

// Package pythongen generates Python source code using [google.golang.org/protobuf/compiler/protogen].
package pythongen

import (
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Config represents a generates Python source code config.
type Config struct{}

// Plugin is a protoc plugin invocation for Python.
type Plugin struct {
	*protogen.Plugin

	Config *Config
	local  map[protoreflect.FullName]bool
}
