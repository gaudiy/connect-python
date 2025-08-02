package generator

import (
	"fmt"
)

// p.P(`import abc`)
// p.P(`from enum import Enum`)
// p.P()
// p.P(`from gconnect.connect import UnaryRequest, UnaryResponse`)
// p.P(`from gconnect.handler import UnaryHandler`)
// p.P(`from gconnect.options import HandlerOptions`)
// p.P(`from google.protobuf.descriptor import MethodDescriptor, ServiceDescriptor`)

// PythonIdent is a Python identifier, consisting of a name and import path.
//
// The name is a single identifier and may not be a dot-qualified selector.
type PythonIdent struct {
	PythonName       string
	PythonImportPath PythonImportPath
}

func (id PythonIdent) String() string {
	return fmt.Sprintf("%s.%s", id.PythonImportPath, id.PythonName)
}

// PythonImportPath is the import path of a Python package.
//
// For example: "google.protobuf.descriptor".
type PythonImportPath string

func (p PythonImportPath) String() string { return string(p) }

// Ident returns a PythonIdent with s as the PythonName and p as the PythonImportPath.
func (p PythonImportPath) Ident(s string) PythonIdent {
	return PythonIdent{
		PythonName:       s,
		PythonImportPath: p,
	}
}

// Package represents a generates Python import directive.
// type Package string

// func (p Package) Import(gf *protogen.GeneratedFile) {
// 	gf.Import()
// }
