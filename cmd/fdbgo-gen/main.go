// Command fdbgo-gen generates the option and error-code tables of fdbgo from
// vendored copies of FoundationDB's fdb.options and error_definitions.h.
//
// Run it through go generate in package fdb:
//
//	go generate ./fdb
//
// It writes internal/options/options.go, fdb/options_gen.go,
// internal/fdberr/codes.go and fdb/errors_gen.go below -root.
package main

import (
	"bytes"
	"encoding/xml"
	"flag"
	"fmt"
	"go/format"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

func main() {
	root := flag.String("root", ".", "module root directory the outputs are written below")
	optionsPath := flag.String("options", "", "fdb.options input (default: next to this generator)")
	errorsPath := flag.String("errors", "", "error_definitions.h input (default: next to this generator)")
	flag.Parse()
	log.SetFlags(0)
	log.SetPrefix("fdbgo-gen: ")

	here := filepath.Join(*root, "cmd", "fdbgo-gen")
	if *optionsPath == "" {
		*optionsPath = filepath.Join(here, "fdb.options")
	}
	if *errorsPath == "" {
		*errorsPath = filepath.Join(here, "error_definitions.h")
	}

	scopes, err := parseOptions(*optionsPath)
	if err != nil {
		log.Fatal(err)
	}
	codes, err := parseErrors(*errorsPath)
	if err != nil {
		log.Fatal(err)
	}

	outputs := []struct {
		path string
		src  []byte
	}{
		{"internal/options/options.go", emitInternalOptions(scopes)},
		{"fdb/options_gen.go", emitPublicOptions(scopes)},
		{"internal/fdberr/codes.go", emitInternalErrors(codes)},
		{"fdb/errors_gen.go", emitPublicErrors(codes)},
	}
	for _, o := range outputs {
		src, err := format.Source(o.src)
		if err != nil {
			log.Fatalf("%s: gofmt: %v", o.path, err)
		}
		if err := os.WriteFile(filepath.Join(*root, o.path), src, 0o644); err != nil {
			log.Fatal(err)
		}
	}
}

// ---- naming ----

var initialisms = map[string]string{
	"tls": "TLS", "id": "ID", "ids": "IDs", "api": "API", "json": "JSON", "uid": "UID",
	"ip": "IP", "grv": "GRV", "url": "URL", "http": "HTTP", "dns": "DNS", "cpu": "CPU",
	"io": "IO", "ssl": "SSL", "rpc": "RPC", "grpc": "GRPC", "kms": "KMS", "ryw": "RYW",
	"lru": "LRU", "sql": "SQL", "acl": "ACL", "utf8": "UTF8", "os": "OS", "ttl": "TTL",
	"uri": "URI", "eof": "EOF",
}

// goName converts a snake_case name to CamelCase, upper-casing initialisms.
func goName(s string) string {
	var b strings.Builder
	for _, part := range strings.Split(s, "_") {
		if part == "" {
			continue
		}
		if up, ok := initialisms[strings.ToLower(part)]; ok {
			b.WriteString(up)
			continue
		}
		b.WriteString(strings.ToUpper(part[:1]))
		b.WriteString(part[1:])
	}
	return b.String()
}

// ---- comments ----

const commentWidth = 100

var rstLiteral = regexp.MustCompile("``([^`]*)``")

// plain renders RST “literal“ markup as plain text, so gofmt leaves it alone.
func plain(s string) string {
	s = rstLiteral.ReplaceAllString(s, "$1")
	return strings.Join(strings.Fields(s), " ")
}

// comment writes text as a // comment block wrapped at commentWidth columns.
func comment(b *bytes.Buffer, indent, text string) {
	line := ""
	flush := func() {
		if line != "" {
			fmt.Fprintf(b, "%s// %s\n", indent, line)
			line = ""
		}
	}
	for _, w := range strings.Fields(text) {
		if line != "" && len(indent)*4+3+len(line)+1+len(w) > commentWidth {
			flush()
		}
		if line == "" {
			line = w
		} else {
			line += " " + w
		}
	}
	flush()
}

// ---- fdb.options ----

type xmlOptions struct {
	Scopes []struct {
		Name    string `xml:"name,attr"`
		Options []struct {
			Name             string `xml:"name,attr"`
			Code             int    `xml:"code,attr"`
			ParamType        string `xml:"paramType,attr"`
			ParamDescription string `xml:"paramDescription,attr"`
			Description      string `xml:"description,attr"`
			Hidden           string `xml:"hidden,attr"`
		} `xml:"Option"`
	} `xml:"Scope"`
}

type option struct {
	name, goName       string
	code               int
	paramType          string // "", "Int", "String" or "Bytes"
	paramDesc, desc    string
	hidden, deprecated bool
}

// public reports whether the option is exposed by package fdb.
func (o option) public() bool { return !o.hidden && !o.deprecated }

type scope struct {
	name string
	opts []option
}

func parseOptions(path string) (map[string]*scope, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var x xmlOptions
	if err := xml.Unmarshal(data, &x); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	scopes := map[string]*scope{}
	for _, xs := range x.Scopes {
		s := &scope{name: xs.Name}
		for _, xo := range xs.Options {
			s.opts = append(s.opts, option{
				name:       xo.Name,
				goName:     goName(xo.Name),
				code:       xo.Code,
				paramType:  xo.ParamType,
				paramDesc:  plain(xo.ParamDescription),
				desc:       plain(xo.Description),
				hidden:     xo.Hidden == "true",
				deprecated: strings.HasPrefix(xo.Description, "Deprecated"),
			})
		}
		scopes[xs.Name] = s
	}
	for _, n := range scopeOrder {
		if scopes[n.xml] == nil {
			return nil, fmt.Errorf("%s: missing scope %s", path, n.xml)
		}
	}
	return scopes, nil
}

// scopeOrder lists the scopes and the Go prefix of their constants.
var scopeOrder = []struct{ xml, prefix string }{
	{"NetworkOption", "Network"},
	{"DatabaseOption", "Database"},
	{"TransactionOption", "Transaction"},
	{"StreamingMode", "StreamingMode"},
	{"MutationType", "MutationType"},
	{"ConflictRangeType", "ConflictRangeType"},
	{"ErrorPredicate", "ErrorPredicate"},
}

const optionsHeader = "// Code generated by fdbgo-gen from fdb.options; DO NOT EDIT.\n\n"

func emitInternalOptions(scopes map[string]*scope) []byte {
	var b bytes.Buffer
	b.WriteString(optionsHeader)
	b.WriteString(`// Package options holds the raw codes of fdb.options, the values the C API
// takes in fdb_*_set_option, fdb_transaction_atomic_op and
// fdb_transaction_get_range. StreamingMode codes are the C values.
package options
`)
	for _, so := range scopeOrder {
		fmt.Fprintf(&b, "\n// %s codes.\nconst (\n", so.xml)
		for _, o := range scopes[so.xml].opts {
			text := fmt.Sprintf("%s%s is %s", so.prefix, o.goName, o.name)
			if o.paramType != "" {
				text += " (param " + o.paramType + ")"
			}
			text += "."
			switch {
			case o.hidden:
				text += " Hidden."
			case o.deprecated:
				text += " Deprecated."
			}
			comment(&b, "\t", text)
			fmt.Fprintf(&b, "\t%s%s = %d\n", so.prefix, o.goName, o.code)
		}
		b.WriteString(")\n")
	}
	return b.Bytes()
}

// optionDoc writes the doc comment of a public option: the lead sentence, the
// verbatim description and, on a separate line, its parameter description.
func optionDoc(b *bytes.Buffer, indent, lead string, o option) {
	comment(b, indent, lead+" "+o.desc)
	if o.paramDesc != "" {
		comment(b, indent, "Parameter: "+o.paramDesc)
	}
}

func emitPublicOptions(scopes map[string]*scope) []byte {
	var b bytes.Buffer
	b.WriteString(optionsHeader)
	b.WriteString("package fdb\n\nimport \"github.com/can1357/fdbgo/internal/options\"\n")

	setters := []struct {
		scope, prefix, typ, recv, doc, call string
	}{
		{"NetworkOption", "Network", "NetworkOptions", "struct{}",
			"NetworkOptions sets process-wide options; obtain it with Options.", "setNetworkOption"},
		{"DatabaseOption", "Database", "DatabaseOptions", "struct{ d *Database }",
			"DatabaseOptions sets options of one Database; obtain it with Database.Options.", "o.d.setOption"},
		{"TransactionOption", "Transaction", "TransactionOptions", "struct{ t *Transaction }",
			"TransactionOptions sets options of one Transaction; obtain it with Transaction.Options.", "o.t.setOption"},
	}
	for _, st := range setters {
		fmt.Fprintf(&b, "\n// %s\ntype %s %s\n", st.doc, st.typ, st.recv)
		if st.scope == "NetworkOption" {
			b.WriteString("\n// Options returns the process-wide network options.\nfunc Options() NetworkOptions { return NetworkOptions{} }\n")
		}
		for _, o := range scopes[st.scope].opts {
			if !o.public() {
				continue
			}
			var param, arg string
			switch o.paramType {
			case "":
				arg = "nil"
			case "Int":
				param, arg = "param int64", "int64Param(param)"
			case "String":
				param, arg = "param string", "[]byte(param)"
			case "Bytes":
				param, arg = "param []byte", "param"
			default:
				log.Fatalf("%s.%s: unknown paramType %q", st.scope, o.name, o.paramType)
			}
			b.WriteString("\n")
			optionDoc(&b, "", "Set"+o.goName+" sets the "+o.name+" option.", o)
			fmt.Fprintf(&b, "func (o %s) Set%s(%s) error {\n\treturn %s(options.%s%s, %s)\n}\n",
				st.typ, o.goName, param, st.call, st.prefix, o.goName, arg)
		}
	}

	b.WriteString(`
// StreamingMode tunes how a range read is batched. Values are the C codes
// plus one, so the zero value is StreamingModeIterator.
type StreamingMode int

// Streaming modes.
const (
`)
	for _, o := range scopes["StreamingMode"].opts {
		if !o.public() {
			continue
		}
		optionDoc(&b, "\t", "StreamingMode"+o.goName+" is the "+o.name+" streaming mode.", o)
		fmt.Fprintf(&b, "\tStreamingMode%s StreamingMode = %d\n", o.goName, o.code+1)
	}
	b.WriteString(")\n")

	mutations := scopes["MutationType"].opts
	b.WriteString(`
// MutationType is an atomic operation applied with Transaction.Atomic.
type MutationType int

// Mutation types.
const (
`)
	for _, o := range mutations {
		if !o.public() {
			continue
		}
		optionDoc(&b, "\t", "MutationType"+o.goName+" is the "+o.name+" mutation type.", o)
		fmt.Fprintf(&b, "\tMutationType%s MutationType = options.MutationType%s\n", o.goName, o.goName)
	}
	b.WriteString(")\n")
	for _, o := range mutations {
		if !o.public() {
			continue
		}
		b.WriteString("\n")
		optionDoc(&b, "", o.goName+" applies the "+o.name+" mutation type to key.", o)
		fmt.Fprintf(&b, "func (t *Transaction) %s(key, param []byte) {\n\tt.Atomic(MutationType%s, key, param)\n}\n",
			o.goName, o.goName)
	}

	b.WriteString(`
// ConflictRangeType selects the conflict set a range is added to.
type ConflictRangeType int

// Conflict range types.
const (
`)
	for _, o := range scopes["ConflictRangeType"].opts {
		if !o.public() {
			continue
		}
		optionDoc(&b, "\t", "ConflictRangeType"+o.goName+" is the "+o.name+" conflict range type.", o)
		fmt.Fprintf(&b, "\tConflictRangeType%s ConflictRangeType = options.ConflictRangeType%s\n", o.goName, o.goName)
	}
	b.WriteString(")\n")
	return b.Bytes()
}

// ---- error_definitions.h ----

type errorCode struct {
	name, goName, desc string
	code               int
}

var errorDef = regexp.MustCompile(`(?m)^\s*ERROR\(\s*(\w+)\s*,\s*(\d+)\s*,\s*"((?:[^"\\]|\\.)*)"\s*\)`)

func parseErrors(path string) ([]errorCode, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var codes []errorCode
	seen := map[string]bool{}
	for _, m := range errorDef.FindAllSubmatch(data, -1) {
		code, err := strconv.Atoi(string(m[2]))
		if err != nil {
			return nil, fmt.Errorf("%s: %s: %w", path, m[1], err)
		}
		desc, err := strconv.Unquote(`"` + string(m[3]) + `"`)
		if err != nil {
			return nil, fmt.Errorf("%s: %s: description: %w", path, m[1], err)
		}
		e := errorCode{name: string(m[1]), goName: goName(string(m[1])), desc: desc, code: code}
		if seen[e.goName] {
			return nil, fmt.Errorf("%s: duplicate Go name %s", path, e.goName)
		}
		seen[e.goName] = true
		codes = append(codes, e)
	}
	if len(codes) == 0 {
		return nil, fmt.Errorf("%s: no ERROR definitions found", path)
	}
	return codes, nil
}

const errorsHeader = "// Code generated by fdbgo-gen from error_definitions.h; DO NOT EDIT.\n\n"

func emitInternalErrors(codes []errorCode) []byte {
	var b bytes.Buffer
	b.WriteString(errorsHeader)
	b.WriteString("package fdberr\n\n// Error codes from error_definitions.h.\nconst (\n")
	for _, e := range codes {
		comment(&b, "\t", fmt.Sprintf("%s is %s (%d): %s", e.goName, e.name, e.code, e.desc))
		fmt.Fprintf(&b, "\t%s Error = %d\n", e.goName, e.code)
	}
	b.WriteString(")\n\nfunc lookup(code Error) (name, description string, ok bool) {\n\tswitch code {\n")
	for _, e := range codes {
		fmt.Fprintf(&b, "\tcase %d:\n\t\treturn %q, %q, true\n", e.code, e.name, e.desc)
	}
	b.WriteString("\t}\n\treturn \"\", \"\", false\n}\n")
	return b.Bytes()
}

func emitPublicErrors(codes []errorCode) []byte {
	var b bytes.Buffer
	b.WriteString(errorsHeader)
	b.WriteString("package fdb\n\nimport \"github.com/can1357/fdbgo/internal/fdberr\"\n\n" +
		"// FoundationDB error codes. Match them with errors.Is(err, fdb.ErrNotCommitted).\nconst (\n")
	for _, e := range codes {
		if e.name == "success" {
			continue
		}
		comment(&b, "\t", fmt.Sprintf("Err%s is %s (%d): %s", e.goName, e.name, e.code, e.desc))
		fmt.Fprintf(&b, "\tErr%s = fdberr.%s\n", e.goName, e.goName)
	}
	b.WriteString(")\n")
	return b.Bytes()
}
