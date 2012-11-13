package gonatsd

import (
	steno "github.com/cloudfoundry/gosteno"
	"os"
)

var (
	Log Logger // Global singleton for current logger
)

func init() {
	defaultLogger, err := NewLogger(os.Stderr, "debug")
	if err != nil {
		panic(err)
	}
	Log = defaultLogger
}

type Logger interface {
	steno.Logger
}

func NewLogger(file *os.File, level string) (Logger, error) {
	if len(level) == 0 {
		level = "debug"
	}

	logLevel, err := steno.GetLogLevel(level)
	if err != nil {
		return nil, err
	}

	out := steno.NewIOSink(file)
	out.SetCodec(steno.NewJsonPrettifier(steno.EXCLUDE_DATA))

	cnf := &steno.Config{
		Sinks:     []steno.Sink{out},
		Level:     logLevel,
		Port:      8080, // TODO: parameterize?
		EnableLOC: false,
	}
	steno.Init(cnf)

	return steno.NewLogger("gonatsd"), nil
}

func ReplaceLogger(logger Logger) {
	Log = logger
}
