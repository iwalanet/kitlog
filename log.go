package kitlog

import (
	"fmt"
	"io"
	"os"

	"github.com/NadiaSama/eslogger"
	"github.com/NadiaSama/logrouter"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"

	"github.com/iwalanet/kitlog/es"
)

type (
	//Config logger配置根据配置可以将logger内容输出到文件及elastic search
	Config struct {
		File  string    `mapstructure:"file"`  //文件名称，stdout输出到标准输出
		Level int       `mapstructure:"level"` //1=error, 2=warn, 3=info, 4=debug
		ES    *ESConfig `mapstructure:"es"`    //设置elastic search 存储
	}

	//ESConfig 配置elastic search
	ESConfig struct {
		Addr   string `mapstructure:"addr"`
		User   string `mapstructure:"user"`
		Pass   string `mapstructure:"pass"`
		Stream string `mapstructure:"stream"` //stream名称会根据datasream pattern进行调整
	}

	//Logger 实现了kit log interface{},便于与level, with等方法整合。同时通过logrouter整合了
	//logger以及ESLogger便于信息的记录以及将指定信息转存到ES
	Logger struct {
		logger   log.Logger
		esLogger *eslogger.ESLogger
		file     io.WriteCloser
	}
)

var (
	gLogger *Logger
)

//Open 根据cfg初始化gLogger
func Open(cfg *Config) error {
	var err error
	gLogger, err = cfg.OpenLogger()
	if err != nil {
		return errors.WithMessage(err, "open logger failed")
	}

	return nil
}

//Close close gLogger
func Close() error {
	return gLogger.Close()
}

//Wrapper 通过log.With以及gLogger, args构建新的wrapper
//设立设置为全局方法便于其他代码使用
func Wrapper(args ...interface{}) *es.Wrapper {
	return es.NewWrapper(log.With(gLogger.logger, args...))
}

func (l *Logger) Log(keyvals ...interface{}) error {
	return l.logger.Log(keyvals...)
}

//Close 关闭Logger对应的文件
func (l *Logger) Close() error {
	if l.file != nil {
		if err := l.file.Close(); err != nil {
			return errors.WithMessage(err, "file close error")
		}
	}

	if l.esLogger != nil {
		l.esLogger.Close()
	}
	return nil
}

func (l *Logger) Wrapper() *es.Wrapper {
	return es.NewWrapper(l.logger)
}

//OpenLogger 根据config配置创建Logger
func (c *Config) OpenLogger() (*Logger, error) {
	var (
		ret      Logger
		logger   log.Logger
		file     *os.File
		esLogger *eslogger.ESLogger
		err      error
	)

	if c.File == "stdout" {
		logger = log.NewLogfmtLogger(os.Stdout)
	} else {
		file, err = os.OpenFile(c.File, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, errors.WithMessagef(err, "open log file fail '%s'", c.File)
		}
		logger = log.NewLogfmtLogger(file)
	}

	if c.ES != nil {
		if err := es.Create(c.ES.Addr, c.ES.User, c.ES.Pass); err != nil {
			return nil, errors.WithMessage(err, "es init fail")
		}

		eslc := eslogger.NewConfig(fmt.Sprintf("http://%s", c.ES.Addr), es.DataStreamName(c.ES.Stream))
		if c.ES.User != "" {
			eslc.BasicAuth(c.ES.User, c.ES.Pass)
		}
		esLogger = eslogger.New(eslc)

		if err := esLogger.Open(); err != nil {
			return nil, errors.WithMessage(err, "eslogger open fail")
		}

		mapper := logrouter.NewMapper(es.ESField)
		mapper.SetDefault(logger)
		mapper.AddLogger(es.ESOn, esLogger)
		logger = mapper
	}

	ret.logger = log.With(logger, "@timestamp", log.DefaultTimestamp)
	ret.logger = c.SetLevel(ret.logger)
	ret.file = file
	ret.esLogger = esLogger
	return &ret, nil
}

func (c *Config) SetLevel(log log.Logger) log.Logger {
	var option level.Option
	switch c.Level {
	case 1:
		option = level.AllowError()
	case 2:
		option = level.AllowWarn()
	case 3:
		option = level.AllowInfo()
	case 4:
		option = level.AllowDebug()
	default:
		panic(fmt.Sprintf("invalid level %d", c.Level))
	}

	return level.NewFilter(log, option)
}
