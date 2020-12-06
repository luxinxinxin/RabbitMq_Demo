package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"rabbitmq_demo/internal/consumer"
	"rabbitmq_demo/internal/publisher"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/natefinch/lumberjack.v2"
)

func initZeroLog(level string) error {
	zerolog.TimestampFieldName = "ts"
	zerolog.LevelFieldName = "l"
	zerolog.MessageFieldName = "m"
	zerolog.CallerFieldName = "c"
	zerolog.TimeFieldFormat = "0102-15:04:05Z07"
	zerolog.ErrorFieldName = "e"
	zerolog.CallerMarshalFunc = func(file string, line int) string {
		return filepath.Base(file) + ":" + strconv.Itoa(line)
	}

	lvl, _ := zerolog.ParseLevel(level)
	if lvl == zerolog.NoLevel {
		lvl = zerolog.InfoLevel
	}

	zerolog.SetGlobalLevel(lvl)

	viper.SetDefault(`Log.ConsoleEnable`, 1)
	viper.SetDefault(`Log.FileEnable`, 0)
	viper.SetDefault(`Log.Filepath`, "/var/log/naspeer.log")
	viper.SetDefault(`Log.MaxBackups`, 2)
	viper.SetDefault(`Log.MaxSize`, 1)
	viper.SetDefault(`Log.MaxAge`, 3)

	fileEnable := viper.GetInt(`Log.FileEnable`)
	ConsoleEnable := viper.GetInt(`Log.ConsoleEnable`)

	var writers []io.Writer
	if ConsoleEnable != 0 {
		writers = append(writers, zerolog.NewConsoleWriter(func(w *zerolog.ConsoleWriter) {
			w.TimeFormat = "0102-15:04:05Z07"
		}))
	}

	if fileEnable != 0 {
		filePath := viper.GetString(`Log.Filepath`)
		dir := filepath.Dir(filePath)

		if err := os.MkdirAll(dir, 0744); err != nil {
			return err
		}

		writer := lumberjack.Logger{
			Filename:   filePath,
			MaxSize:    viper.GetInt(`Log.MaxSize`),
			MaxAge:     viper.GetInt(`Log.MaxAge`),
			MaxBackups: viper.GetInt(`Log.MaxBackups`),
			LocalTime:  true,
			Compress:   true,
		}
		writers = append(writers, &writer)
	}

	if len(writers) > 0 {
		log.Logger = zerolog.New(io.MultiWriter(writers...)).With().Caller().Timestamp().Logger()
	} else {
		log.Logger = zerolog.New(io.MultiWriter(writers...)).With().Caller().Logger()
	}

	return nil
}

func initialize(config, logLvl string) error {
	viper.AutomaticEnv() // read in environment variables that match
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	if config != "" {
		viper.SetConfigFile(config)
		if err := viper.ReadInConfig(); err != nil {
			return err
		}
	}

	// 打印实际配置
	if len(os.Getenv("DEBUG_PRINTFX")) > 0 {
		defCfg := viper.AllSettings()
		bs, _ := json.MarshalIndent(defCfg, "", " ")
		fmt.Println(string(bs))
	}

	return initZeroLog(logLvl)
}

func main() {
	rootCmd := &cobra.Command{
		Use: "rabbit",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}

	rootCmd.AddCommand(newPublisherCmd())
	rootCmd.AddCommand(newConsumerCmd())
	if err := rootCmd.Execute(); err != nil {
		fmt.Printf("%s\n", err.Error())
		os.Exit(1)
	}
}

func newPublisherCmd() *cobra.Command {
	var config, logLvl string
	cmd := &cobra.Command{
		Use:   "publisher",
		Short: "publisher",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initialize(config, logLvl)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return publisher.Start()
		},
	}

	cmd.Flags().StringVarP(&logLvl, `level`, `l`, `info`, "loglevel")
	cmd.Flags().StringVarP(&config, `config`, `c`, ``, "config path")
	return cmd
}

func newConsumerCmd() *cobra.Command {
	var config, logLvl string
	cmd := &cobra.Command{
		Use:   "consumer",
		Short: "consumer",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initialize(config, logLvl)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return consumer.Start()
		},
	}
	cmd.Flags().StringVarP(&logLvl, `level`, `l`, `info`, "loglevel")
	cmd.Flags().StringVarP(&config, `config`, `c`, ``, "config path")

	return cmd
}
