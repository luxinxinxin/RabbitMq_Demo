package utils

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/rs/zerolog/log"

	"go.uber.org/fx"
)

type FxHookUtil struct{}

type iDaemon interface {
	Start() error
	Stop()
}

func NewFxHookUtil2(lc fx.Lifecycle, daemons ...iDaemon) *FxHookUtil {
	checkStart := func(ins iDaemon, errChan chan error) {
		moduleName := reflect.TypeOf(ins).String()

		log.Info().Str(`mod`, moduleName).Msg(`start`)
		defer log.Info().Str(`mod`, moduleName).Msg(`stop`)

		err := ins.Start()
		if err == nil {
			return
		}

		select {
		case errChan <- err:
		default:
		}
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			errChan := make(chan error)
			for _, obj := range daemons {
				go checkStart(obj, errChan)
			}

			select {
			case e := <-errChan:
				return errors.New(`start fail: ` + e.Error())
			case <-time.After(time.Second * 2):
				return nil
			}
		},

		OnStop: func(ctx context.Context) error {
			for _, obj := range daemons {
				obj.Stop()
			}
			time.Sleep(time.Second)
			return nil
		},
	})

	return &FxHookUtil{}
}
