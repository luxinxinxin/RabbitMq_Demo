package publisher

import (
	"rabbitmq_demo/internal/pkg/utils"

	"github.com/spf13/viper"
	"go.uber.org/fx"
)

type hooker struct{}

func start(_ *hooker) {}
func newHooker(lc fx.Lifecycle, rabbitMq IRabbitMQ) *hooker {
	utils.NewFxHookUtil2(lc, rabbitMq)
	return &hooker{}
}

func Start() error {
	providers := []interface{}{
		newHooker,
		newRabbitMq,
	}

	opts := []fx.Option{fx.Provide(providers...), fx.Invoke(start)}
	if viper.GetInt(`PrintFxEnable`) != 1 {
		opts = append(opts, fx.NopLogger)
	}

	fx.New(opts...).Run()
	return nil
}
