package publisher

import "rabbitmq_demo/internal/pkg/iface"

type IRabbitMQ interface {
	iface.IDaemon
}
